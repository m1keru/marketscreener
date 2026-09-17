package mcpsrv_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/m1ke/marketscreener/internal/config"
	"github.com/m1ke/marketscreener/internal/edgar"
	"github.com/m1ke/marketscreener/internal/mcpsrv"
	"github.com/m1ke/marketscreener/internal/prices"
	"github.com/m1ke/marketscreener/internal/screen"
	"github.com/m1ke/marketscreener/internal/store"
)

func newSession(t *testing.T) *mcp.ClientSession {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "msc.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	cfg := config.Default()
	cfg.UserAgentContact = "tester@example.com"
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sc := &screen.Screener{
		Cfg:   cfg,
		Edgar: edgar.New(edgar.Options{UserAgent: cfg.UserAgent(), Logger: log, DataHost: "http://127.0.0.1:1", WWWHost: "http://127.0.0.1:1"}),
		// No network in unit tests: the endpoints above are unreachable on
		// purpose, so only the offline paths are exercised here.
		Prices: prices.New(prices.Options{Store: st, Logger: log,
			StooqBase: "http://127.0.0.1:1/", YahooBase: "http://127.0.0.1:1/"}),
		Store: st, Log: log,
	}
	srv := (&mcpsrv.Server{Screener: sc, Log: log}).NewServer()

	clientT, serverT := mcp.NewInMemoryTransports()
	ctx := context.Background()
	if _, err := srv.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	sess, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	return sess
}

func TestToolsAreAdvertised(t *testing.T) {
	sess := newSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
		if tool.Description == "" {
			t.Errorf("tool %s has no description", tool.Name)
		}
		if tool.InputSchema == nil {
			t.Errorf("tool %s has no input schema", tool.Name)
		}
	}
	sort.Strings(names)
	want := []string{"explain", "funnel", "history", "runs", "screen"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("tools = %v, want %v", names, want)
	}
}

// TestToolErrorsAreReturnedNotThrown: a failing call must come back as a tool
// result with IsError, so the agent sees the message instead of a dead session.
func TestToolErrorsAreReturnedNotThrown(t *testing.T) {
	sess := newSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cases := []struct {
		tool string
		args map[string]any
		want string
	}{
		{"funnel", map[string]any{}, "no runs stored"},
		{"history", map[string]any{"ticker": "GOOD", "metric": "ebitda"}, "unknown metric"},
		{"runs", map[string]any{"compare": []string{"only-one"}}, "exactly two run ids"},
		{"screen", map[string]any{"profile": "graham_enterprising"}, "unknown profile"},
	}
	for _, tc := range cases {
		t.Run(tc.tool, func(t *testing.T) {
			res, err := sess.CallTool(ctx, &mcp.CallToolParams{Name: tc.tool, Arguments: tc.args})
			if err != nil {
				t.Fatalf("call %s: transport error %v", tc.tool, err)
			}
			if !res.IsError {
				t.Fatalf("call %s should have failed, got %+v", tc.tool, res)
			}
			text := contentText(res)
			if !strings.Contains(text, tc.want) {
				t.Errorf("error %q does not contain %q", text, tc.want)
			}
		})
	}
}

func TestRunsToolAnswersOnAnEmptyDatabase(t *testing.T) {
	sess := newSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := sess.CallTool(ctx, &mcp.CallToolParams{Name: "runs", Arguments: map[string]any{"limit": 5}})
	if err != nil {
		t.Fatalf("call runs: %v", err)
	}
	if res.IsError {
		t.Fatalf("runs on an empty database is not an error: %s", contentText(res))
	}
	if !strings.Contains(contentText(res), `"runs"`) {
		t.Errorf("unexpected payload: %s", contentText(res))
	}
}

// TestGuardStdout is the safety net for stdio transport: anything written to
// os.Stdout during a session must end up in stderr, never in the protocol.
func TestGuardStdout(t *testing.T) {
	realErr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	defer func() { os.Stderr = realErr }()

	realStdout, restore, err := mcpsrv.GuardStdout()
	if err != nil {
		t.Fatalf("guard: %v", err)
	}
	if realStdout == os.Stdout {
		t.Error("os.Stdout must be replaced while the guard is active")
	}
	fmt.Println("this must not reach the protocol stream")
	restore()
	w.Close()

	captured, _ := io.ReadAll(r)
	if !strings.Contains(string(captured), "must not reach the protocol") {
		t.Errorf("stdout was not redirected to stderr, captured: %q", captured)
	}
	if os.Stdout != realStdout {
		t.Error("os.Stdout was not restored")
	}
}

func contentText(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

// TestToolsCarrySafetyAnnotations: without them a client has to ask the user to
// approve every call, which makes an unattended daily run impossible.
func TestToolsCarrySafetyAnnotations(t *testing.T) {
	sess := newSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := sess.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	readOnly := map[string]bool{"explain": true, "history": true, "runs": true, "funnel": true}
	for _, tool := range res.Tools {
		a := tool.Annotations
		if a == nil {
			t.Errorf("tool %s has no annotations", tool.Name)
			continue
		}
		if tool.Title == "" {
			t.Errorf("tool %s has no display title", tool.Name)
		}
		if readOnly[tool.Name] && !a.ReadOnlyHint {
			t.Errorf("tool %s only reads and should say so", tool.Name)
		}
		if tool.Name == "screen" {
			if a.ReadOnlyHint {
				t.Error("screen records a run, so it is not read-only")
			}
			if a.DestructiveHint == nil || *a.DestructiveHint {
				t.Error("screen only appends a run and must not be marked destructive")
			}
		}
	}
}
