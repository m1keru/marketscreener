// Package mcpsrv exposes the five screening operations over MCP stdio.
//
// In this mode stdout carries the JSON-RPC protocol: a single stray Print
// corrupts the session. GuardStdout replaces os.Stdout with a pipe into stderr
// so that guarantee does not depend on discipline.
package mcpsrv

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/m1ke/marketscreener/internal/screen"
)

// Version is reported to the MCP client.
const Version = "2.0.0"

// GuardStdout hands back the real stdout and redirects os.Stdout to stderr, so
// any accidental write from anywhere in the process lands in the log instead of
// in the protocol stream. The returned function restores os.Stdout.
func GuardStdout() (real io.WriteCloser, restore func(), err error) {
	realFile := os.Stdout
	real = realFile
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	os.Stdout = w
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := io.Copy(os.Stderr, r); err != nil {
			fmt.Fprintf(os.Stderr, "stdout guard: %v\n", err)
		}
	}()
	return real, func() {
		os.Stdout = realFile
		w.Close()
		<-done
		r.Close()
	}, nil
}

// Server wraps the screener with MCP plumbing.
type Server struct {
	Screener *screen.Screener
	Log      *slog.Logger
}

// ScreenInput is the screen tool's argument object.
type ScreenInput struct {
	Profile           string   `json:"profile,omitempty" jsonschema:"screening profile; only graham_defensive exists in v1"`
	MinMarketCapUSD   *float64 `json:"min_market_cap_usd,omitempty" jsonschema:"override the minimum market capitalisation in USD"`
	IncludeFinancials *bool    `json:"include_financials,omitempty" jsonschema:"include SIC 6000-6799 (banks, insurers, REITs); their liquidity criteria stay unknown"`
	MaxResults        int      `json:"max_results,omitempty" jsonschema:"maximum candidates to return, default 25"`
	Refresh           bool     `json:"refresh,omitempty" jsonschema:"ignore the EDGAR cache and refetch everything"`
}

// ExplainInput is the explain tool's argument object.
type ExplainInput struct {
	Ticker  string `json:"ticker" jsonschema:"US ticker as it appears in EDGAR, e.g. MSFT"`
	Years   int    `json:"years,omitempty" jsonschema:"years of history, default 10"`
	Refresh bool   `json:"refresh,omitempty" jsonschema:"ignore the EDGAR cache for this company"`
}

// HistoryInput is the history tool's argument object.
type HistoryInput struct {
	Ticker  string `json:"ticker" jsonschema:"US ticker as it appears in EDGAR"`
	Metric  string `json:"metric" jsonschema:"one of eps, dividends, revenue, equity, debt, assets"`
	Years   int    `json:"years,omitempty" jsonschema:"years of history, default 10"`
	Refresh bool   `json:"refresh,omitempty" jsonschema:"ignore the EDGAR cache for this company"`
}

// RunsInput is the runs tool's argument object.
type RunsInput struct {
	Limit   int      `json:"limit,omitempty" jsonschema:"how many runs to list, default 10"`
	Compare []string `json:"compare,omitempty" jsonschema:"exactly two run ids to diff against each other"`
}

// FunnelInput is the funnel tool's argument object.
type FunnelInput struct {
	RunID string `json:"run_id,omitempty" jsonschema:"run to inspect; empty means the most recent run"`
}

func ptr[T any](v T) *T { return &v }

// annotations describe what a tool does to the world, so a client does not have
// to ask for approval on every call. Four of the five only read; screen adds a
// row to the local run history and touches nothing outside this machine.
var (
	readOnly = &mcp.ToolAnnotations{
		ReadOnlyHint:  true,
		OpenWorldHint: ptr(true), // it fetches from SEC and the quote sources
	}
	writesRunHistory = &mcp.ToolAnnotations{
		ReadOnlyHint:    false,
		DestructiveHint: ptr(false), // it only appends a run
		IdempotentHint:  false,
		OpenWorldHint:   ptr(true),
	}
)

// NewServer wires the five tools.
func (s *Server) NewServer() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "marketscreener",
		Title:   "marketscreener",
		Version: Version,
	}, &mcp.ServerOptions{
		Instructions: "Deterministic Graham-style screening of US equities from SEC EDGAR filings. " +
			"The server decides nothing beyond the numbers: it returns pass/fail/unknown per criterion " +
			"with the XBRL tag and accession numbers behind every value. Companies with missing data are " +
			"reported in incomplete[], never silently dropped, and the reason a ticker left the previous " +
			"run is computed and returned in diff.dropped[] — do not invent one.",
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "screen",
		Title:       "Screen the US market",
		Annotations: writesRunHistory,
		Description: "Run the full screen over the US universe and return candidates that pass every " +
			"Graham criterion, companies blocked by missing data, and the change since the previous run. " +
			"A cold run takes 2-3 minutes; a warm one seconds.",
	}, s.screen)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "explain",
		Title:       "Explain one ticker",
		Annotations: readOnly,
		Description: "Full breakdown of one ticker: every criterion with its value, target, status and " +
			"the XBRL tag and accession numbers it came from, plus the annual series behind them.",
	}, s.explain)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "history",
		Title:       "Raw annual series",
		Annotations: readOnly,
		Description: "Raw annual series of one metric (eps, dividends, revenue, equity, debt, assets) for one ticker.",
	}, s.history)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "runs",
		Title:       "Past runs",
		Annotations: readOnly,
		Description: "List past runs, or diff two of them by passing exactly two run ids in compare.",
	}, s.runs)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "funnel",
		Title:       "Rejection funnel",
		Annotations: readOnly,
		Description: "How many companies each criterion removed in a run. Use it to see which threshold " +
			"is doing the work before changing one.",
	}, s.funnel)

	return srv
}

// Run serves MCP over stdio until the client disconnects.
func (s *Server) Run(ctx context.Context, stdout io.WriteCloser) error {
	return s.NewServer().Run(ctx, &mcp.IOTransport{Reader: os.Stdin, Writer: stdout})
}

func result[T any](v T) (*mcp.CallToolResult, T, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		var zero T
		return nil, zero, err
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}, v, nil
}

func fail[T any](err error) (*mcp.CallToolResult, T, error) {
	var zero T
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
	}, zero, nil
}

func (s *Server) screen(ctx context.Context, _ *mcp.CallToolRequest, in ScreenInput) (*mcp.CallToolResult, *screen.Result, error) {
	res, err := s.Screener.Screen(ctx, screen.Params{
		Profile:           in.Profile,
		MinMarketCapUSD:   in.MinMarketCapUSD,
		IncludeFinancials: in.IncludeFinancials,
		MaxResults:        in.MaxResults,
		Refresh:           in.Refresh,
	})
	if err != nil {
		return fail[*screen.Result](err)
	}
	return result(res)
}

func (s *Server) explain(ctx context.Context, _ *mcp.CallToolRequest, in ExplainInput) (*mcp.CallToolResult, *screen.ExplainResult, error) {
	res, err := s.Screener.Explain(ctx, in.Ticker, in.Years, in.Refresh)
	if err != nil {
		return fail[*screen.ExplainResult](err)
	}
	return result(res)
}

func (s *Server) history(ctx context.Context, _ *mcp.CallToolRequest, in HistoryInput) (*mcp.CallToolResult, *screen.HistoryResult, error) {
	res, err := s.Screener.History(ctx, in.Ticker, in.Metric, in.Years, in.Refresh)
	if err != nil {
		return fail[*screen.HistoryResult](err)
	}
	return result(res)
}

func (s *Server) runs(ctx context.Context, _ *mcp.CallToolRequest, in RunsInput) (*mcp.CallToolResult, *screen.RunsResult, error) {
	res, err := s.Screener.Runs(ctx, in.Limit, in.Compare)
	if err != nil {
		return fail[*screen.RunsResult](err)
	}
	return result(res)
}

func (s *Server) funnel(ctx context.Context, _ *mcp.CallToolRequest, in FunnelInput) (*mcp.CallToolResult, *screen.FunnelResult, error) {
	res, err := s.Screener.Funnel(ctx, in.RunID)
	if err != nil {
		return fail[*screen.FunnelResult](err)
	}
	return result(res)
}
