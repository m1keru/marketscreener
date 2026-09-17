package render_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/render"
	"github.com/m1ke/marketscreener/internal/screen"
)

// TestScreenMarkdown renders the pinned contract fixture, so the CLI output is
// exercised against exactly the payload the agent receives.
func TestScreenMarkdown(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "screen_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var res screen.Result
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	var b strings.Builder
	if err := render.Screen(&b, &res, i18n.For(i18n.EN)); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := b.String()
	for _, want := range []string{"# Screen", "## Candidates", "GOOD", "Data quality"} {
		if !strings.Contains(out, want) {
			t.Errorf("markdown is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "%!") {
		t.Errorf("format verb error in the markdown output:\n%s", out)
	}
}

// TestEmptyResultRenders: an empty candidate list is a valid answer and must
// render without panicking or printing nonsense.
func TestEmptyResultRenders(t *testing.T) {
	res := &screen.Result{RunID: "r", Profile: "graham_defensive", UniverseSize: 10}
	var b strings.Builder
	if err := render.Screen(&b, res, i18n.For(i18n.EN)); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(b.String(), "No company passed") {
		t.Errorf("unexpected output: %s", b.String())
	}
}

func TestJSONIsIndentedAndUnescaped(t *testing.T) {
	var b strings.Builder
	if err := render.JSON(&b, map[string]string{"url": "https://sec.gov/a?b=1&c=2"}); err != nil {
		t.Fatalf("json: %v", err)
	}
	if strings.Contains(b.String(), `\u0026`) {
		t.Errorf("HTML escaping should be off: %s", b.String())
	}
}
