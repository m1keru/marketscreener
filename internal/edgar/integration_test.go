//go:build integration

// These tests talk to the live SEC and Stooq endpoints. They verify the claims
// the design document flagged as "confirm before relying on them". They never
// run in CI: build with -tags integration and set MSC_CONTACT to the address
// SEC should see in the User-Agent header.
//
//	MSC_CONTACT=you@example.com go test -tags integration ./internal/edgar/ -v
package edgar_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m1ke/marketscreener/internal/edgar"
)

func liveClient(t *testing.T) *edgar.Client {
	t.Helper()
	contact := os.Getenv("MSC_CONTACT")
	if contact == "" {
		t.Skip("set MSC_CONTACT to the contact address SEC should see")
	}
	return edgar.New(edgar.Options{
		UserAgent:         fmt.Sprintf("marketscreener/2.0 (%s)", contact),
		RequestsPerSecond: 10,
		MaxRetries:        5,
		Timeout:           60 * time.Second,
	})
}

// TestLiveFramesNeedSeveralQuarters checks the design's first open claim: one
// quarterly frame does not cover the whole market, because off-calendar filers
// land in neighbouring frames.
func TestLiveFramesNeedSeveralQuarters(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	frames := edgar.RecentFrames(time.Now().UTC(), 5, true)

	counts := map[string]int{}
	union := map[string]bool{}
	for _, id := range frames {
		fr, err := c.Frame(ctx, "us-gaap", "AssetsCurrent", "USD", id)
		if err != nil {
			t.Logf("frame %s: %v", id, err)
			continue
		}
		counts[id.String()] = len(fr.Data)
		for _, d := range fr.Data {
			union[edgar.PadCIK(d.CIK.String())] = true
		}
	}
	t.Logf("per-frame counts: %v", counts)
	t.Logf("union across %d frames: %d companies", len(frames), len(union))

	best := 0
	for _, n := range counts {
		if n > best {
			best = n
		}
	}
	if best == 0 {
		t.Fatal("no frame returned data")
	}
	if len(union) <= best {
		t.Errorf("the union (%d) is not larger than the best single frame (%d): "+
			"querying several frames would be unnecessary", len(union), best)
	}
}

// TestLiveCompanyFactsAndFiscalYear pulls a real registrant with an off-calendar
// fiscal year and checks the history is usable.
func TestLiveCompanyFactsAndFiscalYear(t *testing.T) {
	c := liveClient(t)
	// Microsoft: fiscal year ends 30 June.
	cf, err := c.CompanyFacts(context.Background(), "789019")
	if err != nil {
		t.Fatalf("companyfacts: %v", err)
	}
	facts := cf.Flatten("789019")
	if len(facts) < 100 {
		t.Fatalf("got %d facts, expected a full history", len(facts))
	}
	var eps int
	for _, f := range facts {
		if f.Tag == "EarningsPerShareBasic" {
			eps++
		}
	}
	if eps == 0 {
		t.Error("no EarningsPerShareBasic facts in a real filing history")
	}
	t.Logf("%s: %d facts, %d EPS points", cf.EntityName, len(facts), eps)
}

// TestLiveStooqBatch checks the design's second open claim: Stooq answers a
// 50-symbol batch without a key.
func TestLiveStooqBatch(t *testing.T) {
	if os.Getenv("MSC_CONTACT") == "" {
		t.Skip("set MSC_CONTACT to run the live tests")
	}
	symbols := []string{
		"aapl.us", "msft.us", "ko.us", "pep.us", "jnj.us", "xom.us", "cvx.us", "pfe.us",
		"mrk.us", "abt.us", "mmm.us", "cat.us", "de.us", "ge.us", "hon.us", "ibm.us",
		"intc.us", "csco.us", "orcl.us", "txn.us", "qcom.us", "adi.us", "amat.us", "lrcx.us",
		"nke.us", "sbux.us", "mcd.us", "tgt.us", "wmt.us", "cost.us", "hd.us", "low.us",
		"t.us", "vz.us", "so.us", "duk.us", "d.us", "aep.us", "ed.us", "xel.us",
		"jpm.us", "bac.us", "wfc.us", "c.us", "gs.us", "ms.us", "axp.us", "usb.us",
		"pnc.us", "tfc.us",
	}
	url := fmt.Sprintf("https://stooq.com/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv", strings.Join(symbols, ","))
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("User-Agent", "marketscreener/2.0")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("stooq: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stooq returned HTTP %d for a 50-symbol batch", resp.StatusCode)
	}
	buf := make([]byte, 64*1024)
	n, _ := resp.Body.Read(buf)
	lines := strings.Split(strings.TrimSpace(string(buf[:n])), "\n")
	t.Logf("stooq returned %d lines for %d symbols", len(lines), len(symbols))
	if len(lines) < len(symbols) {
		t.Errorf("got %d lines for %d symbols: the batch was truncated", len(lines), len(symbols))
	}
	priced := 0
	for _, l := range lines[1:] {
		cols := strings.Split(l, ",")
		if len(cols) > 6 && cols[6] != "N/D" {
			priced++
		}
	}
	if priced < len(symbols)/2 {
		t.Errorf("only %d of %d symbols came back with a price", priced, len(symbols))
	}
}
