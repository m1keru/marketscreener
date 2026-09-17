package edgar_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m1ke/marketscreener/internal/edgar"
)

const testUA = "marketscreener/2.0 (tester@example.com)"

func newClient(t *testing.T, srv *httptest.Server, rps float64) *edgar.Client {
	t.Helper()
	return edgar.New(edgar.Options{
		UserAgent:         testUA,
		RequestsPerSecond: rps,
		MaxRetries:        4,
		Timeout:           5 * time.Second,
		DataHost:          srv.URL,
		WWWHost:           srv.URL,
	})
}

// writeGzipJSON answers the way SEC does when the client asks for gzip.
func writeGzipJSON(w http.ResponseWriter, r *http.Request, v any) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		http.Error(w, "client did not ask for gzip", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("Content-Type", "application/json")
	zw := gzip.NewWriter(w)
	defer zw.Close()
	_ = json.NewEncoder(zw).Encode(v)
}

func TestGzipAndUserAgent(t *testing.T) {
	var sawUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawUA = r.Header.Get("User-Agent")
		writeGzipJSON(w, r, map[string]any{
			"0": map[string]any{"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
			"1": map[string]any{"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
		})
	}))
	defer srv.Close()

	got, err := newClient(t, srv, 10).CompanyTickers(context.Background())
	if err != nil {
		t.Fatalf("CompanyTickers: %v", err)
	}
	if sawUA != testUA {
		t.Errorf("User-Agent = %q, want %q", sawUA, testUA)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	// The map is unordered on the wire; the client must sort it.
	if got[0].CIK != "0000320193" || got[1].CIK != "0000789019" {
		t.Errorf("CIKs = %s, %s; want zero-padded and sorted", got[0].CIK, got[1].CIK)
	}
}

func TestForbiddenIsFatalAndExplainsUserAgent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	c := newClient(t, srv, 10)
	_, err := c.CompanyTickers(context.Background())
	if err == nil {
		t.Fatal("expected an error on 403")
	}
	if !strings.Contains(err.Error(), "contact address") {
		t.Errorf("403 error should explain the User-Agent requirement, got: %v", err)
	}
	if n := c.Stats.Requests.Load(); n != 1 {
		t.Errorf("403 must not be retried, made %d requests", n)
	}
}

func TestRetriesOn429(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		writeGzipJSON(w, r, map[string]any{"cik": 320193, "entityName": "Apple Inc.", "facts": map[string]any{}})
	}))
	defer srv.Close()

	c := edgar.New(edgar.Options{
		UserAgent: testUA, RequestsPerSecond: 10, MaxRetries: 4,
		Timeout: 5 * time.Second, DataHost: srv.URL, WWWHost: srv.URL,
		// Backoff starts at one second; the test only needs to see that the
		// call eventually succeeds and that the 429s were counted.
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cf, err := c.CompanyFacts(ctx, "320193")
	if err != nil {
		t.Fatalf("CompanyFacts: %v", err)
	}
	if cf.EntityName != "Apple Inc." {
		t.Errorf("entityName = %q", cf.EntityName)
	}
	if got := c.Stats.TooMany.Load(); got != 2 {
		t.Errorf("counted %d 429s, want 2", got)
	}
	if got := c.Stats.Requests.Load(); got != 3 {
		t.Errorf("made %d requests, want 3", got)
	}
}

func TestRateLimitIsRespected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeGzipJSON(w, r, map[string]any{"cik": 1, "entityName": "X", "facts": map[string]any{}})
	}))
	defer srv.Close()

	// 5 requests per second: four calls must take at least 600ms.
	c := newClient(t, srv, 5)
	start := time.Now()
	for i := 0; i < 4; i++ {
		if _, err := c.CompanyFacts(context.Background(), "1"); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 600*time.Millisecond {
		t.Errorf("4 requests at 5/s took %v, expected at least 600ms", elapsed)
	}
}

// TestFramesMergeKeepsFreshestPerCIK covers the trap the design calls out: a
// single quarterly frame misses companies whose fiscal year ends off-calendar,
// so several frames are queried and the freshest fact per CIK wins.
func TestFramesMergeKeepsFreshestPerCIK(t *testing.T) {
	frames := map[string]any{
		"CY2026Q2I": map[string]any{"data": []map[string]any{
			{"cik": 101, "end": "2026-06-30", "val": 150.0, "accn": "a-2", "form": "10-Q"},
		}},
		"CY2026Q1I": map[string]any{"data": []map[string]any{
			{"cik": 101, "end": "2026-03-31", "val": 100.0, "accn": "a-1", "form": "10-Q"},
			{"cik": 202, "end": "2026-01-31", "val": 900.0, "accn": "b-1", "form": "10-K"},
		}},
	}
	var requested []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimSuffix(r.URL.Path, ".json"), "/")
		id := parts[len(parts)-1]
		requested = append(requested, id)
		body, ok := frames[id]
		if !ok {
			http.NotFound(w, r) // frame not published yet
			return
		}
		writeGzipJSON(w, r, body)
	}))
	defer srv.Close()

	c := newClient(t, srv, 10)
	ids := []edgar.FrameID{
		{Year: 2026, Quarter: 3, Instant: true}, // not published: 404
		{Year: 2026, Quarter: 2, Instant: true},
		{Year: 2026, Quarter: 1, Instant: true},
	}
	got, err := c.LatestFromFrames(context.Background(), "us-gaap", "AssetsCurrent", "USD", ids)
	if err != nil {
		t.Fatalf("LatestFromFrames: %v", err)
	}
	if len(requested) != 3 {
		t.Errorf("queried %d frames, want 3: %v", len(requested), requested)
	}
	if len(got) != 2 {
		t.Fatalf("got %d companies, want 2 (the off-calendar filer must survive)", len(got))
	}
	if f := got["0000000101"]; f.Val != 150 || f.End != "2026-06-30" {
		t.Errorf("company 101 = %+v, want the freshest fact (150 at 2026-06-30)", f)
	}
	if f := got["0000000202"]; f.Val != 900 {
		t.Errorf("company 202 = %+v, want the January filer from the older frame", f)
	}
}

func TestFrameIDStrings(t *testing.T) {
	cases := []struct {
		id   edgar.FrameID
		want string
	}{
		{edgar.FrameID{Year: 2026, Quarter: 2, Instant: true}, "CY2026Q2I"},
		{edgar.FrameID{Year: 2026, Quarter: 2}, "CY2026Q2"},
		{edgar.FrameID{Year: 2025}, "CY2025"},
	}
	for _, c := range cases {
		if got := c.id.String(); got != c.want {
			t.Errorf("FrameID %+v = %q, want %q", c.id, got, c.want)
		}
	}
	now := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	q := edgar.RecentFrames(now, 5, true)
	want := []string{"CY2026Q3I", "CY2026Q2I", "CY2026Q1I", "CY2025Q4I", "CY2025Q3I"}
	for i := range want {
		if q[i].String() != want[i] {
			t.Errorf("RecentFrames[%d] = %s, want %s", i, q[i], want[i])
		}
	}
	a := edgar.AnnualFrames(now, 3)
	if a[0].String() != "CY2025" || a[2].String() != "CY2023" {
		t.Errorf("AnnualFrames = %v, want CY2025..CY2023", a)
	}
}

func TestPadCIK(t *testing.T) {
	for in, want := range map[string]string{
		"320193": "0000320193", "0000320193": "0000320193",
		"CIK0000320193": "0000320193", "1": "0000000001",
	} {
		if got := edgar.PadCIK(in); got != want {
			t.Errorf("PadCIK(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCompanyFactsFlatten(t *testing.T) {
	doc := `{"cik":101,"entityName":"X","facts":{
	  "us-gaap":{"EarningsPerShareBasic":{"units":{"USD/shares":[
	    {"start":"2024-01-01","end":"2024-12-31","val":3.6,"accn":"a1","fy":2024,"fp":"FY","form":"10-K","filed":"2025-02-15"}]}}},
	  "dei":{"EntityCommonStockSharesOutstanding":{"units":{"shares":[
	    {"end":"2024-12-31","val":1000,"accn":"a1","fy":2024,"fp":"FY","form":"10-K","filed":"2025-02-15"}]}}}}}`
	var cf edgar.CompanyFacts
	if err := json.Unmarshal([]byte(doc), &cf); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	facts := cf.Flatten("101")
	if len(facts) != 2 {
		t.Fatalf("got %d facts, want 2", len(facts))
	}
	var sawDEI bool
	for _, f := range facts {
		if f.CIK != "0000000101" {
			t.Errorf("CIK = %q, want padded", f.CIK)
		}
		if f.Tag == "dei:EntityCommonStockSharesOutstanding" {
			sawDEI = true
			if !f.Instant() {
				t.Error("share count must be an instant fact")
			}
		}
	}
	if !sawDEI {
		t.Error("the dei namespace must be preserved in the tag name")
	}
}

// TestCIKAcceptsStringOrNumber: EDGAR serves companyfacts with "cik" as a
// number for most registrants and as a zero-padded string for others. Both must
// decode, and a type mismatch must never be retried five times with backoff.
func TestCIKAcceptsStringOrNumber(t *testing.T) {
	for _, body := range []string{
		`{"cik":1867102,"entityName":"N","facts":{}}`,
		`{"cik":"0001867102","entityName":"N","facts":{}}`,
	} {
		var cf edgar.CompanyFacts
		if err := json.Unmarshal([]byte(body), &cf); err != nil {
			t.Fatalf("decode %s: %v", body, err)
		}
		if got := cf.CIK.Int(); got != 1867102 {
			t.Errorf("cik = %d from %s, want 1867102", got, body)
		}
	}
}

func TestDecodeMismatchIsNotRetried(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		// "facts" is an object in reality; a list can never decode.
		writeGzipJSON(w, r, map[string]any{"cik": 1, "entityName": "N", "facts": []int{1, 2}})
	}))
	defer srv.Close()

	c := newClient(t, srv, 10)
	if _, err := c.CompanyFacts(context.Background(), "1"); err == nil {
		t.Fatal("expected a decode error")
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("made %d requests, want 1: a type mismatch is deterministic", n)
	}
}
