// Command msc screens US equities against Graham's defensive criteria using SEC
// EDGAR data. It runs either as an MCP server over stdio or as a plain CLI; both
// modes call the same functions, so their answers cannot diverge.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/m1ke/marketscreener/internal/config"
	"github.com/m1ke/marketscreener/internal/edgar"
	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/mcpsrv"
	"github.com/m1ke/marketscreener/internal/prices"
	"github.com/m1ke/marketscreener/internal/render"
	"github.com/m1ke/marketscreener/internal/screen"
	"github.com/m1ke/marketscreener/internal/store"
)

var (
	flagLang     string
	flagConfig   string
	flagDB       string
	flagFormat   string
	flagLogLevel string
	flagWorkers  int
)

func main() {
	root := &cobra.Command{
		Use:   "msc",
		Short: "Graham-style screening of US equities from SEC EDGAR",
		Long: "msc screens US equities against Benjamin Graham's defensive criteria using SEC EDGAR\n" +
			"filings as the only source of fundamentals. Every criterion returns pass, fail or\n" +
			"unknown, and every number carries the XBRL tag and accession number it came from.",
		// Errors are printed once, by main, to stderr.
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&flagConfig, "config", "", "path to the YAML config (default: search XDG then config/default.yaml)")
	root.PersistentFlags().StringVar(&flagDB, "db", "", "path to the SQLite database (default: $XDG_DATA_HOME/marketscreener/msc.db)")
	root.PersistentFlags().StringVar(&flagFormat, "format", "markdown", "output format: markdown or json")
	root.PersistentFlags().StringVar(&flagLogLevel, "log-level", "info", "log level: debug, info, warn, error")
	root.PersistentFlags().StringVar(&flagLang, "lang", "",
		"output language for reports and reasons: "+strings.Join(i18n.Supported(), " or ")+" (default from config)")
	root.PersistentFlags().IntVar(&flagWorkers, "workers", 8, "concurrent company fetches (the SEC rate limit still applies)")

	root.AddCommand(cmdMCP(), cmdScreen(), cmdExplain(), cmdHistory(), cmdRuns(), cmdFunnel())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// deps holds everything a command needs, wired from the config.
type deps struct {
	cfg      config.Config
	store    *store.Store
	screener *screen.Screener
	log      *slog.Logger
}

// setup validates the config and builds the screener. Logs always go to stderr:
// in MCP mode stdout belongs to the protocol.
func setup() (*deps, func(), error) {
	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(flagLogLevel)); err != nil {
		return nil, nil, fmt.Errorf("bad --log-level %q", flagLogLevel)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg, err := config.Load(flagConfig)
	if err != nil {
		return nil, nil, err
	}
	if flagLang != "" {
		cfg.Language = flagLang
	}
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}
	st, err := store.Open(cfg.ResolveDBPath(flagDB))
	if err != nil {
		return nil, nil, err
	}
	ec := edgar.New(edgar.Options{
		UserAgent:         cfg.UserAgent(),
		RequestsPerSecond: cfg.HTTP.RequestsPerSecond,
		MaxRetries:        cfg.HTTP.MaxRetries,
		Timeout:           time.Duration(cfg.HTTP.TimeoutSeconds) * time.Second,
		Logger:            log,
	})
	pp := prices.New(prices.Options{
		Store:             st,
		TTL:               time.Duration(cfg.Cache.PricesTTLHours) * time.Hour,
		Logger:            log,
		YahooBase:         cfg.Prices.YahooBase,
		SparkBase:         cfg.Prices.SparkBase,
		StooqBase:         cfg.Prices.StooqBase,
		CboeBase:          cfg.Prices.CboeBase,
		Concurrency:       cfg.Prices.Concurrency,
		RequestsPerSecond: cfg.Prices.RequestsPerSecond,
		Sources:           cfg.Prices.Sources,
	})
	sc := &screen.Screener{Cfg: cfg, Edgar: ec, Prices: pp, Store: st, Log: log, Workers: flagWorkers}
	return &deps{cfg: cfg, store: st, screener: sc, log: log}, func() { st.Close() }, nil
}

// emit writes a result in the requested format.
func emit(v any, md func() error) error {
	switch flagFormat {
	case "json":
		return render.JSON(os.Stdout, v)
	case "markdown", "md", "":
		return md()
	default:
		return fmt.Errorf("unknown --format %q: expected markdown or json", flagFormat)
	}
}

func cmdMCP() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Serve the five screening tools over MCP on stdio",
		Long: "Serve screen, explain, history, runs and funnel over the Model Context Protocol.\n" +
			"stdout carries the protocol; every log line goes to stderr, and os.Stdout is\n" +
			"redirected to stderr for the lifetime of the session so stray writes cannot\n" +
			"corrupt the stream.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, cleanup, err := setup()
			if err != nil {
				return err
			}
			defer cleanup()

			realStdout, restore, err := mcpsrv.GuardStdout()
			if err != nil {
				return fmt.Errorf("guard stdout: %w", err)
			}
			defer restore()

			d.log.Info("mcp server starting", "db", d.cfg.ResolveDBPath(flagDB), "config", d.cfg.Path)
			srv := &mcpsrv.Server{Screener: d.screener, Log: d.log}
			if err := srv.Run(cmd.Context(), realStdout); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		},
	}
}

func cmdScreen() *cobra.Command {
	var (
		minCap            float64
		includeFinancials bool
		maxResults        int
		refresh           bool
		noStore           bool
	)
	c := &cobra.Command{
		Use:   "screen",
		Short: "Run the full screen over the US universe",
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, cleanup, err := setup()
			if err != nil {
				return err
			}
			defer cleanup()
			p := screen.Params{MaxResults: maxResults, Refresh: refresh, NoStore: noStore}
			if cmd.Flags().Changed("min-market-cap") {
				p.MinMarketCapUSD = &minCap
			}
			if cmd.Flags().Changed("include-financials") {
				p.IncludeFinancials = &includeFinancials
			}
			res, err := d.screener.Screen(cmd.Context(), p)
			if err != nil {
				return err
			}
			return emit(res, func() error { return render.Screen(os.Stdout, res, d.cfg.Catalog()) })
		},
	}
	c.Flags().Float64Var(&minCap, "min-market-cap", 0, "override the minimum market capitalisation in USD")
	c.Flags().BoolVar(&includeFinancials, "include-financials", false, "include SIC 6000-6799 with unknown liquidity criteria")
	c.Flags().IntVar(&maxResults, "max-results", 0, "maximum candidates to return (default from config)")
	c.Flags().BoolVar(&refresh, "refresh", false, "ignore the EDGAR cache and refetch")
	c.Flags().BoolVar(&noStore, "no-store", false,
		"run without recording the run; for warming the cache before an agent session, "+
			"so the stored day-over-day diff stays intact")
	return c
}

func cmdExplain() *cobra.Command {
	var (
		years   int
		refresh bool
	)
	c := &cobra.Command{
		Use:   "explain TICKER",
		Short: "Show every criterion, value and source for one ticker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, cleanup, err := setup()
			if err != nil {
				return err
			}
			defer cleanup()
			res, err := d.screener.Explain(cmd.Context(), args[0], years, refresh)
			if err != nil {
				return err
			}
			return emit(res, func() error { return render.Explain(os.Stdout, res, d.cfg.Catalog()) })
		},
	}
	c.Flags().IntVar(&years, "years", 0, "years of history (default from config)")
	c.Flags().BoolVar(&refresh, "refresh", false, "ignore the EDGAR cache for this company")
	return c
}

func cmdHistory() *cobra.Command {
	var (
		metric  string
		years   int
		refresh bool
	)
	c := &cobra.Command{
		Use:   "history TICKER",
		Short: "Print one raw annual series for a ticker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, cleanup, err := setup()
			if err != nil {
				return err
			}
			defer cleanup()
			res, err := d.screener.History(cmd.Context(), args[0], metric, years, refresh)
			if err != nil {
				return err
			}
			return emit(res, func() error { return render.History(os.Stdout, res, d.cfg.Catalog()) })
		},
	}
	c.Flags().StringVar(&metric, "metric", "eps", "metric: "+joinMetrics())
	c.Flags().IntVar(&years, "years", 0, "years of history (default from config)")
	c.Flags().BoolVar(&refresh, "refresh", false, "ignore the EDGAR cache for this company")
	return c
}

func joinMetrics() string {
	out := ""
	for i, m := range screen.HistoryMetrics() {
		if i > 0 {
			out += ", "
		}
		out += m
	}
	return out
}

func cmdRuns() *cobra.Command {
	var (
		limit   int
		compare []string
	)
	c := &cobra.Command{
		Use:   "runs",
		Short: "List past runs, or diff two of them",
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, cleanup, err := setup()
			if err != nil {
				return err
			}
			defer cleanup()
			res, err := d.screener.Runs(cmd.Context(), limit, compare)
			if err != nil {
				return err
			}
			return emit(res, func() error { return render.Runs(os.Stdout, res, d.cfg.Catalog()) })
		},
	}
	c.Flags().IntVar(&limit, "limit", 10, "how many runs to list")
	c.Flags().StringSliceVar(&compare, "compare", nil, "two run ids to diff, comma separated")
	return c
}

func cmdFunnel() *cobra.Command {
	var runID string
	c := &cobra.Command{
		Use:   "funnel",
		Short: "Show how many companies each criterion removed",
		RunE: func(cmd *cobra.Command, _ []string) error {
			d, cleanup, err := setup()
			if err != nil {
				return err
			}
			defer cleanup()
			res, err := d.screener.Funnel(cmd.Context(), runID)
			if err != nil {
				return err
			}
			return emit(res, func() error { return render.Funnel(os.Stdout, res, d.cfg.Catalog()) })
		},
	}
	c.Flags().StringVar(&runID, "run-id", "", "run to inspect (default: the most recent)")
	return c
}
