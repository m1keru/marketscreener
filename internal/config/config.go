// Package config loads the YAML file that holds every tunable threshold.
// Changing a threshold must never require a rebuild.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/m1ke/marketscreener/internal/i18n"
)

// Thresholds are the numeric limits of the graham_defensive profile.
type Thresholds struct {
	MinMarketCapUSD        float64 `yaml:"min_market_cap_usd" json:"min_market_cap_usd"`
	CurrentRatioMin        float64 `yaml:"current_ratio_min" json:"current_ratio_min"`
	UtilityDebtToEquityMax float64 `yaml:"utility_debt_to_equity_max" json:"utility_debt_to_equity_max"`
	EPSPositiveYears       int     `yaml:"eps_positive_years" json:"eps_positive_years"`
	DividendYears          int     `yaml:"dividend_years" json:"dividend_years"`
	EPSGrowthMinPct        float64 `yaml:"eps_growth_min_pct" json:"eps_growth_min_pct"`
	PEMax                  float64 `yaml:"pe_max" json:"pe_max"`
	PBMax                  float64 `yaml:"pb_max" json:"pb_max"`
	GrahamProductMax       float64 `yaml:"graham_product_max" json:"graham_product_max"`
	HistoryYears           int     `yaml:"history_years" json:"history_years"`
}

// Universe controls which registrants enter the pipeline at all.
type Universe struct {
	IncludeFinancials bool `yaml:"include_financials" json:"include_financials"`
}

// HTTP holds the SEC politeness settings. The 10 req/s cap is a hard SEC
// requirement, not a tuning knob: raising it earns a 403.
type HTTP struct {
	RequestsPerSecond float64 `yaml:"requests_per_second" json:"requests_per_second"`
	MaxRetries        int     `yaml:"max_retries" json:"max_retries"`
	TimeoutSeconds    int     `yaml:"timeout_seconds" json:"timeout_seconds"`
}

// Prices configures the quote sources. They are tried in order; the first one
// that answers for a ticker wins. Stooq is the only batching source, so losing
// it means one request per ticker — keep it first unless it is unavailable.
type Prices struct {
	Sources   []string `yaml:"sources" json:"sources"`
	YahooBase string   `yaml:"yahoo_base" json:"yahoo_base"`
	SparkBase string   `yaml:"spark_base" json:"spark_base"`
	StooqBase string   `yaml:"stooq_base" json:"stooq_base"`
	CboeBase  string   `yaml:"cboe_base" json:"cboe_base"`
	// Concurrency bounds parallel quote requests; the per-ticker endpoints
	// answer 429 when pushed.
	Concurrency       int     `yaml:"concurrency" json:"concurrency"`
	RequestsPerSecond float64 `yaml:"requests_per_second" json:"requests_per_second"`
}

// Cache holds cache lifetimes in hours.
type Cache struct {
	FactsTTLHours  int `yaml:"facts_ttl_hours" json:"facts_ttl_hours"`
	PricesTTLHours int `yaml:"prices_ttl_hours" json:"prices_ttl_hours"`
	// CompaniesTTLHours covers the identity and SIC of a registrant, which
	// costs one request each and changes at most once in a company's life.
	// Keeping it long is what makes the second run of the week fast.
	CompaniesTTLHours int `yaml:"companies_ttl_hours" json:"companies_ttl_hours"`
}

// Config is the whole file.
type Config struct {
	// UserAgentContact is the real contact address SEC requires in the
	// User-Agent header. Without it every request comes back 403.
	UserAgentContact string `yaml:"user_agent_contact" json:"user_agent_contact"`
	// Language renders targets, reasons and the markdown report. Field names,
	// criterion ids and statuses stay in English: they are identifiers.
	Language   string     `yaml:"language" json:"language"`
	Profile    string     `yaml:"profile" json:"profile"`
	DBPath     string     `yaml:"db_path" json:"db_path"`
	MaxResults int        `yaml:"max_results" json:"max_results"`
	Thresholds Thresholds `yaml:"thresholds" json:"thresholds"`
	Universe   Universe   `yaml:"universe" json:"universe"`
	Prices     Prices     `yaml:"prices" json:"prices"`
	HTTP       HTTP       `yaml:"http" json:"http"`
	Cache      Cache      `yaml:"cache" json:"cache"`

	// Path records where the config was read from, for diagnostics.
	Path string `yaml:"-" json:"-"`
}

// ProfileGrahamDefensive is the only profile implemented in v1.
const ProfileGrahamDefensive = "graham_defensive"

// Default returns the built-in defaults from the design document.
func Default() Config {
	return Config{
		Language:   string(i18n.EN),
		Profile:    ProfileGrahamDefensive,
		MaxResults: 25,
		Thresholds: Thresholds{
			MinMarketCapUSD:        300_000_000,
			CurrentRatioMin:        2.0,
			UtilityDebtToEquityMax: 2.0,
			EPSPositiveYears:       10,
			DividendYears:          10,
			EPSGrowthMinPct:        33,
			PEMax:                  15,
			PBMax:                  1.5,
			GrahamProductMax:       22.5,
			HistoryYears:           10,
		},
		HTTP: HTTP{RequestsPerSecond: 10, MaxRetries: 5, TimeoutSeconds: 60},
		Prices: Prices{
			Sources:           []string{"yahoo", "cboe"},
			YahooBase:         "https://query1.finance.yahoo.com/v8/finance/chart/",
			SparkBase:         "https://query1.finance.yahoo.com/v7/finance/spark",
			StooqBase:         "https://stooq.com/q/l/",
			CboeBase:          "https://cdn.cboe.com/api/global/delayed_quotes/quotes/",
			Concurrency:       4,
			RequestsPerSecond: 8,
		},
		Cache: Cache{FactsTTLHours: 24, PricesTTLHours: 1, CompaniesTTLHours: 720},
	}
}

// SearchPaths lists where the config is looked for when --config is absent.
func SearchPaths() []string {
	var p []string
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		p = append(p, filepath.Join(x, "marketscreener", "config.yaml"))
	} else if home, err := os.UserHomeDir(); err == nil {
		p = append(p, filepath.Join(home, ".config", "marketscreener", "config.yaml"))
	}
	p = append(p, "config/default.yaml")
	return p
}

// Load reads the config from path, or from the first existing search path when
// path is empty. A missing file is not an error as long as the contact address
// arrives through MSC_CONTACT.
func Load(path string) (Config, error) {
	cfg := Default()
	candidates := []string{path}
	if path == "" {
		candidates = SearchPaths()
	}
	for _, p := range candidates {
		if p == "" {
			continue
		}
		data, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) && path == "" {
			continue
		}
		if err != nil {
			return cfg, fmt.Errorf("read config %s: %w", p, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", p, err)
		}
		cfg.Path = p
		break
	}
	if v := os.Getenv("MSC_CONTACT"); v != "" {
		cfg.UserAgentContact = v
	}
	if cfg.Profile == "" {
		cfg.Profile = ProfileGrahamDefensive
	}
	if v := os.Getenv("MSC_LANG"); v != "" {
		cfg.Language = v
	}
	if cfg.MaxResults <= 0 {
		cfg.MaxResults = 25
	}
	return cfg, nil
}

var contactRe = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

// ErrNoContact is returned when the SEC User-Agent contact is missing or is not
// an address SEC would accept.
var ErrNoContact = errors.New("SEC requires a real contact address in the User-Agent header")

// Validate fails fast, before the first request, so a misconfigured contact
// surfaces as a readable message and not as a 403 in the middle of a run.
func (c Config) Validate() error {
	contact := strings.TrimSpace(c.UserAgentContact)
	if contact == "" || !contactRe.MatchString(contact) {
		return fmt.Errorf(
			"%w: set user_agent_contact: your.name@example.com in the config file "+
				"(searched: %s) or export MSC_CONTACT; SEC blocks anonymous traffic "+
				"with 403 (https://www.sec.gov/os/webmaster-faq#developers)",
			ErrNoContact, strings.Join(SearchPaths(), ", "))
	}
	if c.Profile != ProfileGrahamDefensive {
		return fmt.Errorf("unknown profile %q: only %q is implemented in v1", c.Profile, ProfileGrahamDefensive)
	}
	if c.Language != "" && string(i18n.Parse(c.Language)) != c.Language {
		return fmt.Errorf("unknown language %q: expected one of %s",
			c.Language, strings.Join(i18n.Supported(), ", "))
	}
	if c.Thresholds.HistoryYears < 3 {
		return fmt.Errorf("thresholds.history_years must be >= 3, got %d", c.Thresholds.HistoryYears)
	}
	if len(c.Prices.Sources) == 0 {
		return errors.New("prices.sources is empty: without a quote source, size, P/E and P/B " +
			"can never be decided and every company ends up incomplete")
	}
	for _, src := range c.Prices.Sources {
		switch src {
		case "yahoo", "cboe", "stooq":
		default:
			return fmt.Errorf("unknown price source %q: expected yahoo, cboe or stooq", src)
		}
	}
	if c.HTTP.RequestsPerSecond <= 0 || c.HTTP.RequestsPerSecond > 10 {
		return fmt.Errorf("http.requests_per_second must be in (0, 10], got %v: 10/s is SEC's hard cap", c.HTTP.RequestsPerSecond)
	}
	return nil
}

// CompaniesTTL is the registrant cache lifetime, defaulting to 30 days.
func (c Config) CompaniesTTL() time.Duration {
	h := c.Cache.CompaniesTTLHours
	if h <= 0 {
		h = 720
	}
	return time.Duration(h) * time.Hour
}

// FactsTTL is the XBRL cache lifetime.
func (c Config) FactsTTL() time.Duration {
	h := c.Cache.FactsTTLHours
	if h <= 0 {
		h = 24
	}
	return time.Duration(h) * time.Hour
}

// UserAgent renders the header value SEC requires.
func (c Config) UserAgent() string {
	return fmt.Sprintf("marketscreener/2.0 (%s)", strings.TrimSpace(c.UserAgentContact))
}

// Catalog returns the message catalog for the configured language.
func (c Config) Catalog() *i18n.Catalog { return i18n.For(i18n.Parse(c.Language)) }

// DefaultDBPath is $XDG_DATA_HOME/marketscreener/msc.db, falling back to
// ~/.local/share/marketscreener/msc.db.
func DefaultDBPath() string {
	base := os.Getenv("XDG_DATA_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "msc.db"
		}
		base = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(base, "marketscreener", "msc.db")
}

// ResolveDBPath picks the flag value, then the config value, then the default.
func (c Config) ResolveDBPath(flagValue string) string {
	switch {
	case flagValue != "":
		return flagValue
	case c.DBPath != "":
		return c.DBPath
	default:
		return DefaultDBPath()
	}
}
