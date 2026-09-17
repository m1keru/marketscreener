package i18n_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/m1ke/marketscreener/internal/i18n"
)

// TestCatalogsCoverTheSameKeys: a missing translation must fail the build, not
// surface as an English sentence in the middle of a Russian report.
func TestCatalogsCoverTheSameKeys(t *testing.T) {
	en := i18n.For(i18n.EN)
	ru := i18n.For(i18n.RU)
	for _, key := range i18n.Keys() {
		if ru.T(key) == en.T(key) && !isIdentifierLike(en.T(key)) {
			t.Errorf("key %q is not translated (still %q)", key, en.T(key))
		}
	}
}

func isIdentifierLike(s string) bool {
	// A handful of messages are the same in both languages by design.
	return s == ">= %.1f"
}

var verb = regexp.MustCompile(`%[#+\-0-9.']*[a-zA-Z]`)

// TestFormatVerbsMatch: the arguments are passed positionally, so a catalog
// whose verbs differ would render garbage such as "%!d(string=…)".
func TestFormatVerbsMatch(t *testing.T) {
	en := i18n.For(i18n.EN)
	ru := i18n.For(i18n.RU)
	for _, key := range i18n.Keys() {
		want := verb.FindAllString(en.T(key), -1)
		got := verb.FindAllString(ru.T(key), -1)
		if strings.Join(want, ",") != strings.Join(got, ",") {
			t.Errorf("key %q: verbs %v in en, %v in ru", key, want, got)
		}
	}
}

func TestParseFallsBackToEnglish(t *testing.T) {
	if i18n.Parse("ru") != i18n.RU {
		t.Error("ru must parse")
	}
	for _, s := range []string{"", "de", "EN"} {
		if i18n.Parse(s) != i18n.EN {
			t.Errorf("%q should fall back to English", s)
		}
	}
}

// TestUnknownKeyIsVisible: a gap must be obvious in the output, never empty.
func TestUnknownKeyIsVisible(t *testing.T) {
	if got := i18n.For(i18n.RU).T("no.such.key"); got != "no.such.key" {
		t.Errorf("unknown key rendered as %q", got)
	}
}
