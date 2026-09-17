#!/usr/bin/env python3
"""Generate the trimmed companyfacts fixtures used by the unit tests.

The shapes match data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json exactly; the
numbers are synthetic so the expected verdicts are unambiguous and the tests
never need the network. Re-run with: python3 testdata/gen.py
"""
import json, os

OUT = os.path.dirname(os.path.abspath(__file__))

def pt(start, end, val, fy, form="10-K", accn=None, filed=None):
    p = {"end": end, "val": val, "accn": accn or f"0000000000-{fy}-000001",
         "fy": fy, "fp": "FY", "form": form, "filed": filed or f"{fy+1}-02-15"}
    if start:
        p["start"] = start
    return p

def annual(years, value_of, fye=(1, 1, 12, 31), unit="USD/shares", tag_form="10-K"):
    """Duration facts, one per fiscal year."""
    sm, sd, em, ed = fye
    out = []
    for y in years:
        start = f"{y}-{sm:02d}-{sd:02d}"
        end_year = y if em >= sm else y + 1
        end = f"{end_year}-{em:02d}-{ed:02d}"
        out.append(pt(start, end, value_of(y), y))
    return out

def instant(years, value_of, em=12, ed=31, shift=0):
    """Balance facts, one per fiscal year end. shift aligns the balance date
    with the fiscal year when the year starts in one calendar year and ends in
    the next."""
    out = []
    for y in years:
        out.append(pt(None, f"{y + shift}-{em:02d}-{ed:02d}", value_of(y), y))
    return out

def facts(cik, name, tags):
    us_gaap, dei = {}, {}
    for (taxonomy, tag, unit), points in tags.items():
        bucket = dei if taxonomy == "dei" else us_gaap
        bucket[tag] = {"label": tag, "description": tag, "units": {unit: points}}
    # EDGAR serves cik as a number in companyfacts, zero-padded only in URLs.
    doc = {"cik": int(cik), "entityName": name, "facts": {}}
    if us_gaap:
        doc["facts"]["us-gaap"] = us_gaap
    if dei:
        doc["facts"]["dei"] = dei
    return doc

def write(fname, doc):
    with open(os.path.join(OUT, fname), "w") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
        f.write("\n")
    print("wrote", fname)

YEARS = list(range(2015, 2025))          # FY2015..FY2024
EPS = {2015: 2.00, 2016: 2.10, 2017: 2.20, 2018: 2.50, 2019: 2.70,
       2020: 2.60, 2021: 2.90, 2022: 3.20, 2023: 3.40, 2024: 3.60}
SHARES = 100_000_000
EQUITY = 5_000_000_000
CUR_ASSETS = 3_000_000_000
CUR_LIABS = 1_000_000_000
LTD = 1_500_000_000

def base_tags(eps=None, eps_tag="EarningsPerShareBasic",
              equity_tag="StockholdersEquity", ltd_tag="LongTermDebtNoncurrent",
              div_tag="CommonStockDividendsPerShareDeclared", div_of=None,
              shares_dei=True, current=True, fye=(1, 1, 12, 31), em=12, ed=31, shift=0):
    eps = eps or EPS
    div_of = div_of or (lambda y: 1.00)
    t = {
        ("us-gaap", eps_tag, "USD/shares"): annual(YEARS, lambda y: eps[y], fye),
        ("us-gaap", div_tag, "USD/shares"): [p for p in annual(YEARS, div_of, fye) if p["val"] > 0],
        ("us-gaap", equity_tag, "USD"): instant(YEARS, lambda y: EQUITY, em, ed, shift),
        ("us-gaap", ltd_tag, "USD"): instant(YEARS, lambda y: LTD, em, ed, shift),
        ("us-gaap", "Assets", "USD"): instant(YEARS, lambda y: 8_000_000_000, em, ed, shift),
        ("us-gaap", "Revenues", "USD"): annual(YEARS, lambda y: 6_000_000_000, fye),
    }
    if current:
        t[("us-gaap", "AssetsCurrent", "USD")] = instant(YEARS, lambda y: CUR_ASSETS, em, ed, shift)
        t[("us-gaap", "LiabilitiesCurrent", "USD")] = instant(YEARS, lambda y: CUR_LIABS, em, ed, shift)
    if shares_dei:
        t[("dei", "EntityCommonStockSharesOutstanding", "shares")] = instant(YEARS, lambda y: SHARES, em, ed, shift)
    else:
        t[("us-gaap", "CommonStockSharesOutstanding", "shares")] = instant(YEARS, lambda y: SHARES, em, ed, shift)
    return t

# 1. passes every criterion
write("pass_all.json", facts("0000000101", "GOODCO INC", base_tags()))

# 2. one loss year in the middle of the decade
eps_loss = dict(EPS); eps_loss[2019] = -0.50
write("negative_eps.json", facts("0000000102", "CYCLICO INC", base_tags(eps=eps_loss)))

# 3. an insurer: no current asset/liability split at all
write("financial_no_current.json", facts("0000000103", "INSURECO INC", base_tags(current=False)))

# 4. every primary tag missing, only the fallbacks present
write("fallback_tags.json", facts("0000000104", "FALLBACKCO INC", base_tags(
    eps_tag="EarningsPerShareDiluted",
    equity_tag="StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ltd_tag="LongTermDebt",
    div_tag="CommonStockDividendsPerShareCashPaid",
    shares_dei=False)))

# 5. fiscal year ending 30 June
write("fiscal_year_june.json", facts("0000000105", "JUNECO INC", base_tags(
    fye=(7, 1, 6, 30), em=6, ed=30, shift=1)))

# 6. dividend suspended for one year in the middle
write("dividend_gap.json", facts("0000000106", "GAPCO INC", base_tags(
    div_of=lambda y: 0.0 if y == 2020 else 1.00)))
