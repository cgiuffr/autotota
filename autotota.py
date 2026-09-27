#!/usr/bin/env python3
"""
Citation counts via DBLP (papers) + OpenAlex (counts).

CSV columns:
year,title,doi,url,citations_total,citations_5y,normalized_total_citations,normalized_5y_citations
"""

import csv
import html
import math
import os
import re
import time
import statistics
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

# ----------------- Config -----------------
DBLP_INDEX = "https://dblp.org/db/conf/<venue>/index"
OUTFILE = "citations_normalized.csv"
USER_AGENT = {"User-Agent": "citations/1.2 (+your-contact)"}

# OpenAlex API key (free: https://help.openalex.org/api/authentication/). Without one, requests
# draw on a small daily budget shared by everyone on your IP address.
OPENALEX_API_KEY = None

# Optional: restrict years (inclusive). Set to None to fetch all.
YEAR_MIN = None  # e.g., 2010
YEAR_MAX = None  # e.g., 2025

# Politeness / retry
DBLP_DELAY_SEC = 1.0
OPENALEX_DELAY_SEC = 0.1
OPENALEX_BATCH_SIZE = 50  # DOIs per request; a batch costs the same as a single lookup
MAX_RETRIES = 4
TIMEOUT = 45

# Rolling 5-year window relative to "now"
CURRENT_YEAR = datetime.now().year
FIVE_YEAR_CUTOFF = CURRENT_YEAR - 5                # e.g., 2020 if current year is 2025

# ----------------- Helpers -----------------

DBLP_SESSION = requests.Session()
DBLP_SESSION.headers.update(USER_AGENT)
ANUBIS_REFRESH_RX = re.compile(r'(\d+);\s*url=([^"\s]*anubis/api/pass-challenge[^"\s]*)', re.I)

def dblp_get(url, timeout=TIMEOUT):
    # DBLP rate-limits with 429 + Retry-After, or by dropping connections; back off and retry.
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = DBLP_SESSION.get(url, timeout=timeout)
        except requests.ConnectionError:
            if attempt == MAX_RETRIES:
                raise
            wait = 60 * (attempt + 1)
            print(f"DBLP dropped the connection, waiting {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code != 429 or attempt == MAX_RETRIES:
            break
        retry_after = r.headers.get("Retry-After", "")
        wait = int(retry_after) if retry_after.isdigit() else 30 * (attempt + 1)
        print(f"DBLP rate limit hit, waiting {wait}s...")
        time.sleep(wait)
    r.raise_for_status()
    return r

def get_soup(url, timeout=TIMEOUT):
    r = dblp_get(url, timeout)
    # DBLP sits behind an Anubis bot check; its meta-refresh challenge is passed by
    # following the refresh link (sent as a Refresh header or a <meta> tag), which
    # sets a cookie reused by the session.
    m = ANUBIS_REFRESH_RX.search(r.headers.get("Refresh", "")) or ANUBIS_REFRESH_RX.search(r.text)
    if m:
        time.sleep(int(m.group(1)))
        r = dblp_get(urljoin(url, html.unescape(m.group(2))), timeout)
    if "anubis_challenge" in r.text:
        raise RuntimeError(f"Could not pass DBLP bot check for {url}")
    return BeautifulSoup(r.text, "html.parser")

def extract_proceedings_links_and_years(index_url):
    soup = get_soup(index_url)
    links = []
    for a in soup.select("a"):
        if a.get_text(strip=True) == "[contents]":
            href = a.get("href")
            if not href:
                continue
            m = re.search(r"(\d{4})", href)
            year = int(m.group(1)) if m else None
            if YEAR_MIN and year and year < YEAR_MIN:
                continue
            if YEAR_MAX and year and year > YEAR_MAX:
                continue
            links.append((href, year))
    links.sort(key=lambda t: (t[1] or 0))
    return links

def extract_papers_from_proceedings(url, year_hint=None):
    psoup = get_soup(url)
    papers = []
    doi_rx = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)
    entries = psoup.select("li.entry.inproceedings") or psoup.select("li.entry")

    for entry in entries:
        title_el = entry.select_one("span.title")
        title = title_el.get_text(" ", strip=True) if title_el else None
        if not title:
            continue
        raw = entry.decode()
        m = doi_rx.search(raw)
        doi = m.group(0) if m else None
        year = year_hint or (int(re.search(r"(\d{4})", url).group(1)) if re.search(r"(\d{4})", url) else None)
        papers.append({"year": year, "title": title, "doi": doi})
    return papers

OPENALEX_WORKS = "https://api.openalex.org/works"
OPENALEX_FIELDS = "doi,cited_by_count,counts_by_year"

def openalex_get(url, params):
    """GET from the OpenAlex API with retries. Returns the parsed JSON, or None if not found."""
    headers = dict(USER_AGENT)
    if OPENALEX_API_KEY:
        headers["Authorization"] = f"Bearer {OPENALEX_API_KEY}"
    backoff = 1.0
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff); backoff *= 2
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None
        retry_after = r.headers.get("Retry-After", "")
        if r.status_code == 429 and retry_after.isdigit() and int(retry_after) > 60:
            # Daily request budget used up: stop rather than record 0 citations for the remaining papers.
            raise RuntimeError(f"OpenAlex budget exhausted: {r.text}")
        if r.status_code not in (429, 500, 502, 503, 504) or attempt == MAX_RETRIES:
            break
        time.sleep(int(retry_after) if retry_after.isdigit() else backoff); backoff *= 2
    raise RuntimeError(f"OpenAlex request failed with HTTP {r.status_code}: {r.text[:500]}")

def citation_counts(work):
    """
    Returns (citations_total, citations_5y) for an OpenAlex work record.
    5y: sum counts_by_year (citations binned by the citing work's publication year, last ten years)
    over the years after FIVE_YEAR_CUTOFF.
    """
    total = int(work.get("cited_by_count", 0) or 0)
    five_year = sum(int(c.get("cited_by_count", 0) or 0) for c in work.get("counts_by_year") or []
                    if (c.get("year") or 0) > FIVE_YEAR_CUTOFF)
    # Clamp (just in case the per-year counts exceed the total due to indexing delays)
    return total, min(five_year, total)

def openalex_counts(dois):
    """
    Returns {doi (lowercase): (citations_total, citations_5y)} using OpenAlex only; DOIs unknown to
    OpenAlex are left out. Looks DOIs up OPENALEX_BATCH_SIZE at a time with an OR filter, then one by
    one for DOIs no batch returned: works too new for the filter index, and DOIs containing the
    filter separators ',' or '|'.
    """
    dois = sorted({d.lower() for d in dois if d})
    batchable = [d for d in dois if "," not in d and "|" not in d]
    counts = {}
    for i in range(0, len(batchable), OPENALEX_BATCH_SIZE):
        batch = batchable[i:i + OPENALEX_BATCH_SIZE]
        # per-page above the batch size leaves room for duplicate records
        j = openalex_get(OPENALEX_WORKS, {"filter": "doi:" + "|".join(batch), "per-page": 200,
                                          "select": OPENALEX_FIELDS})
        for work in (j or {}).get("results", []):
            doi = (work.get("doi") or "").lower().removeprefix("https://doi.org/")
            c = citation_counts(work)
            # OpenAlex sometimes has several records for one DOI (e.g., the paper and its preprint);
            # keep the most-cited one.
            if doi and c > counts.get(doi, (-1, -1)):
                counts[doi] = c
        print(f"  ...processed {min(i + OPENALEX_BATCH_SIZE, len(batchable))}/{len(batchable)}")
        time.sleep(OPENALEX_DELAY_SEC)

    missing = [d for d in dois if d not in counts]
    if missing:
        print(f"  ...looking up {len(missing)} DOIs one by one")
    for doi in missing:
        work = openalex_get(f"{OPENALEX_WORKS}/https://doi.org/{doi}", {"select": OPENALEX_FIELDS})
        if work:
            counts[doi] = citation_counts(work)
        time.sleep(OPENALEX_DELAY_SEC)
    return counts

# ----------------- Main -----------------

def main():
    # 1) Collect papers from DBLP
    proc_links = extract_proceedings_links_and_years(DBLP_INDEX)
    if not proc_links:
        print("No proceedings links found on DBLP.")
        return

    papers = []
    for plink, year in proc_links:
        print(f"Fetching proceedings {year}: {plink}")
        papers.extend(extract_papers_from_proceedings(plink, year_hint=year))
        time.sleep(DBLP_DELAY_SEC)

    print(f"Found {len(papers)} papers across {len(proc_links)} proceedings.")

    # 2) Get total and 5y citation counts from OpenAlex
    print(f"Fetching citation counts from OpenAlex ({'with' if OPENALEX_API_KEY else 'no'} API key)...")
    counts = openalex_counts(p["doi"] for p in papers)
    for p in papers:
        p["citations_total"], p["citations_5y"] = counts.get((p["doi"] or "").lower(), (0, 0))

    # 3) Normalize per publication year (log(c+1) minus year median)
    def medians_for(key):
        by_year = {}
        for p in papers:
            y = p.get("year")
            if y is None:
                continue
            by_year.setdefault(y, []).append(math.log1p(p.get(key, 0)))
        return {y: statistics.median(vals) for y, vals in by_year.items() if vals}

    med_total = medians_for("citations_total")
    med_5y = medians_for("citations_5y")

    for p in papers:
        y = p.get("year")
        p["normalized_total_citations"] = math.log1p(p["citations_total"]) - med_total.get(y, 0.0)
        p["normalized_5y_citations"] = math.log1p(p["citations_5y"]) - med_5y.get(y, 0.0)
        p["url"] = f"https://doi.org/{p['doi']}" if p.get("doi") else ""

    # 4) Write CSV to a temp file first, so the results survive if OUTFILE can't be replaced
    #    (e.g., it is open in Excel)
    base, ext = os.path.splitext(OUTFILE)
    tmpfile = f"{base}.{datetime.now():%Y%m%d-%H%M%S}{ext}"
    with open(tmpfile, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "year", "title", "doi", "url",
            "citations_total", "citations_5y",
            "normalized_total_citations", "normalized_5y_citations"
        ])
        for p in sorted(papers, key=lambda x: (x["year"] or 0, x["title"] or "")):
            w.writerow([
                p.get("year"),
                p.get("title"),
                p.get("doi") or "",
                p.get("url") or "",
                p.get("citations_total", 0),
                p.get("citations_5y", 0),
                f"{p.get('normalized_total_citations', 0.0):.6f}",
                f"{p.get('normalized_5y_citations', 0.0):.6f}",
            ])

    try:
        os.replace(tmpfile, OUTFILE)
    except OSError as e:
        print(f"Could not save {OUTFILE} ({e.strerror or e}). Results are in {tmpfile}")
        return
    print(f"Saved {OUTFILE} with {len(papers)} rows.")

if __name__ == "__main__":
    main()
