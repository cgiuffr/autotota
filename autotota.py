#!/usr/bin/env python3
"""
Citation counts via DBLP (papers) + OpenAlex (counts).

CSV columns:
year,title,doi,url,citations_total,citations_5y,normalized_total_citations,normalized_5y_citations
"""

import csv
import math
import re
import time
import statistics
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ----------------- Config -----------------
DBLP_INDEX = "https://dblp.org/db/conf/<venue>/index"
OUTFILE = "citations_normalized.csv"
USER_AGENT = {"User-Agent": "citations/1.2 (+your-contact)"}

# Optional: restrict years (inclusive). Set to None to fetch all.
YEAR_MIN = None  # e.g., 2010
YEAR_MAX = None  # e.g., 2025

# Politeness / retry
DBLP_DELAY_SEC = 0.2
OPENALEX_DELAY_SEC = 0.1
MAX_RETRIES = 4
TIMEOUT = 45

# Rolling 5-year window relative to "now"
CURRENT_YEAR = datetime.now().year
FIVE_YEAR_CUTOFF = CURRENT_YEAR - 5                # e.g., 2020 if current year is 2025
CUTOFF_DATE = f"{FIVE_YEAR_CUTOFF+1}-01-01"        # start of the next year -> strict last 5 years

# ----------------- Helpers -----------------

def get_soup(url, timeout=TIMEOUT):
    r = requests.get(url, timeout=timeout, headers=USER_AGENT)
    r.raise_for_status()
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

def openalex_totals_and_5y(doi):
    """
    Returns (citations_total, citations_5y) for a DOI using OpenAlex only.
    Fast 5y: read meta['count'] from cited_by_api_url with per-page=1.
    IMPORTANT: append the 5y filter to the existing 'cites:W...' filter in the URL (comma-joined).
    """
    if not doi:
        return 0, 0

    base = "https://api.openalex.org/works/https://doi.org/"
    backoff = 1.0
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(base + doi, headers=USER_AGENT, timeout=TIMEOUT)
            if r.status_code == 200:
                j = r.json()
                total = int(j.get("cited_by_count", 0) or 0)

                five_year = 0
                cited_by_api = j.get("cited_by_api_url")  # e.g., https://api.openalex.org/works?filter=cites:W...
                if cited_by_api:
                    # Build a URL that COMBINES filters with a comma, instead of sending a second 'filter' param
                    if "filter=" in cited_by_api:
                        # add both an integer-year filter and a date filter; either can fail on edge-cases
                        url_5y_year = cited_by_api.replace(
                            "filter=",
                            f"filter=publication_year:>{FIVE_YEAR_CUTOFF},"
                        )
                        url_5y_date = cited_by_api.replace(
                            "filter=",
                            f"filter=from_publication_date:{CUTOFF_DATE},"
                        )
                    else:
                        url_5y_year = f"{cited_by_api}?filter=publication_year:>{FIVE_YEAR_CUTOFF}"
                        url_5y_date = f"{cited_by_api}?filter=from_publication_date:{CUTOFF_DATE}"

                    # Try publication_year filter first
                    for url_5y in (url_5y_year, url_5y_date):
                        cr = requests.get(url_5y, params={"per-page": 1, "select": "id"},
                                          headers=USER_AGENT, timeout=TIMEOUT)
                        if cr.status_code == 200:
                            meta = cr.json().get("meta", {})
                            five_year = int(meta.get("count", 0) or 0)
                            break

                    # Clamp (just in case filtering returns a count > total due to indexing delays)
                    if five_year > total:
                        five_year = total

                return total, five_year

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff); backoff *= 2
                continue
            return 0, 0
        except requests.RequestException:
            time.sleep(backoff); backoff *= 2
    return 0, 0

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
    for i, p in enumerate(papers, 1):
        total, fivey = openalex_totals_and_5y(p["doi"])
        p["citations_total"] = total
        p["citations_5y"] = fivey
        time.sleep(OPENALEX_DELAY_SEC)
        if i % 25 == 0:
            print(f"  ...processed {i}/{len(papers)}")

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

    # 4) Write CSV
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
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

    print(f"Saved {OUTFILE} with {len(papers)} rows.")

if __name__ == "__main__":
    main()
