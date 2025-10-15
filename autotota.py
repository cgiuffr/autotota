#!/usr/bin/env python3
"""
Citation counts via DBLP (papers) + OpenAlex (counts).

Output columns:
year,title,doi,url,citations,normalized_citations
"""

import csv
import math
import re
import time
import statistics
import requests
from bs4 import BeautifulSoup

# ----------------- Config -----------------
DBLP_INDEX = "https://dblp.org/db/conf/<venue>/index"
OUTFILE = "citations_normalized.csv"
USER_AGENT = {"User-Agent": "citations/1.0"}

# Optional: restrict years (inclusive). Set to None to fetch all.
YEAR_MIN = None  # e.g., 2010
YEAR_MAX = None  # e.g., 2025

# Politeness / retry
DBLP_DELAY_SEC = 0.2
OPENALEX_DELAY_SEC = 0.12
MAX_RETRIES = 4

# ----------------- Helpers -----------------

def get_soup(url, timeout=30):
    r = requests.get(url, timeout=timeout, headers=USER_AGENT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def extract_proceedings_links_and_years(index_url):
    """
    Returns list of (proceedings_url, year) from the DBLP <venue> index page.
    """
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
    # Newest first on DBLP; sort ascending just to be deterministic
    links.sort(key=lambda t: (t[1] or 0))
    return links

def extract_papers_from_proceedings(url, year_hint=None):
    """
    Parse a DBLP proceedings page, return list of dicts with year, title, doi.
    Keeps entries even without a DOI.
    """
    psoup = get_soup(url)
    papers = []
    doi_rx = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)

    entries = psoup.select("li.entry.inproceedings")
    # Some years may use different markup; include generic list items as fallback
    if not entries:
        entries = psoup.select("li.entry")

    for entry in entries:
        title_el = entry.select_one("span.title")
        title = title_el.get_text(" ", strip=True) if title_el else None
        if not title:
            continue

        raw = entry.decode()
        m = doi_rx.search(raw)
        doi = m.group(0) if m else None

        year = year_hint
        if year is None:
            um = re.search(r"(\d{4})", url)
            if um:
                year = int(um.group(1))

        papers.append({"year": year, "title": title, "doi": doi})
    return papers

def openalex_cited_by_count_from_doi(doi):
    """
    DOI → OpenAlex cited_by_count (int). If no DOI, returns 0.
    Only OpenAlex is used for counts.
    """
    if not doi:
        return 0
    base = "https://api.openalex.org/works/https://doi.org/"
    backoff = 1.0
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(base + doi, headers=USER_AGENT, timeout=30)
            if r.status_code == 200:
                j = r.json()
                return int(j.get("cited_by_count", 0) or 0)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff *= 2
                continue
            # Non-retryable
            return 0
        except requests.RequestException:
            time.sleep(backoff)
            backoff *= 2
    return 0

# ----------------- Main -----------------

def main():
    # 1) Collect all <venue> papers from DBLP (per year)
    proc_links = extract_proceedings_links_and_years(DBLP_INDEX)
    if not proc_links:
        print("No proceedings links found on DBLP index; check the URL or connectivity.")
        return

    papers = []
    for plink, year in proc_links:
        year_info = f" {year}" if year else ""
        print(f"Fetching proceedings{year_info}: {plink}")
        papers.extend(extract_papers_from_proceedings(plink, year_hint=year))
        time.sleep(DBLP_DELAY_SEC)

    # 2) Fetch citations from OpenAlex (DOI-based only)
    print(f"Found {len(papers)} papers across {len(proc_links)} proceedings pages.")
    for i, p in enumerate(papers, 1):
        p["citations"] = openalex_cited_by_count_from_doi(p["doi"])
        time.sleep(OPENALEX_DELAY_SEC)
        if i % 25 == 0:
            print(f"  ...processed {i}/{len(papers)}")

    # 3) Compute per-year median of log(c+1), then normalized values
    by_year = {}
    for p in papers:
        y = p.get("year")
        if y is None:
            continue
        by_year.setdefault(y, []).append(math.log1p(p["citations"]))

    median_log_by_year = {y: statistics.median(vals) for y, vals in by_year.items() if vals}

    for p in papers:
        y = p.get("year")
        clog = math.log1p(p["citations"])
        med = median_log_by_year.get(y, 0.0)
        p["normalized_citations"] = clog - med
        p["url"] = f"https://doi.org/{p['doi']}" if p.get("doi") else ""

    # 4) Write CSV
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["year", "title", "doi", "url", "citations", "normalized_citations"])
        for p in sorted(papers, key=lambda x: (x["year"] or 0, x["title"] or "")):
            w.writerow([
                p.get("year"),
                p.get("title"),
                p.get("doi") or "",
                p.get("url") or "",
                p.get("citations", 0),
                f"{p.get('normalized_citations', 0.0):.6f}",
            ])

    print(f"Saved {OUTFILE} with {len(papers)} rows.")

if __name__ == "__main__":
    main()
