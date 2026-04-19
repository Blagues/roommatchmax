#!/usr/bin/env python3
"""Scrape roommatch.nl/onlangs-verhuurd and append new rows to a persistent CSV."""

import csv
import sys
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

URL = "https://www.roommatch.nl/onlangs-verhuurd"
OUTPUT = Path("roommatch_historisch.csv")
SCRAPED_AT_COL = "Gescraped op"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def fetch_listings():
    resp = requests.get(URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        print("Geen tabel gevonden op de pagina.", file=sys.stderr)
        sys.exit(1)

    header_row = table.find("thead")
    column_names = []
    if header_row:
        column_names = [th.get_text(strip=True) for th in header_row.find_all("th")]

    tbody = table.find("tbody") or table
    rows = []
    for tr in tbody.find_all("tr"):
        cells = [" ".join(td.get_text().split()) for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)

    if not column_names and rows:
        column_names = [f"kolom_{i+1}" for i in range(len(rows[0]))]

    return column_names, rows


def load_existing(columns_with_ts):
    """Return set of existing dedup keys (adres, contractdatum)."""
    existing_keys = set()
    if not OUTPUT.exists():
        return existing_keys

    with open(OUTPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row.get("Adres", ""), row.get("Contractdatum", ""))
            existing_keys.add(key)

    return existing_keys


def main():
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    column_names, rows = fetch_listings()

    # Normalize column names to find address and date columns
    col_lower = [c.lower() for c in column_names]
    addr_idx = next((i for i, c in enumerate(col_lower) if "adres" in c), 0)
    date_idx = next((i for i, c in enumerate(col_lower) if "datum" in c or "contract" in c), 4)

    all_columns = column_names + [SCRAPED_AT_COL]
    existing_keys = load_existing(all_columns)

    is_new_file = not OUTPUT.exists()
    new_count = 0

    with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(all_columns)

        for row in rows:
            padded = row + [""] * (len(column_names) - len(row))
            adres = padded[addr_idx] if addr_idx < len(padded) else ""
            datum = padded[date_idx] if date_idx < len(padded) else ""
            key = (adres, datum)

            if key not in existing_keys:
                writer.writerow(padded + [scraped_at])
                existing_keys.add(key)
                new_count += 1

    total = sum(1 for _ in open(OUTPUT, encoding="utf-8")) - 1  # minus header
    print(f"{new_count} nieuwe rijen toegevoegd. Totaal in {OUTPUT}: {total} rijen.")


if __name__ == "__main__":
    main()
