#!/usr/bin/env python3
"""Scrape roommatch.nl actueel aanbod via API en sla op in persistent CSV."""

import csv
import json
from datetime import datetime
from pathlib import Path

import requests

API_URL = "https://roommatching-aanbodapi.zig365.nl/api/v1/actueel-aanbod"
OUTPUT = Path("roommatch_aanbod_historisch.csv")
SCRAPED_AT_COL = "gescraped_op"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://www.roommatch.nl/aanbod/studentenwoningen",
    "Accept": "application/json",
}

# Velden die we willen exporteren (platte velden + geneste extracties)
FLAT_FIELDS = [
    "id", "street", "houseNumber", "houseNumberAddition", "postalcode",
    "totalRent", "netRent", "serviceCosts", "heatingCosts", "eenmaligeKosten",
    "areaDwelling", "constructionYear", "isZelfstandig",
    "availableFromDate", "availableFromOriginalDate", "availableFrom",
    "publicationDate", "closingDate",
    "numberOfReactions", "huurtoeslagMogelijk",
    "toewijzingModelTypeInCode", "infoveldKort",
    "latitude", "longitude", "urlKey",
]

NESTED_FIELDS = {
    "stad": ("city", "name"),
    "gemeente": ("municipality", "name"),
    "wijk": ("quarter", "name"),
    "buurt": ("neighborhood", "name"),
    "corporatie": ("corporation", "code"),
    "woningtype": ("dwellingType", "name"),
    "energielabel": ("energyLabel", "localizedNaam"),
    "verwarming": ("heating", "localizedName"),
    "slaapkamers": ("sleepingRoom", "localizedName"),
    "keuken": ("kitchen", "localizedName"),
    "verdieping": ("floor", "localizedName"),
    "actielabel": ("actionLabel", "localizedLabel"),
    "model": ("model", "code"),
    "woningsoort": ("woningsoort", "icon"),
}


def extract(listing: dict) -> dict:
    row = {f: listing.get(f) for f in FLAT_FIELDS}
    for col, (key, subkey) in NESTED_FIELDS.items():
        obj = listing.get(key)
        row[col] = obj.get(subkey) if isinstance(obj, dict) else None
    # Doelgroepen als kommalijst
    doelgroepen = listing.get("doelgroepen", []) or []
    row["doelgroepen"] = ", ".join(d.get("code", "") for d in doelgroepen)
    return row


def fetch_all() -> list[dict]:
    listings = []
    page = 0
    while True:
        r = requests.get(API_URL, headers=HEADERS, params={"page": page, "limit": 100}, timeout=15)
        r.raise_for_status()
        data = r.json()
        batch = data.get("data", [])
        listings.extend(batch)
        meta = data.get("_metadata", {})
        total = meta.get("total_count", 0)
        if len(listings) >= total or not batch:
            break
        page += 1
    return listings


def load_existing_ids() -> set:
    if not OUTPUT.exists():
        return set()
    with open(OUTPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Dedup op (id + publicationDate) zodat herplaatsingen ook worden bijgehouden
        return {(row.get("id", ""), row.get("publicationDate", "")) for row in reader}


def main():
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"Ophalen van aanbod...")
    listings = fetch_all()
    print(f"{len(listings)} woningen opgehaald.")

    all_columns = FLAT_FIELDS + list(NESTED_FIELDS.keys()) + ["doelgroepen", SCRAPED_AT_COL]
    existing_keys = load_existing_ids()
    is_new = not OUTPUT.exists()

    new_count = 0
    with open(OUTPUT, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_columns, extrasaction="ignore")
        if is_new:
            writer.writeheader()

        for listing in listings:
            row = extract(listing)
            key = (str(row.get("id", "")), str(row.get("publicationDate", "")))
            if key not in existing_keys:
                row[SCRAPED_AT_COL] = scraped_at
                writer.writerow(row)
                existing_keys.add(key)
                new_count += 1

    total_rows = sum(1 for _ in open(OUTPUT, encoding="utf-8")) - 1
    print(f"{new_count} nieuwe rijen toegevoegd. Totaal in {OUTPUT}: {total_rows} rijen.")


if __name__ == "__main__":
    main()
