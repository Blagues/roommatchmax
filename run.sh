#!/bin/bash
set -e
cd "$(dirname "$0")"

PYTHON=$(which python3)

echo "[$(date '+%Y-%m-%d %H:%M')] Scraping..."
$PYTHON scrape.py      >> "$(dirname "$0")/logs.txt" 2>&1
$PYTHON scrape_aanbod.py >> "$(dirname "$0")/logs.txt" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M')] Building static site..."
$PYTHON build.py >> "$(dirname "$0")/logs.txt" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M')] Pushing to GitHub..."
git add docs/data.json roommatch_historisch.csv roommatch_aanbod_historisch.csv >> "$(dirname "$0")/logs.txt" 2>&1
git commit -m "data: scrape $(date '+%Y-%m-%d %H:%M')" >> "$(dirname "$0")/logs.txt" 2>&1 || echo "Nothing to commit"
git push >> "$(dirname "$0")/logs.txt" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M')] Done."
