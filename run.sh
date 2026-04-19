#!/bin/bash
set -e
cd /Users/kenzoheijman/dev/roommatchtool

PYTHON=/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

echo "[$(date '+%Y-%m-%d %H:%M')] Scraping..."
$PYTHON scrape.py      >> logs.txt 2>&1
$PYTHON scrape_aanbod.py >> logs.txt 2>&1

echo "[$(date '+%Y-%m-%d %H:%M')] Building static site..."
$PYTHON build.py >> logs.txt 2>&1

echo "[$(date '+%Y-%m-%d %H:%M')] Pushing to GitHub..."
git add docs/data.json roommatch_historisch.csv roommatch_aanbod_historisch.csv >> logs.txt 2>&1
git commit -m "data: scrape $(date '+%Y-%m-%d %H:%M')" >> logs.txt 2>&1 || echo "Nothing to commit"
git push >> logs.txt 2>&1

echo "[$(date '+%Y-%m-%d %H:%M')] Done."
