#!/usr/bin/env python3
"""
Pre-compute all analysis and generate docs/data.json for GitHub Pages.
Also tries to cross-reference aanbod (size/price) with verhuurd (inschrijfduur)
by matching normalized street + housenumber — this gets richer over time.
"""
import csv, json, re, shutil
from datetime import datetime
from pathlib import Path
from collections import Counter
from statistics import median

BASE = Path(__file__).parent
DOCS = BASE / "docs"
DOCS.mkdir(exist_ok=True)


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_aanbod():
    path = BASE / "roommatch_aanbod_historisch.csv"
    if not path.exists():
        return []
    seen = set()
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r.get("latitude") or not r.get("longitude"):
                continue
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            rows.append({
                "id": r["id"],
                "adres": f"{r['street']} {r['houseNumber']}{r['houseNumberAddition']}".strip(),
                "straat": r["street"].strip().lower(),
                "huisnummer": r["houseNumber"].strip(),
                "postcode": r["postalcode"],
                "stad": r["stad"],
                "wijk": r["wijk"],
                "lat": float(r["latitude"]),
                "lon": float(r["longitude"]),
                "huurTotaal": float(r["totalRent"]) if r["totalRent"] else None,
                "huurKaal": float(r["netRent"]) if r["netRent"] else None,
                "servicekosten": float(r["serviceCosts"]) if r["serviceCosts"] else None,
                "oppervlakte": int(r["areaDwelling"]) if r["areaDwelling"] else None,
                "energielabel": r["energielabel"].replace("Energielabel ", "") if r["energielabel"] else None,
                "beschikbaarPer": r["availableFrom"].strip() if r["availableFrom"] else None,
                "sluitingsdatum": r["closingDate"][:10] if r["closingDate"] else None,
                "reacties": int(r["numberOfReactions"]) if r["numberOfReactions"] else 0,
                "woningtype": r["woningtype"],
                "verdieping": r["verdieping"],
                "verwarming": r["verwarming"],
                "keuken": r["keuken"],
                "model": r["model"],
                "actielabel": r["actielabel"],
                "doelgroepen": r["doelgroepen"],
                "infoveld": r["infoveldKort"].strip() if r["infoveldKort"] else None,
                "urlKey": r["urlKey"],
            })
    return rows


def parse_inschrijfduur(toewijzing: str):
    if not toewijzing.startswith("Inschrijfduur:"):
        return None
    voorrang = toewijzing.strip().endswith("*")
    jaren   = re.search(r"(\d+)\s+ja(?:ar|ren)", toewijzing)
    maanden = re.search(r"(\d+)\s+maand(?:en)?", toewijzing)
    dagen   = re.search(r"(\d+)\s+dag(?:en)?", toewijzing)
    d = 0
    if jaren:   d += int(jaren.group(1))   * 365
    if maanden: d += int(maanden.group(1)) * 30
    if dagen:   d += int(dagen.group(1))
    return {"dagen": d, "voorrang": voorrang}


def normalize_straat(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def load_verhuurd():
    path = BASE / "roommatch_historisch.csv"
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            raw = r.get("Toewijzing o.b.v. (* is met voorrang)", "")
            if raw.startswith("Geannuleerd"):
                continue
            inschrijf = parse_inschrijfduur(raw)
            m = re.match(r"^([^:(]+)", raw)
            basis = m.group(1).strip().rstrip("*").strip() if m else raw
            try:
                reacties = int(r.get("Aantal reacties", ""))
            except:
                reacties = None
            adres = r.get("Adres", "")
            # Extract street (everything before first digit sequence)
            sm = re.match(r"^([A-Za-zÀ-ÿ\s\-'.]+)", adres)
            straat = normalize_straat(sm.group(1)) if sm else ""
            # Extract housenumber (first digit sequence)
            nm = re.search(r"(\d+)", adres)
            huisnummer = nm.group(1) if nm else ""
            rows.append({
                "adres": adres,
                "straat": straat,
                "huisnummer": huisnummer,
                "plaats": r.get("Plaats", "").strip(),
                "reacties": reacties,
                "basis": basis,
                "inschrijfduur": inschrijf,
                "contractdatum": r.get("Contractdatum", ""),
            })
    return rows


# ── Cross-reference: match verhuurd entries to known aanbod size/price ───────

def build_aanbod_index(aanbod_history_path: Path) -> dict:
    """
    Build a lookup {(straat, huisnummer) -> {oppervlakte, huurTotaal}} from ALL
    historical aanbod rows (not just latest). This way houses that have since
    disappeared from the feed are still indexed.
    """
    index = {}
    if not aanbod_history_path.exists():
        return index
    with open(aanbod_history_path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            straat = normalize_straat(r.get("street", ""))
            huisnummer = r.get("houseNumber", "").strip()
            key = (straat, huisnummer)
            if key not in index and r.get("areaDwelling"):
                index[key] = {
                    "oppervlakte": int(r["areaDwelling"]) if r["areaDwelling"] else None,
                    "huurTotaal": float(r["totalRent"]) if r["totalRent"] else None,
                    "stad": r.get("stad", ""),
                }
    return index


# ── Analysis ─────────────────────────────────────────────────────────────────

def match_city(stad: str, v_stad: str) -> bool:
    s, vs = stad.lower(), v_stad.lower()
    if s == "den haag":
        return vs in ("den haag", "'s-gravenhage")
    return s == vs


def analyse(listing, verhuurd, aanbod, aanbod_index):
    stad   = listing["stad"]
    straat = listing["straat"]

    stad_verhuurd  = [v for v in verhuurd if match_city(stad, v["plaats"])]
    straat_verhuurd = [v for v in stad_verhuurd if v["straat"] == straat]

    # Enrich verhuurd rows with size data from aanbod index
    for v in straat_verhuurd:
        key = (v["straat"], v["huisnummer"])
        match = aanbod_index.get(key)
        if match:
            v["_oppervlakte"] = match["oppervlakte"]
            v["_huurTotaal"]  = match["huurTotaal"]

    # Top 5 buurt items (most recent first)
    buurt_lijst = sorted(straat_verhuurd, key=lambda v: v["contractdatum"], reverse=True)[:5]
    buurt_items = []
    for v in buurt_lijst:
        ins = v["inschrijfduur"]
        buurt_items.append({
            "adres":             v["adres"],
            "contractdatum":     v["contractdatum"],
            "basis":             v["basis"],
            "inschrijfduurDagen": ins["dagen"] if ins else None,
            "voorrang":          ins["voorrang"] if ins else False,
            "reacties":          v["reacties"],
            "oppervlakte":       v.get("_oppervlakte"),
            "huurTotaal":        v.get("_huurTotaal"),
        })

    # Toewijzingsbases
    bases_scope = straat_verhuurd if len(straat_verhuurd) >= 3 else stad_verhuurd
    basis_teller = Counter(v["basis"] for v in bases_scope if v["basis"])
    totaal_bases = sum(basis_teller.values())
    toewijzing_bases = [
        {"basis": b, "aantal": n, "pct": round(n / totaal_bases * 100)}
        for b, n in basis_teller.most_common(4)
    ]

    # Stad inschrijfduur stats (without voorrang)
    alle_stad_dagen = [
        v["inschrijfduur"]["dagen"]
        for v in stad_verhuurd
        if v["inschrijfduur"] and not v["inschrijfduur"]["voorrang"]
    ]
    stad_stats = None
    if alle_stad_dagen:
        stad_stats = {
            "min":     min(alle_stad_dagen),
            "gem":     round(sum(alle_stad_dagen) / len(alle_stad_dagen)),
            "mediaan": int(median(alle_stad_dagen)),
            "max":     max(alle_stad_dagen),
            "n":       len(alle_stad_dagen),
        }

    # Price per m²
    stad_aanbod = [a for a in aanbod if a["stad"] == stad and a["huurTotaal"] and a["oppervlakte"]]
    prijsm2_vals = [a["huurTotaal"] / a["oppervlakte"] for a in stad_aanbod if a["oppervlakte"]]
    gem_prijsm2  = round(sum(prijsm2_vals) / len(prijsm2_vals), 2) if prijsm2_vals else None
    eigen_prijsm2 = round(listing["huurTotaal"] / listing["oppervlakte"], 2) \
        if listing["huurTotaal"] and listing["oppervlakte"] else None

    return {
        "buurtLijst":        buurt_items,
        "aantalZelfdeStraat": len(straat_verhuurd),
        "toewijzingBases":   toewijzing_bases,
        "stadStats":         stad_stats,
        "prijsPerM2":        eigen_prijsm2,
        "gemPrijsPerM2Stad": gem_prijsm2,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def build():
    print("Laden…")
    aanbod   = load_aanbod()
    verhuurd = load_verhuurd()
    aanbod_index = build_aanbod_index(BASE / "roommatch_aanbod_historisch.csv")

    print(f"  {len(aanbod)} actieve listings, {len(verhuurd)} verhuurd-entries, {len(aanbod_index)} geïndexeerde adressen")

    listings_out = []
    for listing in aanbod:
        a = analyse(listing, verhuurd, aanbod, aanbod_index)
        listings_out.append({**listing, "analyse": a})

    output = {
        "gegenereerd_op": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "listings": listings_out,
    }

    out_path = DOCS / "data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    print(f"  Geschreven naar {out_path} ({out_path.stat().st_size // 1024} KB)")

    # Copy static index
    src = BASE / "docs" / "index.html"
    if not src.exists():
        print("  docs/index.html nog niet aangemaakt — run na het genereren van die file")
    else:
        print("  docs/index.html aanwezig")


if __name__ == "__main__":
    build()
