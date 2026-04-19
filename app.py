#!/usr/bin/env python3
import csv, re
from pathlib import Path
from collections import Counter
from statistics import median
from flask import Flask, jsonify, render_template

app = Flask(__name__)
BASE = Path(__file__).parent


def load_aanbod():
    rows = []
    path = BASE / "roommatch_aanbod_historisch.csv"
    if not path.exists():
        return rows
    seen = set()
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r.get("latitude") or not r.get("longitude"):
                continue
            key = r["id"]
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "id": r["id"],
                "adres": f"{r['street']} {r['houseNumber']}{r['houseNumberAddition']}".strip(),
                "straat": r["street"].strip().lower(),
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
                "corporatie": r["corporatie"],
                "urlKey": r["urlKey"],
            })
    return rows


def parse_inschrijfduur(toewijzing: str):
    """Return dict with dagen (int), voorrang (bool), or None if not inschrijfduur."""
    if not toewijzing.startswith("Inschrijfduur:"):
        return None
    voorrang = toewijzing.strip().endswith("*")
    jaren = re.search(r"(\d+)\s+ja(?:ar|ren)", toewijzing)
    maanden = re.search(r"(\d+)\s+maand(?:en)?", toewijzing)
    dagen = re.search(r"(\d+)\s+dag(?:en)?", toewijzing)
    d = 0
    if jaren:  d += int(jaren.group(1)) * 365
    if maanden: d += int(maanden.group(1)) * 30
    if dagen:  d += int(dagen.group(1))
    return {"dagen": d, "voorrang": voorrang}


def load_verhuurd():
    rows = []
    path = BASE / "roommatch_historisch.csv"
    if not path.exists():
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            raw = r.get("Toewijzing o.b.v. (* is met voorrang)", "")
            # strip geannuleerd entries
            if raw.startswith("Geannuleerd"):
                continue
            inschrijf = parse_inschrijfduur(raw)
            # basis: first word(s) before colon or *
            m = re.match(r"^([^:(]+)", raw)
            basis = m.group(1).strip().rstrip("*").strip() if m else raw
            try:
                reacties = int(r.get("Aantal reacties", ""))
            except:
                reacties = None
            # extract street from adres: everything before first digit
            adres = r.get("Adres", "")
            straat_m = re.match(r"^([A-Za-zÀ-ÿ\s\-'.]+)", adres)
            straat = straat_m.group(1).strip().lower() if straat_m else ""
            rows.append({
                "adres": adres,
                "straat": straat,
                "plaats": r.get("Plaats", "").strip(),
                "reacties": reacties,
                "basis": basis,
                "inschrijfduur": inschrijf,  # {dagen, voorrang} or None
                "contractdatum": r.get("Contractdatum", ""),
            })
    return rows


def match_city(stad: str, v_stad: str) -> bool:
    s, vs = stad.lower(), v_stad.lower()
    if s == "den haag":
        return vs in ("den haag", "'s-gravenhage")
    return s == vs


def days_to_str(d: int) -> str:
    jaren = d // 365
    rest = d % 365
    maanden = rest // 30
    if jaren and maanden:
        return f"{jaren}j {maanden}m"
    if jaren:
        return f"{jaren} jaar"
    return f"{maanden} maanden"


def analyse(listing, verhuurd, aanbod):
    stad = listing["stad"]
    straat = listing["straat"]

    stad_verhuurd = [v for v in verhuurd if match_city(stad, v["plaats"])]

    # --- Zelfde straat ---
    straat_verhuurd = [v for v in stad_verhuurd if v["straat"] == straat]

    # --- Inschrijfduur analyse (zelfde straat first, anders stad) ---
    scope_inschrijf = straat_verhuurd if len(straat_verhuurd) >= 3 else stad_verhuurd
    scope_label = "dit complex" if len(straat_verhuurd) >= 3 else f"{stad}"
    inschrijf_entries = [v["inschrijfduur"] for v in scope_inschrijf if v["inschrijfduur"]]

    inschrijf_zonder_voorrang = [e["dagen"] for e in inschrijf_entries if not e["voorrang"]]
    inschrijf_met_voorrang   = [e["dagen"] for e in inschrijf_entries if e["voorrang"]]

    inschrijf_stats = None
    if inschrijf_zonder_voorrang:
        inschrijf_stats = {
            "min": min(inschrijf_zonder_voorrang),
            "mediaan": int(median(inschrijf_zonder_voorrang)),
            "max": max(inschrijf_zonder_voorrang),
            "aantalVoorrang": len(inschrijf_met_voorrang),
            "aantalZonderVoorrang": len(inschrijf_zonder_voorrang),
            "scope": scope_label,
        }

    # --- Toewijzingsbasis verdeling (zelfde straat of stad) ---
    bases_scope = straat_verhuurd if len(straat_verhuurd) >= 3 else stad_verhuurd
    bases = [v["basis"] for v in bases_scope if v["basis"]]
    basis_teller = Counter(bases)
    totaal_bases = sum(basis_teller.values())
    toewijzing_bases = [
        {"basis": b, "aantal": n, "pct": round(n / totaal_bases * 100)}
        for b, n in basis_teller.most_common(4)
    ]

    # --- Voorrang % (stad) ---
    alle_inschrijf_stad = [v["inschrijfduur"] for v in stad_verhuurd if v["inschrijfduur"]]
    pct_voorrang_stad = None
    if alle_inschrijf_stad:
        n_voorrang = sum(1 for e in alle_inschrijf_stad if e["voorrang"])
        pct_voorrang_stad = round(n_voorrang / len(alle_inschrijf_stad) * 100)

    # --- Reacties in stad ---
    reactie_vals = [v["reacties"] for v in stad_verhuurd if v["reacties"] is not None]
    gem_reacties = round(sum(reactie_vals) / len(reactie_vals)) if reactie_vals else None

    # --- Prijs per m² vs stad ---
    stad_aanbod = [a for a in aanbod if a["stad"] == stad and a["huurTotaal"] and a["oppervlakte"]]
    prijsm2_vals = [a["huurTotaal"] / a["oppervlakte"] for a in stad_aanbod if a["oppervlakte"] > 0]
    gem_prijsm2 = round(sum(prijsm2_vals) / len(prijsm2_vals), 2) if prijsm2_vals else None
    eigen_prijsm2 = round(listing["huurTotaal"] / listing["oppervlakte"], 2) if listing["huurTotaal"] and listing["oppervlakte"] else None

    # --- Top 5 toewijzingen zelfde straat (meest recent) ---
    buurt_lijst = sorted(straat_verhuurd, key=lambda v: v["contractdatum"], reverse=True)[:5]
    buurt_items = []
    for v in buurt_lijst:
        ins = v["inschrijfduur"]
        buurt_items.append({
            "adres": v["adres"],
            "contractdatum": v["contractdatum"],
            "basis": v["basis"],
            "inschrijfduurDagen": ins["dagen"] if ins else None,
            "voorrang": ins["voorrang"] if ins else False,
            "reacties": v["reacties"],
        })

    # --- Stad inschrijfduur stats (min/gem/mediaan/max) ---
    alle_stad_dagen = [e["dagen"] for v in stad_verhuurd
                       for e in [v["inschrijfduur"]] if e and not e["voorrang"]]
    stad_stats = None
    if alle_stad_dagen:
        stad_stats = {
            "min": min(alle_stad_dagen),
            "gem": round(sum(alle_stad_dagen) / len(alle_stad_dagen)),
            "mediaan": int(median(alle_stad_dagen)),
            "max": max(alle_stad_dagen),
            "n": len(alle_stad_dagen),
        }

    return {
        "inschrijfduurStats": inschrijf_stats,
        "toewijzingBases": toewijzing_bases,
        "pctVoorrangStad": pct_voorrang_stad,
        "gemReactiesStad": gem_reacties,
        "aantalVerhuurdStad": len(stad_verhuurd),
        "aantalZelfdeStraat": len(straat_verhuurd),
        "prijsPerM2": eigen_prijsm2,
        "gemPrijsPerM2Stad": gem_prijsm2,
        "buurtLijst": buurt_items,
        "stadStats": stad_stats,
    }


_cache = {}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/aanbod")
def api_aanbod():
    return jsonify(load_aanbod())

@app.route("/api/analyse/<listing_id>")
def api_analyse(listing_id):
    if "verhuurd" not in _cache:
        _cache["verhuurd"] = load_verhuurd()
    if "aanbod" not in _cache:
        _cache["aanbod"] = load_aanbod()
    listing = next((a for a in _cache["aanbod"] if a["id"] == listing_id), None)
    if not listing:
        return jsonify({"error": "niet gevonden"}), 404
    return jsonify(analyse(listing, _cache["verhuurd"], _cache["aanbod"]))

if __name__ == "__main__":
    app.run(debug=True, port=5050)
