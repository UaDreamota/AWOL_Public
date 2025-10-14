#AWOL_Research/scraping/court_scraper/geoscrap.py
# ---------------------------------------------------------------------
# pip install pandas geopy
# ---------------------------------------------------------------------

import re
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# =========================
# >>> HARD-CODE PATHS <<<
# =========================
INPUT_CSV    = r"D:\Projects\GitRepositories\TheAWOLthing\AWOL_Research\data\awol_court\index_ad.csv"
OUTPUT_CSV   = r"D:\Projects\GitRepositories\TheAWOLthing\AWOL_Research\data\awol_court\courts_geocoded_ad.csv"
CACHE_JSON   = r"D:\DATA\geocode_cache.json"
COURT_COLUMN = "Назва суду"

# ================
# REFRESH POLICY (optional)
# ================
FORCE_REFRESH_ALL = False         # True => ignore cache and redo everything
CACHE_MIN_CONFIDENCE = 0.80       # redo cached results with conf < this

# ================
# RATE & VERBOSITY
# ================
VERBOSE = True
MIN_DELAY_SECONDS = 1.1

# oblast genitive -> nominative
OBLAST_GEN_TO_NOM = {
    "Вінницької області": "Вінницька область",
    "Волинської області": "Волинська область",
    "Дніпропетровської області": "Дніпропетровська область",
    "Донецької області": "Донецька область",
    "Житомирської області": "Житомирська область",
    "Закарпатської області": "Закарпатська область",
    "Запорізької області": "Запорізька область",
    "Івано-Франківської області": "Івано-Франківська область",
    "Київської області": "Київська область",
    "Кіровоградської області": "Кіровоградська область",
    "Луганської області": "Луганська область",
    "Львівської області": "Львівська область",
    "Миколаївської області": "Миколаївська область",
    "Одеської області": "Одеська область",
    "Полтавської області": "Полтавська область",
    "Рівненської області": "Рівненська область",
    "Сумської області": "Сумська область",
    "Тернопільської області": "Тернопільська область",
    "Харківської області": "Харківська область",
    "Херсонської області": "Херсонська область",
    "Хмельницької області": "Хмельницька область",
    "Черкаської області": "Черкаська область",
    "Чернівецької області": "Чернівецька область",
    "Чернігівської області": "Чернігівська область",
    "Автономної Республіки Крим": "Автономна Республіка Крим",
}
ADJ_ENDINGS = ["ський", "зький", "цький", "йський"]

# Common city genitive→nominative fixes for city-district courts
CITY_GEN_TO_NOM = {
    "Києва": "Київ",
    "Львова": "Львів",
    "Харкова": "Харків",
    "Чернігова": "Чернігів",
    "Миколаєва": "Миколаїв",
    "Дніпра": "Дніпро",
    "Одеси": "Одеса",
    "Полтави": "Полтава",
    "Черкас": "Черкаси",
    "Тернополя": "Тернопіль",
    "Івано-Франківська": "Івано-Франківськ",
    "Ужгорода": "Ужгород",
    "Хмельницького": "Хмельницький",
    "Луцька": "Луцьк",
    "Рівного": "Рівне",
    "Житомира": "Житомир",
    "Запоріжжя": "Запоріжжя",  # already ok
    "Сум": "Суми",             # special
    "Чернівців": "Чернівці",
    "Кропивницького": "Кропивницький",
    "Кам'янця-Подільського": "Кам'янець-Подільський",
}

def canon(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("’", "'")
    return re.sub(r"\s+", " ", s.strip())

def make_key(s: str) -> str:
    return hashlib.sha1(canon(s).lower().encode("utf-8")).hexdigest()

def strip_adj_ending(word: str) -> Optional[str]:
    if not word:
        return None
    for suf in ADJ_ENDINGS:
        if word.endswith(suf):
            base = word[: -len(suf)]
            if base.endswith("ь"):
                base = base[:-1]
            return base
    return None

def extract_oblast_nominative(text: str) -> Optional[str]:
    for gen, nom in OBLAST_GEN_TO_NOM.items():
        if gen in text:
            return nom
    return None

def normalize_city_token(city_raw: Optional[str]) -> List[str]:
    """
    Return a small set of city name variants to try:
    - original token
    - fixed via CITY_GEN_TO_NOM (if known)
    - a couple of simple heuristics for genitive -> nominative
    """
    if not city_raw:
        return []
    variants = []
    city_raw = canon(city_raw)

    # exact as written
    variants.append(city_raw)

    # dictionary fix
    if city_raw in CITY_GEN_TO_NOM:
        variants.append(CITY_GEN_TO_NOM[city_raw])

    # heuristics:
    # 1) ...ова/єва/ева/іва → ...ів  (Львова→Львів, Чернігова→Чернігів, Миколаєва→Миколаїв)
    if re.search(r"(ова|єва|ева|іва)$", city_raw):
        variants.append(re.sub(r"(ова|єва|ева|іва)$", "ів", city_raw))
    # 2) trailing 'а' → drop (Луцька→Луцьк), trailing 'я' → 'я' stays (Одеси already handled)
    if city_raw.endswith("а"):
        variants.append(city_raw[:-1])
    # 3) 'м. Сум' -> 'Суми' (already in dict, but keep)
    if city_raw.endswith("ум"):
        variants.append(city_raw + "и")

    # dedupe while preserving order
    seen = set()
    out = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

def extract_tokens(court_name: str) -> Dict[str, Optional[str]]:
    """
    Returns:
      city_token(s), raion_token_adj, raion_token_base, oblast_nom, kind, city_raion
    city_raion=True if pattern "... районний суд м./міста <City>" is present.
    """
    t = canon(court_name)
    oblast_nom = extract_oblast_nominative(t)

    # detect kind
    kind = None
    for k in ["міськрайонний", "районний", "міський", "окружний"]:
        if re.search(rf"\b{k}\b", t):
            kind = k
            break

    # base token before "... суд"
    m = re.search(r"([A-ЯІЇЄҐa-яієїґ'’\-]+)\s+(?:міськрайонний|районний|міський|окружний)\s+суд", t)
    token = m.group(1) if m else None

    # city after "м." or "міста"
    m_city = re.search(r"(?:м\.|міста)\s+([A-ЯІЇЄҐa-яієїґ'’\-]+)", t)
    city_raw = m_city.group(1) if m_city else None
    city_variants = normalize_city_token(city_raw) if city_raw else []

    # City-district?
    city_raion = bool(city_raw and kind == "районний")

    raion_token_adj = token if kind in {"районний", "міськрайонний"} else None
    city_token = token if kind in {"міський", "міськрайонний", "окружний"} else None
    raion_base = strip_adj_ending(raion_token_adj) if raion_token_adj else None

    return dict(
        city_token=city_token,                # from name before "... суд" (for міський/окружний)
        city_variants=city_variants,          # extracted from "м./міста <City>"
        raion_token_adj=raion_token_adj,
        raion_token_base=raion_base,
        oblast_nom=oblast_nom,
        kind=kind,
        city_raion=city_raion
    )

def build_geocoder():
    g = Nominatim(user_agent="ua-courts-geocoder/3.2 (research use)", timeout=12)
    return RateLimiter(lambda *args, **kw: g.geocode(*args, **kw),
                       min_delay_seconds=MIN_DELAY_SECONDS,
                       swallow_exceptions=False)

def _norm_addr(s: str) -> str:
    return canon(s).lower()

def score_candidate(addr: str,
                    city_any: List[str],
                    raion_adj: Optional[str],
                    raion_base: Optional[str],
                    oblast: Optional[str],
                    district_label: Optional[str]) -> int:
    a = _norm_addr(addr)
    score = 0
    if oblast and _norm_addr(oblast) in a:
        score += 3
    if raion_adj and _norm_addr(raion_adj) in a:
        score += 4
    if raion_base and _norm_addr(raion_base) in a:
        score += 2
    for c in city_any:
        if _norm_addr(c) in a:
            score += 2
            break
    if district_label and _norm_addr(district_label) in a:
        score += 3  # explicit "… район" in address
    if "суд" in a or "courthouse" in a:
        score += 1
    return score

def pick_best(locations, city_any, raion_adj, raion_base, oblast, district_label=None):
    if not locations:
        return None
    locs = locations if isinstance(locations, list) else [locations]
    best, best_score = None, -1
    for loc in locs:
        addr = getattr(loc, "address", "") or getattr(loc, "raw", {}).get("display_name", "")
        sc = score_candidate(addr, city_any, raion_adj, raion_base, oblast, district_label)
        if sc > best_score:
            best, best_score = loc, sc
    return best

def q_geocode(geo, params: dict, limit=5):
    return geo(params, addressdetails=False, language="uk",
               exactly_one=False, limit=limit, country_codes="ua")

# -------- CITY-DISTRICT (район міста) FIRST --------
def geocode_city_district_first(geo,
                                city_any: List[str],
                                district_adj: Optional[str],
                                oblast: Optional[str]) -> Optional[Dict[str, Any]]:
    if not city_any or not district_adj:
        return None
    district_label = f"{district_adj} район"

    # Try both city_district and suburb selectors
    for city in city_any:
        # 1) city_district
        params1 = {
            "amenity": "courthouse",
            "city_district": district_label,
            "city": city,
            "country": "Україна",
        }
        if oblast:
            params1["state"] = oblast
        locs1 = q_geocode(geo, params1)
        best1 = pick_best(locs1, city_any, district_adj, None, oblast, district_label)
        if best1:
            return {
                "lat": best1.latitude, "lon": best1.longitude,
                "display_name": best1.address, "source": "nominatim_city_district",
                "confidence": 0.96, "error": ""
            }

        # 2) suburb (some OSM data encodes districts as suburb)
        params2 = {
            "amenity": "courthouse",
            "suburb": district_label,
            "city": city,
            "country": "Україна",
        }
        if oblast:
            params2["state"] = oblast
        locs2 = q_geocode(geo, params2)
        best2 = pick_best(locs2, city_any, district_adj, None, oblast, district_label)
        if best2:
            return {
                "lat": best2.latitude, "lon": best2.longitude,
                "display_name": best2.address, "source": "nominatim_city_suburb",
                "confidence": 0.94, "error": ""
            }

    return None

# -------- OBLAST/RAION (county) PATHS --------
def geocode_raion_oblast_paths(geo,
                               city_token: Optional[str],
                               raion_adj: Optional[str],
                               raion_base: Optional[str],
                               oblast: Optional[str],
                               city_any: List[str]) -> Optional[Dict[str, Any]]:
    # county (обласний район)
    if oblast and (raion_adj or raion_base):
        county_candidates = []
        if raion_adj:
            county_candidates.append(f"{raion_adj} район")
        if raion_base:
            county_candidates.append(f"{raion_base} район")

        for county in county_candidates:
            params = {"amenity": "courthouse", "county": county, "state": oblast, "country": "Україна"}
            if city_token:
                params["city"] = city_token
            locs = q_geocode(geo, params)
            best = pick_best(locs, city_any or [city_token] if city_token else [], raion_adj, raion_base, oblast, county)
            if best:
                return {"lat": best.latitude, "lon": best.longitude, "display_name": best.address,
                        "source": "nominatim_raion", "confidence": 0.90, "error": ""}

    # city + oblast (міський/окружний)
    if city_token and oblast:
        params2 = {"amenity": "courthouse", "city": city_token, "state": oblast, "country": "Україна"}
        locs2 = q_geocode(geo, params2)
        best2 = pick_best(locs2, [city_token], raion_adj, raion_base, oblast, None)
        if best2:
            return {"lat": best2.latitude, "lon": best2.longitude, "display_name": best2.address,
                    "source": "nominatim_city", "confidence": 0.85, "error": ""}

    # oblast-wide
    if oblast:
        params3 = {"amenity": "courthouse", "state": oblast, "country": "Україна"}
        locs3 = q_geocode(geo, params3)
        best3 = pick_best(locs3, city_any or ([city_token] if city_token else []),
                          raion_adj, raion_base, oblast, None)
        if best3:
            return {"lat": best3.latitude, "lon": best3.longitude, "display_name": best3.address,
                    "source": "nominatim_oblast", "confidence": 0.60, "error": ""}

    return None

# -------- Free-text fallback --------
def geocode_freetext(geo, court_name: str, city_any: List[str], oblast: Optional[str]) -> Optional[Dict[str, Any]]:
    queries = [f"{court_name}, Україна"]
    if "області" in queries[0]:
        queries.append(queries[0].replace("області", "область"))
    for c in city_any:
        if oblast:
            queries.append(f"{c} суд, {oblast}, Україна")
        queries.append(f"{c} районний суд, Україна")

    for q in queries:
        locs = q_geocode(geo, q)
        best = pick_best(locs, city_any, None, None, oblast, None)
        if best:
            return {"lat": best.latitude, "lon": best.longitude, "display_name": best.address,
                    "source": "nominatim_text", "confidence": 0.65, "error": ""}
    return None

def geocode_one(geo, court_name: str) -> Dict[str, Any]:
    t = extract_tokens(court_name)
    city_any = t["city_variants"][:]  # list
    # also include city_token (from before "... суд") if looks like a city name
    if t["city_token"]:
        city_any.append(t["city_token"])
    # dedupe
    city_any = [c for i, c in enumerate(city_any) if c and c not in city_any[:i]]

    last_err = "not found"

    # 1) city-district first (район міста)
    try:
        if t["city_raion"]:
            res = geocode_city_district_first(geo, city_any, t["raion_token_adj"], t["oblast_nom"])
            if res:
                return res
    except Exception as e:
        last_err = f"city_district: {e}"

    # 2) oblast/raion paths
    try:
        res2 = geocode_raion_oblast_paths(geo, t["city_token"], t["raion_token_adj"],
                                          t["raion_token_base"], t["oblast_nom"], city_any)
        if res2:
            return res2
    except Exception as e:
        last_err = f"raion/oblast: {e}"

    # 3) free text
    try:
        res3 = geocode_freetext(geo, court_name, city_any, t["oblast_nom"])
        if res3:
            return res3
    except Exception as e:
        last_err = f"text: {e}"

    return {"lat": "", "lon": "", "display_name": "", "source": "nominatim",
            "confidence": 0.0, "error": last_err}

def load_cache(path: str) -> Dict[str, Any]:
    p = Path(path)
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(path: str, data: Dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    print(f"[INFO] Reading CSV: {INPUT_CSV}", flush=True)
    in_path = Path(INPUT_CSV)
    if not in_path.exists():
        raise FileNotFoundError(f"Missing INPUT_CSV: {in_path}")

    df = pd.read_csv(in_path, encoding="utf-8-sig")
    print(f"[INFO] Rows read: {len(df)}; Columns: {list(df.columns)}", flush=True)

    if COURT_COLUMN not in df.columns:
        raise ValueError(f"Expected column '{COURT_COLUMN}' in {INPUT_CSV}")

    df["_key"] = df[COURT_COLUMN].astype(str).map(make_key)
    key_to_name = (df[[COURT_COLUMN, "_key"]]
                   .dropna()
                   .drop_duplicates("_key")
                   .set_index("_key")[COURT_COLUMN]
                   .to_dict())

    print(f"[INFO] Unique names: {len(key_to_name)}", flush=True)

    cache = load_cache(CACHE_JSON)
    print(f"[INFO] Cache loaded: {len(cache)} entries from {CACHE_JSON}", flush=True)

    geo = build_geocoder()

    results: Dict[str, Dict[str, Any]] = dict(cache)

    def is_bad(r: Dict[str, Any]) -> bool:
        if not r:
            return True
        lat = str(r.get("lat", "")).strip()
        lon = str(r.get("lon", "")).strip()
        conf = float(r.get("confidence", 0) or 0)
        return (lat == "" or lon == "" or conf < CACHE_MIN_CONFIDENCE)

    missing = []
    for k in key_to_name:
        if FORCE_REFRESH_ALL or (k not in results) or is_bad(results.get(k)):
            missing.append(k)

    print(f"[INFO] To geocode: {len(missing)} unique names (rate ~{MIN_DELAY_SECONDS:.1f}s/req)", flush=True)

    total = len(missing)
    for i, k in enumerate(missing, 1):
        name = key_to_name[k]
        if VERBOSE:
            print(f"[{i}/{total}] {name}", flush=True)
        t0 = time.time()
        try:
            results[k] = geocode_one(geo, name)
        except Exception as e:
            results[k] = {"lat": "", "lon": "", "display_name": "",
                          "source": "nominatim", "confidence": 0.0, "error": f"fatal: {e}"}
        dt = time.time() - t0
        if VERBOSE:
            r = results[k]
            print(f"       -> {r['source']} conf={r['confidence']} lat={r['lat']} lon={r['lon']} ({dt:.2f}s)", flush=True)

        cache[k] = results[k]
        if i % 5 == 0 or i == total:
            save_cache(CACHE_JSON, cache)
            if VERBOSE:
                print(f"       [saved cache] {i}/{total}", flush=True)

    res_df = pd.DataFrame(
        [{"_key": k, **v} for k, v in results.items()],
        columns=["_key", "lat", "lon", "display_name", "source", "confidence", "error"]
    )

    out = df.merge(res_df, on="_key", how="left").drop(columns=["_key"])
    out = out.rename(columns={
        "source": "geocode_source",
        "confidence": "geocode_confidence",
        "error": "geocode_error"
    })
    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    ok = (out["lat"].astype(str).str.len() > 0) & (out["lon"].astype(str).str.len() > 0)
    print(f"[DONE] Saved: {OUTPUT_CSV} (rows: {len(out)}); with coords: {ok.sum()}", flush=True)

if __name__ == "__main__":
    main()
