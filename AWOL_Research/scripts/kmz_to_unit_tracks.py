#!/usr/bin/env python3
"""
KMZ → Daily unit tracks → Weekly unit–hex weights (w_{uht})  — v3.1 (points + lines + polygons + overlays diag)

What’s new in v3.1 (since v3)
-----------------------------
- **Name filtering controls**: `--accept-all` to keep *all* features (for frontline/control), or `--name-filter` (regex) to whitelist names. If neither is set, the old `is_unit_like()` heuristic is used.
- **Overlay-aware diagnostics**: `--diag` now prints counts of **GroundOverlay** and **NetworkLink** tags by parsing raw KML XML, plus the KML entry path used inside each KMZ.
- **Better logging**: logs when a file has *no vector geometries* or when features were skipped due to name filtering.
- Still: recursive KMZ discovery, robust KML walking across fastkml versions, date parsed from full path, lines/polygons support.

Outputs (unchanged)
-------------------
- data_work/units_raw.parquet              # one row per kept feature×h3 (for points/lines/polygons)
- data_out/unit_tracks_daily.parquet       # unit×date×h3 with daily weights `w_day`
- data_out/weights_u_h_t.parquet           # unit×week×h3 with weekly weights `w` (sum=1 per unit×week)
- data_out/unit_tracks_qc.csv              # speed/jump anomalies (centroided)

Usage examples
--------------
# 1) Frontline/control harvest (keep everything, prefer lines)
python kmz_to_unit_tracks_v3.py \
  --in AWOL_Research/data/kmz/UAControlMapBackups-master \
  --weeks AWOL_Research/data/spine/weeks.parquet \
  --h3res 5 \
  --geom auto \
  --accept-all \
  --stepkm 5 \
  --out AWOL_Research/data/spine \
  --work AWOL_Research/data/spine \
  --diag

# 2) Keep only features whose names match a regex (e.g., brigades)
python kmz_to_unit_tracks_v3.py ... --name-filter "(ОМБР|БРИГАД)"
"""
from __future__ import annotations
import argparse
import os
import re
import sys
import zipfile
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from fastkml import kml
import h3
from xml.etree import ElementTree as ET

# try to import shapely type hints but do not require explicit use
try:
    from shapely.geometry import LineString, Polygon
except Exception:  # pragma: no cover
    LineString = Polygon = object  # type: ignore

# ------------------------- regex helpers -------------------------
DATE_PATTERNS = [
    re.compile(r"(?<!\d)(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)(?!\d)"),  # YYYYMMDD or YYYY-MM-DD/_
    re.compile(r"(?<!\d)([0-3]\d)[-_]?([01]\d)[-_]?(20\d{2})(?!\d)"),  # DDMMYYYY or DD-MM-YYYY
]

UNIT_PATTERNS = [
    r"(?:в\/ч|військова\s+частина)\s*[A-Za-zА-Яа-я0-9\-\/]+",
    r"\b\d{2,}\s*(?:ОМБР|ОМПБ|МБР|ПДВ|БРИГАДА|БРИГ|ПОЛК|БАТАЛЬЙОН|ОМБ|ОБР|ОМБР\.|БР)\b",
]
UNIT_RE = re.compile("|".join(f"({p})" for p in UNIT_PATTERNS), re.IGNORECASE)

# ------------------------- generic utils -------------------------

def log(msg: str):
    print(msg, file=sys.stderr)


def parse_date_from_name(path_or_name: str) -> Optional[datetime]:
    for pat in DATE_PATTERNS:
        m = pat.search(path_or_name)
        if m:
            if pat is DATE_PATTERNS[0]:
                y, M, d = m.groups()
            else:
                d, M, y = m.groups()
            try:
                return datetime(int(y), int(M), int(d))
            except ValueError:
                return None
    return None


def find_kml_bytes_in_kmz(kmz_path: str) -> Tuple[Optional[bytes], Optional[str]]:
    with zipfile.ZipFile(kmz_path, 'r') as z:
        cands = [n for n in z.namelist() if n.lower().endswith('.kml')]
        if not cands:
            return None, None
        # prefer the shortest path (often doc.kml); fallback to first
        name = sorted(cands, key=len)[0]
        return z.read(name), name


def raw_kml_tag_inventory(kml_bytes: bytes) -> Dict[str,int]:
    counts = {"Placemark":0, "Point":0, "LineString":0, "Polygon":0, "GroundOverlay":0, "NetworkLink":0}
    try:
        root = ET.fromstring(kml_bytes)
    except Exception:
        return counts
    for el in root.iter():
        tag = el.tag.rsplit('}',1)[-1]
        if tag in counts:
            counts[tag] += 1
    return counts


def _children(obj) -> List[object]:
    feats = getattr(obj, 'features', None)
    if feats is None:
        return []
    if callable(feats):
        try:
            return list(feats())
        except TypeError:
            return list(feats)
    return list(feats)


def iter_placemarks(doc: kml.KML) -> Iterable[Tuple[str, Optional[str], object]]:
    """Yield (name, description, geometry) for all placemarks with geometry.
       Handles nested Folders/Documents and MultiGeometry.
    """
    stack = _children(doc)
    while stack:
        f = stack.pop()
        stack.extend(_children(f))
        name = getattr(f, 'name', None)
        desc = getattr(f, 'description', None)
        geom = getattr(f, 'geometry', None)
        if geom is None:
            continue
        geoms = getattr(geom, 'geoms', None)
        if geoms is not None:
            for g in geoms:
                yield (name or '', desc, g)
        else:
            yield (name or '', desc, geom)


def name_passes(name: str, desc: Optional[str], accept_all: bool, name_filter: Optional[re.Pattern]) -> bool:
    if accept_all:
        return True
    if name_filter is not None:
        return bool(name_filter.search(name or ''))
    # fallback to heuristic for unit-like labels
    text = f"{name or ''} {desc or ''}"
    return bool(UNIT_RE.search(text)) or (name and len(name.strip()) >= 2)


def canonicalize_unit(name: str) -> str:
    s = re.sub(r"\s+", " ", name or '').strip()
    s = s.replace("·", "-")
    s = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", s)  # zero-width
    s = re.sub(r"[^A-Za-zА-Яа-я0-9\-_\/\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.upper()


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dlat = p2 - p1
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(p1)*np.cos(p2)*np.sin(dlon/2)**2
    return 2*R*np.arcsin(np.sqrt(a))

# ------------------------- geometry helpers -------------------------

def densify_line_coords(coords_lonlat: List[Tuple[float,float]], step_km: float) -> List[Tuple[float,float]]:
    """Densify a LineString given as [(lon,lat), ...] with roughly step_km spacing (linear interpolation)."""
    if len(coords_lonlat) <= 1:
        return coords_lonlat
    out: List[Tuple[float,float]] = [coords_lonlat[0]]
    for (lon1, lat1), (lon2, lat2) in zip(coords_lonlat[:-1], coords_lonlat[1:]):
        dist = haversine_km(lat1, lon1, lat2, lon2)
        n = max(1, int(dist // max(step_km, 0.1)))
        for i in range(1, n+1):
            t = i / n
            lon = lon1 + t*(lon2 - lon1)
            lat = lat1 + t*(lat2 - lat1)
            out.append((lon, lat))
    return out


def polygon_to_h3cells(polygon: Polygon, res: int) -> List[str]:
    """Return H3 cells covering the polygon outer ring using h3 polyfill."""
    try:
        gj = polygon.__geo_interface__
        try:
            cells = list(h3.polygon_to_cells(gj, res, geo_json_conformant=True))  # h3>=4
        except Exception:
            cells = list(h3.polyfill(gj, res, geo_json_conformant=True))         # h3<4
        return cells
    except Exception:
        coords = list(getattr(polygon.exterior, 'coords', []))
        samples = densify_line_coords(list(coords), step_km=3.0)
        seen: Dict[str,int] = {}
        for lon, lat in samples:
            try:
                seen[h3.geo_to_h3(lat, lon, res)] = 1
            except Exception:
                continue
        return list(seen.keys())

# ------------------------- ETL -------------------------

def ingest_kmz_folder(in_dir: str, h3res: int, geom_mode: str, step_km: float, diag: bool, accept_all: bool, name_filter_pat: Optional[str]) -> pd.DataFrame:
    kmz_files: List[str] = []
    for root, _, files in os.walk(in_dir):
        for f in files:
            if f.lower().endswith('.kmz'):
                kmz_files.append(os.path.join(root, f))
    kmz_files.sort()
    if not kmz_files:
        raise FileNotFoundError(f"No .kmz files found under: {in_dir}")

    name_filter = re.compile(name_filter_pat, re.IGNORECASE) if name_filter_pat else None

    records = []
    for path in kmz_files:
        date_guess = parse_date_from_name(path)
        try:
            kml_bytes, kml_name = find_kml_bytes_in_kmz(path)
        except zipfile.BadZipFile:
            log(f"[WARN] Bad KMZ (zip) file: {path}")
            continue
        if not kml_bytes:
            log(f"[WARN] No KML found in {path}")
            continue
        if diag:
            tag_counts = raw_kml_tag_inventory(kml_bytes)
            log(f"[KML] {os.path.basename(path)} uses {kml_name}; tags={tag_counts}")
        doc = kml.KML()
        try:
            doc.from_string(kml_bytes)
        except Exception as e:
            log(f"[WARN] KML parse failed for {path}: {e}")
            continue

        counts = {"Point":0, "LineString":0, "Polygon":0, "Other":0}
        geoms: List[Tuple[str, Optional[str], object]] = []
        for name, desc, geom in iter_placemarks(doc):
            gt = getattr(geom, 'geom_type', '')
            if gt in counts:
                counts[gt] += 1
            else:
                counts["Other"] += 1
            geoms.append((name, desc, geom))
        if diag:
            log(f"[INV] {os.path.basename(path)} -> {counts}")

        mode = geom_mode
        if mode == 'auto':
            mode = 'point' if counts['Point']>0 else ('line' if counts['LineString']>0 else ('polygon' if counts['Polygon']>0 else 'point'))

        kept = 0
        skipped_name = 0
        for name, desc, geom in geoms:
            if not name_passes(name, desc, accept_all, name_filter):
                skipped_name += 1
                continue
            unit_id = canonicalize_unit(name)
            gt = getattr(geom, 'geom_type', '')
            if mode == 'point' and gt == 'Point':
                try:
                    lon, lat = float(geom.x), float(geom.y)
                    h = h3.geo_to_h3(lat, lon, h3res)
                    records.append({'file': os.path.basename(path), 'path': path, 'date': (date_guess or datetime.utcfromtimestamp(0)).date(), 'unit_name_raw': name or '', 'unit_id': unit_id, 'h3': h, 'geom_type': 'Point'})
                    kept += 1
                except Exception:
                    pass
            elif mode == 'line' and gt == 'LineString':
                try:
                    coords = list(getattr(geom, 'coords', []))
                    dense = densify_line_coords(coords, step_km)
                    seen = {h3.geo_to_h3(lat, lon, h3res) for lon, lat in dense}
                    for cell in seen:
                        records.append({'file': os.path.basename(path), 'path': path, 'date': (date_guess or datetime.utcfromtimestamp(0)).date(), 'unit_name_raw': name or '', 'unit_id': unit_id, 'h3': cell, 'geom_type': 'LineString'})
                    kept += len(seen)
                except Exception:
                    pass
            elif mode == 'polygon' and gt == 'Polygon':
                try:
                    cells = polygon_to_h3cells(geom, h3res)
                    for cell in cells:
                        records.append({'file': os.path.basename(path), 'path': path, 'date': (date_guess or datetime.utcfromtimestamp(0)).date(), 'unit_name_raw': name or '', 'unit_id': unit_id, 'h3': cell, 'geom_type': 'Polygon'})
                    kept += len(cells)
                except Exception:
                    pass
        log(f"[INFO] {os.path.basename(path)}: kml={kml_name}, mode={mode}, kept={kept}, skipped_name={skipped_name}")

    if not records:
        return pd.DataFrame(columns=['file','path','date','unit_name_raw','unit_id','h3','geom_type'])
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    return df


def daily_unit_hex_weights(units_raw: pd.DataFrame) -> pd.DataFrame:
    df = (units_raw.dropna(subset=['unit_id','h3']).drop_duplicates(subset=['unit_id','date','h3']))
    grp = df.groupby(['unit_id','date','h3']).size().rename('n').reset_index()
    grp['w_day'] = grp['n'] / grp.groupby(['unit_id','date'])['n'].transform('sum')
    return grp[['unit_id','date','h3','w_day']]


def weekly_weights(daily_w: pd.DataFrame, weeks_tbl: Optional[pd.DataFrame]) -> pd.DataFrame:
    d = daily_w.copy()
    d['week_start'] = d['date'] - pd.to_timedelta(d['date'].dt.weekday, unit='D')
    d['iso_year_week'] = d['week_start'].dt.strftime('%G-W%V')
    if weeks_tbl is not None and 'week_id' in weeks_tbl.columns:
        wmap = weeks_tbl.set_index('iso_year_week')['week_id']
        d['week_id'] = d['iso_year_week'].map(wmap)
    else:
        d['week_id'] = pd.factorize(d['iso_year_week'])[0] + 1
    ww = (d.groupby(['unit_id','week_id','h3'])['w_day'].sum().rename('w_sum').reset_index())
    ww['w'] = ww.groupby(['unit_id','week_id'])['w_sum'].transform(lambda x: x / x.sum())
    return ww.drop(columns=['w_sum']).sort_values(['unit_id','week_id','h3'])


def qc_speed(daily_w: pd.DataFrame, speed_thresh_km_per_day: float = 120.0) -> pd.DataFrame:
    # centroid per unit×date (weighted by w_day)
    cent_rows = []
    for (u, d), g in daily_w.groupby(['unit_id','date']):
        lats, lons, ws = [], [], []
        for h, w in zip(g['h3'].values, g['w_day'].values):
            lat, lon = h3.h3_to_geo(h)
            lats.append(lat); lons.append(lon); ws.append(w)
        lat = np.average(lats, weights=ws)
        lon = np.average(lons, weights=ws)
        cent_rows.append({'unit_id': u, 'date': d, 'lat': lat, 'lon': lon})
    cent = pd.DataFrame(cent_rows).sort_values(['unit_id','date'])
    cent['lat_prev'] = cent.groupby('unit_id')['lat'].shift(1)
    cent['lon_prev'] = cent.groupby('unit_id')['lon'].shift(1)
    cent['days'] = (cent['date'] - cent.groupby('unit_id')['date'].shift(1)).dt.days
    cent['dist_km'] = haversine_km(cent['lat_prev'], cent['lon_prev'], cent['lat'], cent['lon'])
    cent['km_per_day'] = cent['dist_km'] / cent['days']
    anomalies = cent[(cent['days'] > 0) & (cent['km_per_day'] > speed_thresh_km_per_day)].copy()
    anomalies['note'] = 'Improbable jump; check unit aliasing or bad geocode'
    return anomalies[['unit_id','date','dist_km','km_per_day','note']]

# ------------------------- CLI -------------------------

def main():
    ap = argparse.ArgumentParser(description='Process daily KMZ of unit locations into weekly unit–hex weights.')
    ap.add_argument('--in', dest='in_dir', required=True, help='Input folder with .kmz files (searches recursively)')
    ap.add_argument('--weeks', dest='weeks', default=None, help='Parquet weeks table (optional)')
    ap.add_argument('--h3res', dest='h3res', type=int, default=5, help='H3 resolution (5≈13.5 km, 6≈5.7 km)')
    ap.add_argument('--geom', dest='geom', choices=['auto','point','line','polygon'], default='auto', help='Geometry mode to harvest')
    ap.add_argument('--stepkm', dest='stepkm', type=float, default=5.0, help='Densification step for lines (km)')
    ap.add_argument('--out', dest='out_dir', default='data_out', help='Output folder (default data_out)')
    ap.add_argument('--work', dest='work_dir', default='data_work', help='Working folder (default data_work)')
    ap.add_argument('--speed-thresh', dest='speed_thresh', type=float, default=120.0, help='QC threshold (km/day)')
    ap.add_argument('--diag', dest='diag', action='store_true', help='Print geometry inventory per KMZ')
    ap.add_argument('--accept-all', dest='accept_all', action='store_true', help='Keep features regardless of name (for frontline/control)')
    ap.add_argument('--name-filter', dest='name_filter', default=None, help='Regex: keep only feature names that match (case-insensitive)')
    args = ap.parse_args()

    in_dir = os.path.abspath(args.in_dir)
    if not os.path.isdir(in_dir):
        raise FileNotFoundError(f"Input dir not found: {in_dir}")

    os.makedirs(args.out_dir, exist_ok=True)
    os.makedirs(args.work_dir, exist_ok=True)

    log('[1/5] Ingesting KMZ…')
    raw = ingest_kmz_folder(in_dir, args.h3res, args.geom, args.stepkm, args.diag, args.accept_all, args.name_filter)
    if raw.empty:
        log('No records found. Check KMZ structure and patterns (overlay-only KMLs will produce nothing).')
        sys.exit(1)
    raw.to_parquet(os.path.join(args.work_dir, 'units_raw.parquet'), index=False)

    log('[2/5] Daily unit–hex weights…')
    daily = daily_unit_hex_weights(raw)
    daily.to_parquet(os.path.join(args.out_dir, 'unit_tracks_daily.parquet'), index=False)

    log('[3/5] Weekly weights…')
    weeks_tbl = None
    if args.weeks and os.path.exists(args.weeks):
        weeks_tbl = pd.read_parquet(args.weeks)
        if 'iso_year_week' not in weeks_tbl.columns:
            weeks_tbl['iso_year_week'] = pd.to_datetime(weeks_tbl['week_start']).dt.strftime('%G-W%V')
    weekly = weekly_weights(daily, weeks_tbl)
    weekly.to_parquet(os.path.join(args.out_dir, 'weights_u_h_t.parquet'), index=False)

    log('[4/5] QC anomalies…')
    qc = qc_speed(daily, args.speed_thresh)
    qc.to_csv(os.path.join(args.out_dir, 'unit_tracks_qc.csv'), index=False)

    log('[5/5] Done.')
    log(f"Raw:    {os.path.join(args.work_dir, 'units_raw.parquet')}")
    log(f"Daily:  {os.path.join(args.out_dir, 'unit_tracks_daily.parquet')}")
    log(f"Weekly: {os.path.join(args.out_dir, 'weights_u_h_t.parquet')}")
    log(f"QC:     {os.path.join(args.out_dir, 'unit_tracks_qc.csv')}")

if __name__ == '__main__':
    main()
