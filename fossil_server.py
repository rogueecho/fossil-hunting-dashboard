#!/usr/bin/env python3
"""
Fossil Hunting Dashboard — Calvert Cliffs / Flag Pond
On-demand web interface with live scoring and charts.
Visit http://<host>:5003
"""

from flask import Flask, jsonify, Response
from datetime import datetime, timedelta
import json, math, os, time, threading, urllib.request, urllib.error
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────
# Default site (Calvert/Flag Pond)
NOAA_STATION    = "8577330"   # Cove Point, MD
LAT, LON        = 38.356, -76.432
NWS_ZONE        = "MDZ018"
MARINE_ZONE     = "ANZ539"

# Additional sites
SITES = {
    "calvert": {
        "name": "Calvert Cliffs / Flag Pond",
        "lat": 38.356, "lon": -76.432,
        "noaa_station": "8577330",
        "station_label": "Cove Point, MD",
        "nws_zones": ["MDZ018", "ANZ539"],
        "use_marine": True,
        # wind sectors (deg)
        "clearing": (200, 315),
        "pushing": (22.5, 157.5),
    },
    "purse": {
        "name": "Purse State Park (Nanjemoy)",
        "lat": 38.323, "lon": -77.241,
        "noaa_station": "8635027",  # Dahlgren, VA
        "station_label": "Dahlgren, VA",
        "nws_zones": [],  # skip until we wire zone lookup
        "use_marine": False,  # marine API not reliable for river
        # For now, do not apply push/clear scoring beyond warnings; penalty handled generically below
        "clearing": None,
        "pushing": None,
    },
    "westmoreland": {
        "name": "Westmoreland State Park (Fossil Beach)",
        "lat": 38.169, "lon": -76.865,
        "noaa_station": "8635750",  # Colonial Beach, VA
        "station_label": "Colonial Beach, VA",
        "nws_zones": [],
        "use_marine": False,
        "clearing": None,
        "pushing": None,
    },
    "widewater": {
        "name": "Widewater State Park",
        "lat": 38.437, "lon": -77.315,
        "noaa_station": "8635027",  # Dahlgren, VA (approximation)
        "station_label": "Dahlgren, VA",
        "nws_zones": [],
        "use_marine": False,
        "clearing": None,
        "pushing": None,
    },
    "stratford": {
        "name": "Stratford Hall (Fossil Beach)",
        "lat": 38.142, "lon": -76.922,
        "noaa_station": "8635750",  # Colonial Beach, VA
        "station_label": "Colonial Beach, VA",
        "nws_zones": [],
        "use_marine": False,
        "clearing": None,
        "pushing": None,
    },
    "aquia": {
        "name": "Aquia Landing Park",
        "lat": 38.3698, "lon": -77.3457,
        "noaa_station": "8634858",  # Aquia Creek, VA
        "station_label": "Aquia Creek, VA",
        "nws_zones": [],
        "use_marine": False,
        "clearing": None,
        "pushing": None,
    },
}
SCORE_THRESHOLD = 4
LOOKBACK_HOURS  = 72
RAIN_LOOKAHEAD_HOURS = 24  # keep rain forecast window aligned with UI label
PORT             = int(os.environ.get('PORT', 5003))
HOST             = '0.0.0.0'
APPLICATION_ROOT = os.environ.get('APPLICATION_ROOT', '/')
CACHE_TTL       = 3600          # hourly background refresh
REFRESH_MIN_SECONDS = 60          # minimum gap between manual refresh requests
HEADERS         = {"User-Agent": "FossilHuntingDashboard/1.0 miku@openclaw"}

# Alerting (disabled unless webhook is provided)
ALERT_WEBHOOK_URL   = os.environ.get("FOSSIL_ALERT_WEBHOOK", "").strip()
ALERT_LOOKAHEAD_DAYS= int(os.environ.get("FOSSIL_ALERT_LOOKAHEAD_DAYS", "3"))
ALERT_MIN_SCORE     = int(os.environ.get("FOSSIL_ALERT_MIN_SCORE", str(SCORE_THRESHOLD)))
ALERT_QUIET_START   = int(os.environ.get("FOSSIL_ALERT_QUIET_START", "22"))  # 24h clock
ALERT_QUIET_END     = int(os.environ.get("FOSSIL_ALERT_QUIET_END", "7"))

WEEKEND_DAYS      = {4, 5, 6}   # Fri=4, Sat=5, Sun=6
_TSTORM_CODES     = {95, 96, 99}
_HEAVY_RAIN_CODES = {65, 66, 67, 81, 82}
_RAIN_CODES       = {51, 53, 55, 61, 63, 80}

_cache = {"data": None, "at": 0}
_last_refresh_req = 0.0
_state = {
    "weather":       {"data": None, "ok": False, "err": None, "at": 0.0, "last_ok": 0.0},
    "alerts":        {"data": None, "ok": False, "err": None, "at": 0.0, "last_ok": 0.0},
    "marine":        {"data": None, "ok": False, "err": None, "at": 0.0, "last_ok": 0.0},
    "tides_hourly":  {"data": None, "ok": False, "err": None, "at": 0.0, "last_ok": 0.0},
    "tides_hilo":    {"data": None, "ok": False, "err": None, "at": 0.0, "last_ok": 0.0},
}
_lock  = threading.Lock()
_alerts = {"sent": []}  # list of window ids already alerted
_datums_cache = {}    # station_id → {datum_name: float}; fetched once, never expires (astronomical constants)

# ── HTTP ──────────────────────────────────────────────────────────────────
def fetch(url, tries=3, backoff=0.75):
    """Fetch JSON with simple retries/backoff."""
    last_err = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last_err = e
            if i < tries - 1:
                time.sleep(backoff * (2 ** i))
            else:
                raise

def _rec(name, data=None, err=None):
    now = time.time()
    st = _state.get(name)
    if not st: return
    st["at"] = now
    if err is None:
        st["ok"] = True
        st["err"] = None
        st["data"] = data
        st["last_ok"] = now
    else:
        st["ok"] = False
        st["err"] = str(err)
        # keep last good data for fallback

# ── NOAA Tides ────────────────────────────────────────────────────────────
def get_tides_hourly(station=NOAA_STATION):
    """7-day hourly tide curve for the chart."""
    start = datetime.now().strftime("%Y%m%d")
    end   = (datetime.now() + timedelta(days=7)).strftime("%Y%m%d")
    url   = (f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
             f"?begin_date={start}&end_date={end}&station={station}"
             f"&product=predictions&datum=MLLW&time_zone=lst_ldt"
             f"&interval=h&units=english&application=fossil_dash&format=json")
    return fetch(url).get("predictions", [])

def get_tides_hilo(station=NOAA_STATION):
    """7-day high/low predictions for window finding."""
    start = datetime.now().strftime("%Y%m%d")
    end   = (datetime.now() + timedelta(days=7)).strftime("%Y%m%d")
    url   = (f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
             f"?begin_date={start}&end_date={end}&station={station}"
             f"&product=predictions&datum=MLLW&time_zone=lst_ldt"
             f"&interval=hilo&units=english&application=fossil_dash&format=json")
    return fetch(url).get("predictions", [])

# ── Open-Meteo weather ────────────────────────────────────────────────────
def get_weather(lat=LAT, lon=LON):
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={lat}&longitude={lon}"
           f"&hourly=precipitation,precipitation_probability,windspeed_10m,windgusts_10m,winddirection_10m,weathercode"
           f"&daily=sunrise,sunset"
           f"&past_days=3&forecast_days=8"
           f"&timezone=America%2FNew_York&precipitation_unit=inch")
    return fetch(url)

def get_marine_waves(lat=LAT, lon=LON):
    url = (f"https://marine-api.open-meteo.com/v1/marine"
           f"?latitude={lat}&longitude={lon}"
           f"&hourly=wave_height&past_days=3&forecast_days=7"
           f"&timezone=America%2FNew_York")
    try:
        return fetch(url)
    except Exception:
        return None

# ── NOAA Station Datums ──────────────────────────────────────────────────────────────
# Hardcoded fallbacks (ft above MLLW) used when the NOAA datums API call fails
_FALLBACK_DATUMS = {
    "8577330": {"MHW": 1.36, "MHHW": 1.56},   # Cove Point, MD
    "8635750": {"MHW": 1.18, "MHHW": 1.33},   # Colonial Beach, VA
    "8635027": {"MHW": 1.30, "MHHW": 1.48},   # Dahlgren, VA
    "8634858": {"MHW": 1.45, "MHHW": 1.62},   # Aquia Creek, VA
}

def get_station_datums(station):
    """Return NOAA tidal datums for a station, normalized to MLLW=0 reference.

    The NOAA datums API returns values relative to Station Datum (STND), but
    our tide predictions use MLLW datum. We subtract MLLW from all values so
    everything is in the same MLLW-relative reference (matching predictions).

    After normalization: MLLW=0.000, MHW=~1.33 ft, MHHW=~1.48 ft, etc.
    Results cached permanently — datums are 19-year tidal epoch averages.
    """
    sid = str(station)
    if sid in _datums_cache:
        return _datums_cache[sid]
    url = f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{sid}/datums.json"
    try:
        data = fetch(url)
        raw = {d["name"]: float(d["value"]) for d in data.get("datums", []) if "name" in d and "value" in d}
        if raw.get("MHW") and raw.get("MLLW") is not None:
            # Normalize STND-relative values to MLLW-relative (subtract MLLW offset)
            mllw_offset = raw["MLLW"]
            datums = {k: round(v - mllw_offset, 3) for k, v in raw.items()}
            # Sanity check: MHW should be positive and reasonable (0.5–3.0 ft)
            if 0.5 <= datums.get("MHW", 0) <= 5.0:
                _datums_cache[sid] = datums
                return datums
    except Exception:
        pass
    fallback = _FALLBACK_DATUMS.get(sid, {"MHW": 1.30, "MHHW": 1.50})
    _datums_cache[sid] = fallback
    return fallback

# ── NWS alerts ────────────────────────────────────────────────────────────
def get_alerts(zones=None):
    alerts = []
    if zones is None:
        zones = [NWS_ZONE, MARINE_ZONE]
    for zone in zones:
        try:
            d = fetch(f"https://api.weather.gov/alerts/active?zone={zone}&status=actual")
            alerts.extend(d.get("features", []))
        except Exception:
            pass
    return alerts

# ── Wind / math utils ─────────────────────────────────────────────────────
ADVISORY_KW = ["coastal flood", "storm surge", "special marine warning",
                "gale warning", "storm warning", "high surf"]
COMPASS = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
           "S","SSW","SW","WSW","W","WNW","NW","NNW"]

def circular_mean_deg(angles):
    """Correct circular mean — avoids the 350+10=180 bug."""
    if not angles: return 0.0
    rads = [math.radians(a) for a in angles]
    return math.degrees(math.atan2(
        sum(math.sin(r) for r in rads) / len(rads),
        sum(math.cos(r) for r in rads) / len(rads)
    )) % 360

def is_bay_clearing(d): return 200 <= (d or 0) <= 315
def is_bay_pushing(d):  return 22.5 <= (d or 0) <= 157.5

def in_sector(d, sector):
    if not sector or d is None: return False
    lo, hi = sector
    return lo <= d <= hi
def deg_label(d):
    # Bin using half-sector offset to avoid round() edge flips at boundaries
    return COMPASS[int(((d or 0) + 11.25) // 22.5) % 16]

# ── Scoring ───────────────────────────────────────────────────────────────
def _intensity_multiplier(iph):
    """Non-linear erosion weight per inch of rain based on hourly intensity (iph = inches/hour).
    Light drizzle barely moves sediment; heavy bursts are disproportionately effective at
    dislodging cliff material and exposing fossils."""
    if iph >= 0.50: return 2.5   # flash/heavy — maximum runoff, rapid cliff erosion
    if iph >= 0.25: return 1.8   # heavy — strong surface runoff
    if iph >= 0.10: return 1.0   # moderate — reference baseline
    if iph >= 0.04: return 0.4   # light — minimal erosion effect
    return 0.1                   # trace/drizzle — almost geologically irrelevant

def compute_score(weather, alerts, marine=None, clearing_sector=(200,315)):
    h      = weather["hourly"]
    now    = datetime.now()
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    day2_morning = (now + timedelta(days=2)).replace(hour=8, minute=0, second=0, microsecond=0)

    rain_past_raw = 0.0
    rain_past_eff = 0.0
    rain_peak_iph = 0.0   # single-hour peak intensity in the lookback window
    rain_inc = 0.0  # kept for display; no longer scored globally (per-window instead)
    clearing_units  = 0
    rain_chart = []

    for i, ts in enumerate(h["time"]):
        t = datetime.fromisoformat(ts)
        p = h["precipitation"][i] or 0
        s = h["windspeed_10m"][i] or 0
        d = h["winddirection_10m"][i] or 0
        # past rainfall (raw + intensity-weighted + time-decayed)
        if cutoff <= t <= now:
            rain_past_raw += p
            if p > rain_peak_iph:
                rain_peak_iph = p
            age_h = max(0.0, (now - t).total_seconds()/3600.0)
            w = 1.0 if age_h <= 12 else (0.6 if age_h <= 36 else 0.3)
            # Intensity multiplier: heavy bursts erode far more than the same volume as drizzle
            rain_past_eff += p * w * _intensity_multiplier(p)
            # clearing: weight strong winds double
            if s >= 10 and in_sector(d, clearing_sector):
                clearing_units += 2 if s >= 20 else 1
        elif now < t <= day2_morning:
            rain_inc += p
        if cutoff <= t <= now + timedelta(hours=RAIN_LOOKAHEAD_HOURS):
            rain_chart.append({"x": ts if "T" in ts else ts.replace(" ", "T"),
                                "y": round(p, 3), "past": t <= now})

    pts, signals = 0, []

    # Rainfall score — intensity-weighted + time-decayed accumulation
    # Thresholds are higher than raw inches because intense bursts inflate the effective total
    rain_label = f"Intensity-weighted rain past {LOOKBACK_HOURS}h: {rain_past_eff:.2f} eff (raw {rain_past_raw:.2f}\", peak {rain_peak_iph:.2f}\"/hr)"
    if rain_past_eff >= 2.5:
        pts += 3; signals.append({"text": rain_label, "pts": 3})
    elif rain_past_eff >= 1.0:
        pts += 2; signals.append({"text": rain_label, "pts": 2})
    elif rain_past_eff >= 0.4:
        pts += 1; signals.append({"text": rain_label, "pts": 1})

    # Peak intensity bonus: a single heavy burst earns +1 even if total accumulation was low
    if rain_peak_iph >= 0.50 and pts < 3:  # only add if not already at rain cap
        pts += 1
        signals.append({"text": f"Peak intensity burst: {rain_peak_iph:.2f}\"/hr (high cliff erosion event)", "pts": 1})

    # Incoming rain: now scored per-window, not globally

    for a in alerts:
        ev = a.get("properties", {}).get("event", "").lower()
        if any(k in ev for k in ADVISORY_KW):
            pts += 2; signals.append({"text": f"NWS: {a['properties'].get('event')}", "pts": 2}); break

    if clearing_units >= 3:
        pts += 2; signals.append({"text": f"W/SW bay-clearing winds (weighted {clearing_units})", "pts": 2})

    # Marine wave heights (structured API > NWS text scraping)
    if marine:
        try:
            mh = marine["hourly"]
            past_waves = [mh["wave_height"][i] for i, ts in enumerate(mh["time"])
                          if mh["wave_height"][i] is not None
                          and cutoff <= datetime.fromisoformat(ts) <= now]
            if past_waves:
                mw = max(past_waves)
                mw_ft = mw * 3.281
                if mw >= 0.9:    pts += 2; signals.append({"text": f"Bay waves ~{mw_ft:.1f}ft past 72h", "pts": 2})
                elif mw >= 0.45: pts += 1; signals.append({"text": f"Bay waves ~{mw_ft:.1f}ft past 72h", "pts": 1})
        except Exception:
            pass
    else:
        for a in alerts:
            desc = a.get("properties", {}).get("description", "").lower()
            if "wave" in desc:
                for ft in range(4, 15):
                    if f"{ft} ft" in desc or f"{ft}ft" in desc:
                        pts += 1; signals.append({"text": f"Bay waves ~{ft}ft (NWS)", "pts": 1}); break
                break

    # Current wind: choose nearest forecast hour
    cur_dir = cur_spd = None
    if h.get("time"):
        deltas = [abs((datetime.fromisoformat(ts) - now).total_seconds()) for ts in h["time"]]
        idx = deltas.index(min(deltas))
        cur_dir = h.get("winddirection_10m", [None])[idx]
        cur_spd = h.get("windspeed_10m", [None])[idx]

    return {
        "score":      min(pts, 10),
        "signals":    signals,
        "rain_past":  round(rain_past_raw, 3),
        "rain_inc":   round(rain_inc, 3),
        "clearing":   clearing_units,
        "rain_chart": rain_chart,
        "cur_dir":    cur_dir,
        "cur_spd":    cur_spd,
    }

def _visit_penalty(tide_ts, weather):
    """Penalty for hazardous weather during the ±2h window around the actual low tide time.
    This is more accurate than a whole-day check: a 7 AM low tide is fine even if
    afternoon thunderstorms are forecast."""
    h = weather["hourly"]
    codes = h.get("weathercode", [])
    worst = 0
    window_start = tide_ts - timedelta(hours=2)
    window_end   = tide_ts + timedelta(hours=2)
    for i, ts in enumerate(h["time"]):
        t = datetime.fromisoformat(ts)
        if window_start <= t <= window_end:
            c = int(codes[i] or 0)
            if c in _TSTORM_CODES:       worst = max(worst, 3)
            elif c in _HEAVY_RAIN_CODES: worst = max(worst, 2)
            elif c in _RAIN_CODES:       worst = max(worst, 1)
    if worst >= 3: return 2, "Thunderstorms during window"
    if worst >= 2: return 1, "Heavy rain during window"
    return 0, ""

def _tide_quality_score(ht, datums, hilo_preds=None, tide_ts=None):
    """Score low tide quality relative to NOAA station datums.

    Key insight: what matters for fossil hunting is how much intertidal beach is
    exposed, not the absolute tide height. We compute:

        exposure_fraction = (MHW - low_ht) / MHW

    where MHW is the station's Mean High Water (ft above MLLW=0 datum).
    A tide of 0.0 ft at a station with MHW=1.36 ft exposes 100% of the tidal range;
    a tide of 0.5 ft exposes only 63%. The same 0.0 ft tide at Colonial Beach
    (MHW=1.18 ft) still exposes 100%, but represents a smaller absolute area.

    Spring tide bonus: if the preceding high exceeded MHW by >10% (spring tide
    territory), the enhanced tidal churn earns an extra point.

    Returns (pts: 0-3, label: str, info: dict).
    """
    mhw = float(datums.get("MHW") or 1.30)
    if mhw <= 0:
        mhw = 1.30

    # Exposure fraction: how much of the MLLW→MHW tidal range is currently exposed?
    exposure = max(0.0, min(1.0, (mhw - ht) / mhw))
    exposure_pct = round(exposure * 100, 1)
    info = {"exposure_pct": exposure_pct, "drop_ft": None, "mhw": round(mhw, 2)}

    # Score based on what fraction of the tidal range is exposed
    if exposure >= 0.90:
        pts = 3; label = f"Exceptional low \u2014 {exposure_pct:.0f}% of tidal range exposed"
    elif exposure >= 0.75:
        pts = 2; label = f"Very low tide \u2014 {exposure_pct:.0f}% exposed"
    elif exposure >= 0.58:
        pts = 1; label = f"Good low tide \u2014 {exposure_pct:.0f}% exposed"
    else:
        pts = 0; label = f"Moderate low \u2014 {exposure_pct:.0f}% exposed"

    # Spring/neap tide indicator: find the most recent preceding high tide
    if hilo_preds and tide_ts:
        preceding_highs = []
        for p in hilo_preds:
            if p.get("type") != "H":
                continue
            try:
                pt = datetime.strptime(p["t"], "%Y-%m-%d %H:%M")
                if pt < tide_ts:
                    preceding_highs.append((pt, float(p["v"])))
            except Exception:
                continue
        if preceding_highs:
            _, high_ht = max(preceding_highs, key=lambda x: x[0])
            drop = high_ht - ht
            info["drop_ft"] = round(drop, 2)
            # Spring tide: preceding high exceeded MHW by >10% (enhanced churn/exposure)
            if drop > mhw * 1.10 and pts < 3:
                pts += 1
                label += f" + spring tide ({drop:.2f} ft drop)"
            else:
                label += f" ({drop:.2f} ft drop)"

    return pts, label, info

def _site_recommendation(site_key, ht, mhw=1.30):
    """Return site-specific access guidance based on tidal exposure at this station."""
    exposure = max(0.0, min(1.0, (mhw - ht) / mhw)) if mhw > 0 else 0.5
    if site_key == "calvert":
        if exposure >= 0.85:
            return "Calvert north beach \u2014 max cliff-base exposure, best fossil access"
        elif exposure >= 0.65:
            return "Both Calvert Cliffs & Flag Pond accessible"
        else:
            return "Flag Pond preferred (shorter beach hike at this tide height)"
    elif site_key == "purse":
        if exposure >= 0.80:
            return "Purse SP \u2014 full eroded bank & lower beach access"
        return "Purse SP \u2014 main beach (wading shoes helpful)"
    elif site_key == "westmoreland":
        if exposure >= 0.80:
            return "Westmoreland \u2014 Fossil Beach fully exposed"
        elif exposure >= 0.60:
            return "Westmoreland \u2014 good cliff-base access"
        return "Westmoreland \u2014 partial beach (wading shoes helpful)"
    elif site_key == "widewater":
        return "Widewater SP \u2014 shoreline & gravel bars"
    elif site_key == "stratford":
        if exposure >= 0.80:
            return "Stratford Hall \u2014 full fossil beach, check both ends"
        return "Stratford Hall \u2014 central beach access"
    elif site_key == "aquia":
        if exposure >= 0.80:
            return "Aquia Landing \u2014 lower gravel beds & riverbank exposed"
        return "Aquia Landing \u2014 upper gravel bars"
    return f"{site_key.replace('_', ' ').title()} \u2014 beach access"

def find_windows(hilo, weather, base_score, marine=None, pushing_sector=(22.5, 157.5),
                 site_name="", station_label="", datums=None, site_key="", station_id=""):
    """Any day 1-7 ahead, low tides during daylight. Per-window score:
      base_score (conditions) + tide_quality - weather_penalty - push_penalty + rain_bonus + wave_bonus
    Tide quality is now station-relative (NOAA datum MHW comparison) rather than absolute ft."""
    h   = weather["hourly"]
    now = datetime.now()
    _datums = datums or {}
    mhw = float(_datums.get("MHW") or 1.30)

    # Build sunrise/sunset map per date (fallback 7-17 if API missing)
    sun = {}
    daily = weather.get("daily", {})
    for i, ds in enumerate(daily.get("time", [])):
        try:
            d = datetime.fromisoformat(ds).date()
        except Exception:
            continue
        try:
            sr = datetime.fromisoformat(daily.get("sunrise", [""])[i])
            ss = datetime.fromisoformat(daily.get("sunset",  [""])[i])
        except Exception:
            sr = datetime.combine(datetime.fromisoformat(ds).date(), datetime.min.time()).replace(hour=7)
            ss = datetime.combine(datetime.fromisoformat(ds).date(), datetime.min.time()).replace(hour=17)
        sun[d] = (sr, ss)

    out = []
    for entry in hilo:
        if entry.get("type") != "L": continue
        t    = datetime.strptime(entry["t"], "%Y-%m-%d %H:%M")
        days = (t.date() - now.date()).days
        if days < 1 or days > 7: continue
        # Daylight filter
        sr, ss = sun.get(t.date(), (t.replace(hour=7, minute=0), t.replace(hour=17, minute=0)))
        if not (sr <= t <= ss):
            continue
        ht = float(entry["v"])

        penalty, p_reason           = _visit_penalty(t, weather)
        t_bonus, t_label, tide_info = _tide_quality_score(ht, _datums, hilo, t)

        inc_bonus = 0
        inc_amt   = 0.0
        # Per-window incoming rain in RAIN_LOOKAHEAD_HOURS prior to tide (future hours only)
        try:
            start_w = max(now, t - timedelta(hours=RAIN_LOOKAHEAD_HOURS))
            end_w   = t
            inc = 0.0
            for i, ts in enumerate(h["time"]):
                th = datetime.fromisoformat(ts)
                if start_w <= th <= end_w:
                    precip = h.get("precipitation", [0])[i] or 0
                    prob   = h.get("precipitation_probability", [None])[i]
                    if prob is None:
                        p_eff = precip
                    elif prob < 30:
                        p_eff = 0.0
                    elif prob < 70:
                        p_eff = precip * 0.5
                    else:
                        p_eff = precip
                    inc += p_eff
            inc_amt = round(inc, 3)
            if inc >= 0.75:   inc_bonus = 2
            elif inc >= 0.25: inc_bonus = 1
        except Exception:
            pass

        wave_bonus = 0
        try:
            if marine and marine.get("hourly"):
                mh = marine["hourly"]
                prior = []
                nearest = None
                mindt = 10**9
                for i, ts in enumerate(mh["time"]):
                    tm = datetime.fromisoformat(ts)
                    wh = mh.get("wave_height", [None])[i]
                    if wh is None: continue
                    if t - timedelta(hours=24) <= tm <= t:
                        prior.append(wh)
                    dt = abs((tm - t).total_seconds())
                    if dt < mindt:
                        mindt = dt; nearest = wh
                if prior:
                    mw = max(prior)
                    if mw >= 0.9:    wave_bonus += 2
                    elif mw >= 0.45: wave_bonus += 1
                if nearest is not None and nearest >= 0.9:
                    wave_bonus += 1
        except Exception:
            pass

        is_weekend = t.weekday() in WEEKEND_DAYS

        dirs, speeds = [], []
        for i, ts in enumerate(h["time"]):
            lt = datetime.fromisoformat(ts)
            if lt.date() == t.date() and 6 <= lt.hour < 12:
                dirs.append(h["winddirection_10m"][i] or 0)
                speeds.append(h["windspeed_10m"][i] or 0)

        wind_warn = None
        push_penalty = 0
        push_penalty_why = None
        if dirs:
            avg_dir = circular_mean_deg(dirs)
            avg_spd = sum(speeds) / len(speeds)
            if avg_spd >= 10 and in_sector(avg_dir, pushing_sector):
                wind_warn = f"{deg_label(avg_dir)} winds ~{avg_spd:.0f} mph \u2014 may reduce low tide effect"
                if avg_spd >= 20:
                    push_penalty = 2; push_penalty_why = "Strong bay-pushing winds"
                else:
                    push_penalty = 1; push_penalty_why = "Bay-pushing winds"

        window_score = max(0, min(base_score + t_bonus - penalty - push_penalty + inc_bonus + wave_bonus, 10))
        site = _site_recommendation(site_key, ht, mhw)

        out.append({
            "date":          t.strftime("%A, %B %-d"),
            "time":          t.strftime("%-I:%M %p"),
            "ts":            t.isoformat(),
            "height":        ht,
            "location":      site_name or "",
            "station_label": station_label or "",
            "base_score":    base_score,
            "tide_bonus":    t_bonus,
            "penalty":       penalty,
            "penalty_why":   p_reason,
            "push_penalty":  push_penalty,
            "push_penalty_why": push_penalty_why,
            "window_score":  window_score,
            "qualifies":     window_score >= SCORE_THRESHOLD,
            "is_weekend":    is_weekend,
            "wind_warn":     wind_warn,
            "site":          site,
            "tide_label":    t_label,
            "exposure_pct":  tide_info.get("exposure_pct"),
            "drop_ft":       tide_info.get("drop_ft"),
            "mhw":           tide_info.get("mhw"),
            "inc_rain":      inc_amt,
            "inc_bonus":     inc_bonus,
            "wave_bonus":    wave_bonus,
        })

    # Return all low tides — multiple per day expected on semidiurnal stations.
    # Each low tide gets its own card in the UI.
    return out


def build_data():
    # Per-source fetch with fallback and health recording
    try:
        weather = get_weather(); _rec("weather", weather)
    except Exception as e:
        _rec("weather", err=e)
        weather = (_state["weather"]["data"] or {})
    try:
        alerts = get_alerts(); _rec("alerts", alerts)
    except Exception as e:
        _rec("alerts", err=e)
        alerts = _state["alerts"]["data"] or []
    try:
        marine = get_marine_waves(); _rec("marine", marine)
    except Exception as e:
        _rec("marine", err=e)
        marine = _state["marine"]["data"] or None
    try:
        hourly = get_tides_hourly(); _rec("tides_hourly", hourly)
    except Exception as e:
        _rec("tides_hourly", err=e)
        hourly = _state["tides_hourly"]["data"] or []
    try:
        hilo = get_tides_hilo(); _rec("tides_hilo", hilo)
    except Exception as e:
        _rec("tides_hilo", err=e)
        hilo = _state["tides_hilo"]["data"] or []
    # Hard requirement: weather to compute score
    if not weather or not isinstance(weather, dict) or not weather.get("hourly"):
        raise RuntimeError("weather unavailable and no cached fallback")
    datums_calvert = get_station_datums(NOAA_STATION)
    scoring = compute_score(weather, alerts, marine, clearing_sector=SITES["calvert"]["clearing"])
    calvert_windows = find_windows(hilo, weather, scoring["score"], marine,
                           pushing_sector=SITES["calvert"]["pushing"],
                           site_name=SITES["calvert"]["name"],
                           station_label=SITES["calvert"]["station_label"],
                           datums=datums_calvert, site_key="calvert", station_id=NOAA_STATION)
    windows = list(calvert_windows)

    # Purse site (Nanjemoy) — compute independently and merge windows
    try:
        w2 = get_weather(SITES["purse"]["lat"], SITES["purse"]["lon"])
        mh2 = None  # no marine
        h2h = get_tides_hilo(SITES["purse"]["noaa_station"])
        hh2 = get_tides_hourly(SITES["purse"]["noaa_station"])  # for site-specific tide chart
        datums_purse = get_station_datums(SITES["purse"]["noaa_station"])
        score2 = compute_score(w2, [], None, clearing_sector=SITES["purse"]["clearing"])  # alerts omitted for now
        wins2  = find_windows(h2h, w2, score2["score"], None,
                              pushing_sector=SITES["purse"]["pushing"],
                              site_name=SITES["purse"]["name"],
                              station_label=SITES["purse"]["station_label"],
                              datums=datums_purse, site_key="purse", station_id=SITES["purse"]["noaa_station"])
        windows.extend(wins2)
        purse_site = {
            **{k: score2[k] for k in ["score","signals","rain_past","rain_inc","clearing","rain_chart","cur_dir","cur_spd"]},
            "threshold": SCORE_THRESHOLD,
            "would_alert": any(w["qualifies"] for w in wins2),
            "windows": wins2,
            "tide_chart": [{"x": p["t"].replace(" ", "T") + ":00", "y": float(p["v"])} for p in hh2],
            "noon_lines": [(datetime.now() + timedelta(days=d)).replace(hour=12, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT12:00:00") for d in range(8)],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "station": f"NOAA {SITES['purse']['noaa_station']} — {SITES['purse']['station_label']}",
            "name": SITES["purse"]["name"],
            "station_label": SITES["purse"]["station_label"],
        }
    except Exception as e:
        _state.setdefault("purse", {})["err"] = str(e)
        purse_site = None

    # Stratford Hall (Fossil Beach)
    stratford_site = None
    try:
        ws = get_weather(SITES["stratford"]["lat"], SITES["stratford"]["lon"]) ; _rec("weather:stratford", ws)
        hs = get_tides_hilo(SITES["stratford"]["noaa_station"]) ; _rec("tides_hilo:stratford", hs)
        hhs= get_tides_hourly(SITES["stratford"]["noaa_station"]) ; _rec("tides_hourly:stratford", hhs)
        datums_stratford = get_station_datums(SITES["stratford"]["noaa_station"])
        scs = compute_score(ws, [], None, clearing_sector=SITES["stratford"]["clearing"])  # river
        winss  = find_windows(hs, ws, scs["score"], None,
                              pushing_sector=SITES["stratford"]["pushing"],
                              site_name=SITES["stratford"]["name"],
                              station_label=SITES["stratford"]["station_label"],
                              datums=datums_stratford, site_key="stratford", station_id=SITES["stratford"]["noaa_station"])
        windows.extend(winss)
        stratford_site = {
            **{k: scs[k] for k in ["score","signals","rain_past","rain_inc","clearing","rain_chart","cur_dir","cur_spd"]},
            "threshold": SCORE_THRESHOLD,
            "would_alert": any(w["qualifies"] for w in winss),
            "windows": winss,
            "tide_chart": [{"x": p["t"].replace(" ", "T") + ":00", "y": float(p["v"])} for p in hhs],
            "noon_lines": [(datetime.now() + timedelta(days=d)).replace(hour=12, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT12:00:00") for d in range(8)],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "station": f"NOAA {SITES['stratford']['noaa_station']} — {SITES['stratford']['station_label']}",
            "name": SITES["stratford"]["name"],
            "station_label": SITES["stratford"]["station_label"],
        }
    except Exception as e:
        _state.setdefault("stratford", {})["err"] = str(e)

    # Westmoreland State Park
    westmoreland_site = None
    try:
        ww = get_weather(SITES["westmoreland"]["lat"], SITES["westmoreland"]["lon"]) ; _rec("weather:westmoreland", ww)
        hw = get_tides_hilo(SITES["westmoreland"]["noaa_station"]) ; _rec("tides_hilo:westmoreland", hw)
        hhw= get_tides_hourly(SITES["westmoreland"]["noaa_station"]) ; _rec("tides_hourly:westmoreland", hhw)
        datums_westmoreland = get_station_datums(SITES["westmoreland"]["noaa_station"])
        scw = compute_score(ww, [], None, clearing_sector=SITES["westmoreland"]["clearing"])  # river
        winsw  = find_windows(hw, ww, scw["score"], None,
                              pushing_sector=SITES["westmoreland"]["pushing"],
                              site_name=SITES["westmoreland"]["name"],
                              station_label=SITES["westmoreland"]["station_label"],
                              datums=datums_westmoreland, site_key="westmoreland", station_id=SITES["westmoreland"]["noaa_station"])
        windows.extend(winsw)
        westmoreland_site = {
            **{k: scw[k] for k in ["score","signals","rain_past","rain_inc","clearing","rain_chart","cur_dir","cur_spd"]},
            "threshold": SCORE_THRESHOLD,
            "would_alert": any(w["qualifies"] for w in winsw),
            "windows": winsw,
            "tide_chart": [{"x": p["t"].replace(" ", "T") + ":00", "y": float(p["v"])} for p in hhw],
            "noon_lines": [(datetime.now() + timedelta(days=d)).replace(hour=12, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT12:00:00") for d in range(8)],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "station": f"NOAA {SITES['westmoreland']['noaa_station']} — {SITES['westmoreland']['station_label']}",
            "name": SITES["westmoreland"]["name"],
            "station_label": SITES["westmoreland"]["station_label"],
        }
    except Exception as e:
        _state.setdefault("westmoreland", {})["err"] = str(e)

    # Widewater State Park
    widewater_site = None
    try:
        wwd = get_weather(SITES["widewater"]["lat"], SITES["widewater"]["lon"]) ; _rec("weather:widewater", wwd)
        hwd = get_tides_hilo(SITES["widewater"]["noaa_station"]) ; _rec("tides_hilo:widewater", hwd)
        hhw2= get_tides_hourly(SITES["widewater"]["noaa_station"]) ; _rec("tides_hourly:widewater", hhw2)
        datums_widewater = get_station_datums(SITES["widewater"]["noaa_station"])
        scwd = compute_score(wwd, [], None, clearing_sector=SITES["widewater"]["clearing"])  # river
        winswd  = find_windows(hwd, wwd, scwd["score"], None,
                               pushing_sector=SITES["widewater"]["pushing"],
                               site_name=SITES["widewater"]["name"],
                               station_label=SITES["widewater"]["station_label"],
                               datums=datums_widewater, site_key="widewater", station_id=SITES["widewater"]["noaa_station"])
        windows.extend(winswd)
        widewater_site = {
            **{k: scwd[k] for k in ["score","signals","rain_past","rain_inc","clearing","rain_chart","cur_dir","cur_spd"]},
            "threshold": SCORE_THRESHOLD,
            "would_alert": any(w["qualifies"] for w in winswd),
            "windows": winswd,
            "tide_chart": [{"x": p["t"].replace(" ", "T") + ":00", "y": float(p["v"])} for p in hhw2],
            "noon_lines": [(datetime.now() + timedelta(days=d)).replace(hour=12, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT12:00:00") for d in range(8)],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "station": f"NOAA {SITES['widewater']['noaa_station']} — {SITES['widewater']['station_label']}",
            "name": SITES["widewater"]["name"],
            "station_label": SITES["widewater"]["station_label"],
        }
    except Exception as e:
        _state.setdefault("widewater", {})["err"] = str(e)

    # Aquia Landing Park
    aquia_site = None
    try:
        waq = get_weather(SITES["aquia"]["lat"], SITES["aquia"]["lon"]) ; _rec("weather:aquia", waq)
        haq = get_tides_hilo(SITES["aquia"]["noaa_station"]) ; _rec("tides_hilo:aquia", haq)
        hhaq = get_tides_hourly(SITES["aquia"]["noaa_station"]) ; _rec("tides_hourly:aquia", hhaq)
        datums_aquia = get_station_datums(SITES["aquia"]["noaa_station"])
        scaq = compute_score(waq, [], None, clearing_sector=SITES["aquia"]["clearing"])
        winsaq = find_windows(haq, waq, scaq["score"], None,
                              pushing_sector=SITES["aquia"]["pushing"],
                              site_name=SITES["aquia"]["name"],
                              station_label=SITES["aquia"]["station_label"],
                              datums=datums_aquia, site_key="aquia", station_id=SITES["aquia"]["noaa_station"])
        windows.extend(winsaq)
        aquia_site = {
            **{k: scaq[k] for k in ["score","signals","rain_past","rain_inc","clearing","rain_chart","cur_dir","cur_spd"]},
            "threshold": SCORE_THRESHOLD,
            "would_alert": any(w["qualifies"] for w in winsaq),
            "windows": winsaq,
            "tide_chart": [{"x": p["t"].replace(" ", "T") + ":00", "y": float(p["v"])} for p in hhaq],
            "noon_lines": [(datetime.now() + timedelta(days=d)).replace(hour=12, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT12:00:00") for d in range(8)],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "station": f"NOAA {SITES['aquia']['noaa_station']} \u2014 {SITES['aquia']['station_label']}",
            "name": SITES["aquia"]["name"],
            "station_label": SITES["aquia"]["station_label"],
        }
    except Exception as e:
        _state.setdefault("aquia", {})["err"] = str(e)

    # sort windows chronologically
    try:
        windows.sort(key=lambda w: w.get("ts", ""))
    except Exception:
        pass

    nws_names = list({a["properties"].get("event")
                      for a in alerts if a.get("properties", {}).get("event")})

    noon_lines = []
    for d in range(8):
        dt = (datetime.now() + timedelta(days=d)).replace(hour=12, minute=0, second=0, microsecond=0)
        noon_lines.append(dt.strftime("%Y-%m-%dT12:00:00"))

    tide_chart = [{"x": p["t"].replace(" ", "T") + ":00", "y": float(p["v"])}
                  for p in hourly]

    sites = {
        "calvert": {
            **{k: scoring[k] for k in ["score","signals","rain_past","rain_inc","clearing","rain_chart","cur_dir","cur_spd"]},
            "threshold": SCORE_THRESHOLD,
            "would_alert": any(w["qualifies"] for w in calvert_windows),
            "windows": calvert_windows,
            "tide_chart": tide_chart,
            "noon_lines": noon_lines,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "station":      f"NOAA {NOAA_STATION} — {SITES['calvert']['station_label']}",
            "name": SITES["calvert"]["name"],
            "station_label": SITES["calvert"]["station_label"],
        }
    }
    if purse_site:
        sites["purse"] = purse_site
    if 'stratford_site' in locals() and stratford_site:
        sites["stratford"] = stratford_site
    if 'westmoreland_site' in locals() and westmoreland_site:
        sites["westmoreland"] = westmoreland_site
    if 'widewater_site' in locals() and widewater_site:
        sites["widewater"] = widewater_site
    if 'aquia_site' in locals() and aquia_site:
        sites["aquia"] = aquia_site

    return {
        **scoring,
        "threshold":    SCORE_THRESHOLD,
        "would_alert":  any(w["qualifies"] for w in windows),
        "windows":      windows,
        "nws_alerts":   nws_names,
        "tide_chart":   tide_chart,
        "noon_lines":   noon_lines,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "station":      f"NOAA {NOAA_STATION} — {SITES['calvert']['station_label']}",
        "sites":        sites,
        "health": {
            k: {
                "ok": v["ok"],
                "age_sec": round(time.time() - v["at"], 1) if v["at"] else None,
                "since_ok_sec": round(time.time() - v["last_ok"], 1) if v["last_ok"] else None,
                "err": v["err"],
            } for k, v in _state.items()
        }
    }

# ── Routes ────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "r", encoding="utf-8") as fh:
        return Response(fh.read(), mimetype="text/html")

@app.route("/api/data")
def api_data():
    with _lock:
        cached = _cache["data"]
    if cached is None:
        # Cold-start: background thread hasn't populated cache yet — build once synchronously
        try:
            data = build_data()
            with _lock:
                _cache["data"] = data
                _cache["at"]   = time.time()
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify(cached)

@app.route("/api/refresh")
def api_refresh():
    global _last_refresh_req
    nowt = time.time()
    with _lock:
        if nowt - _last_refresh_req < REFRESH_MIN_SECONDS:
            return jsonify({"ok": False, "reason": f"rate_limited: wait {int(REFRESH_MIN_SECONDS - (nowt - _last_refresh_req))}s"}), 429
        _last_refresh_req = nowt
    # Trigger async rebuild without blocking the HTTP response
    def _do_refresh():
        try:
            data = build_data()
            with _lock:
                _cache["data"] = data
                _cache["at"]   = time.time()
        except Exception as e:
            with _lock:
                if _cache["data"]:
                    _cache["data"]["error"] = str(e)
    threading.Thread(target=_do_refresh, daemon=True).start()
    return jsonify({"ok": True, "next_ok_in": REFRESH_MIN_SECONDS, "status": "refresh_queued"})

@app.route("/healthz")
def healthz():
    nowt = time.time()
    data_age = (nowt - _cache["at"]) if _cache["at"] else None
    hs = {
        k: {
            "ok": v["ok"],
            "age_sec": round(nowt - v["at"], 1) if v["at"] else None,
            "since_ok_sec": round(nowt - v["last_ok"], 1) if v["last_ok"] else None,
            "err": v["err"],
        } for k, v in _state.items()
    }
    overall_ok = all((v["ok"] or v["data"]) for v in _state.values())
    return jsonify({
        "ok": overall_ok,
        "cache_age_sec": round(data_age, 1) if data_age is not None else None,
        "last_refresh_req": _last_refresh_req,
        "sources": hs,
    })

def _in_quiet_hours(now_local: datetime) -> bool:
    hs, he = ALERT_QUIET_START, ALERT_QUIET_END
    if hs == he:
        return False
    if hs < he:
        return hs <= now_local.hour < he
    # overnight wrap
    return now_local.hour >= hs or now_local.hour < he

def _window_id(w):
    if w.get("ts"):
        return f"{w['ts']}|{w.get('height',''):.2f}"
    return f"{w.get('date','')} {w.get('time','')} {w.get('height',''):.2f}"

def _post_webhook(payload: dict):
    if not ALERT_WEBHOOK_URL:
        return False, "no_webhook"
    try:
        req = urllib.request.Request(ALERT_WEBHOOK_URL,
                                     data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json", **HEADERS},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
        return True, None
    except Exception as e:
        return False, str(e)

def _refresh_loop():
    """Background worker: rebuild API cache once per hour.
    Runs immediately on launch so the first page load is instant."""
    while True:
        try:
            data = build_data()
            with _lock:
                _cache["data"] = data
                _cache["at"]   = time.time()
        except Exception as e:
            with _lock:
                if _cache["data"]:
                    _cache["data"]["error"] = str(e)
            _state.setdefault("cache_refresh", {})["err"] = str(e)
        time.sleep(CACHE_TTL)

def _alert_loop():
    if not ALERT_WEBHOOK_URL:
        return  # alerting disabled
    while True:
        try:
            d = build_data()
            now_local = datetime.now()
            if not _in_quiet_hours(now_local):
                for w in d.get("windows", []):
                    # within lookahead, meets score, qualifies
                    # Parse day difference from human date string by matching tide time in hilo is tricky;
                    # use datetime reconstruction from score data not stored; fallback: skip if missing
                    # Here, rely on displayed date/time being within next 7 days by construction.
                    if not w.get("qualifies"): continue
                    if w.get("window_score", 0) < ALERT_MIN_SCORE: continue
                    wid = _window_id(w)
                    if wid in _alerts["sent"]: continue
                    # Lookahead gate using exact timestamp when available
                    win_ts = None
                    try:
                        if w.get("ts"):
                            win_ts = datetime.fromisoformat(w["ts"])
                    except Exception:
                        win_ts = None
                    if win_ts is None:
                        # Fallback to title check
                        try:
                            ok_dates = set((now_local + timedelta(days=i)).strftime("%A, %B %-d") for i in range(1, ALERT_LOOKAHEAD_DAYS+1))
                        except Exception:
                            ok_dates = set()
                        if ok_dates and w.get("date") not in ok_dates:
                            continue
                    else:
                        days_ahead = (win_ts.date() - now_local.date()).days
                        if not (1 <= days_ahead <= ALERT_LOOKAHEAD_DAYS):
                            continue
                    loc = w.get('location') or 'Calvert/Flag Pond'
                    payload = {
                        "username": "Fossil Alerts",
                        "content": f"🦈 {loc}: {w['date']} at {w['time']} — {w['window_score']}/10 (tide {w['height']:.2f} ft @ {w.get('station_label','')}). {w.get('tide_label','')} {('· ' + w['wind_warn']) if w.get('wind_warn') else ''}\nSite hint: {w['site']}"
                    }
                    ok, err = _post_webhook(payload)
                    if ok:
                        _alerts["sent"].append(wid)
                    else:
                        _state.setdefault("alerts_sink", {})["err"] = err
            # prune sent memory
            if len(_alerts["sent"]) > 200:
                _alerts["sent"] = _alerts["sent"][-200:]
        except Exception as e:
            _state.setdefault("alerts_sink", {})["err"] = str(e)
        time.sleep(15*60)

@app.route("/api/alerts")
def api_alerts():
    return jsonify({
        "enabled": bool(ALERT_WEBHOOK_URL),
        "min_score": ALERT_MIN_SCORE,
        "lookahead_days": ALERT_LOOKAHEAD_DAYS,
        "quiet_hours": [ALERT_QUIET_START, ALERT_QUIET_END],
        "sent_count": len(_alerts["sent"]),
    })

if __name__ == "__main__":
    # Start hourly background cache refresh (runs immediately, then every CACHE_TTL seconds)
    threading.Thread(target=_refresh_loop, name="refresh-loop", daemon=True).start()
    # Start alert worker if webhook configured
    if ALERT_WEBHOOK_URL:
        th = threading.Thread(target=_alert_loop, name="alert-loop", daemon=True)
        th.start()
    if APPLICATION_ROOT and APPLICATION_ROOT != '/':
        from flask import Flask as _Flask
        _root = _Flask('root')
        mounted = DispatcherMiddleware(_root, {APPLICATION_ROOT: app})
        run_simple(HOST, PORT, mounted, threaded=True)
    else:
        app.run(host=HOST, port=PORT, debug=False, threaded=True)
