# 🦈 Fossil Hunting Dashboard

A live-scoring dashboard for fossil hunting conditions along the Chesapeake Bay and Potomac River cliffs — specifically tuned for **Calvert Cliffs**, **Flag Pond**, **Purse State Park**, **Westmoreland State Park**, **Stratford Hall**, **Widewater SP**, and **Aquia Landing**.

## Features

- **Tidal exposure scoring** — Station-relative scoring using NOAA datum MHW. Rates how much of the intertidal beach is exposed (not just absolute height).
- **Spring tide detection** — Bonus for preceding high tides that exceeded MHW, indicating enhanced tidal churn.
- **Rainfall erosion scoring** — Decay-weighted 72h lookback; recent storms score higher.
- **NWS advisory monitoring** — Flags active coastal/marine advisories.
- **Bay-clearing wind detection** — W/SW winds push water off the Bay shoreline; opposing winds penalize.
- **Marine wave height** — Wave churn from prior 24h and forecast at tide time.
- **Window-aware weather penalty** — Checks conditions ±2h around the actual low tide, not the whole day.
- **Per-site recommendations** — Context-aware access guidance for each of the 6 sites.
- **Hourly background refresh** — All API calls are pre-fetched; page loads are instant from cache.

## Sites Covered

| Site | Station | River/Bay |
|------|---------|-----------|
| Calvert Cliffs / Flag Pond | Cove Point MD (8577330) | Chesapeake Bay |
| Purse State Park | Dahlgren VA (8635027) | Potomac River |
| Westmoreland State Park | Colonial Beach VA (8635750) | Potomac River |
| Stratford Hall | Colonial Beach VA (8635750) | Potomac River |
| Widewater SP | Dahlgren VA (8635027) | Potomac River |
| Aquia Landing | Aquia Creek VA (8634858) | Potomac River |

## Scoring

Each low tide window (daylight hours, 1–7 days ahead) is scored out of 10:

```
window_score = base_score + tide_quality - weather_penalty - push_penalty + rain_bonus + wave_bonus
```

| Component | Max | Description |
|-----------|-----|-------------|
| Rain (72h decay) | +3 | Recent rainfall = erosion surfacing fossils |
| NWS advisory | +2 | Active coastal/marine advisory |
| Bay-clearing winds | +2 | W/SW winds lower effective tide |
| Marine waves | +2 | Wave churn prior 24h |
| Tide quality | +3 | % of tidal range exposed vs NOAA MHW datum |
| Spring tide bonus | +1 | Preceding high > 110% of MHW |
| Pre-window rain | +2 | Rain in hours leading up to low tide |
| Wave bonus at window | +1 | Wave height at tide time |
| Weather penalty | −2 | Thunderstorms during the window |
| Push penalty | −2 | Strong bay-pushing winds |

Windows scoring ≥ 4/10 qualify. Best windows typically score 7–9 after a storm with spring tides.

## Running

```bash
pip install flask werkzeug
APPLICATION_ROOT=/fossil python fossil_server.py
```

Visit: `http://localhost:5003/fossil/`

## Data Sources

- **Tides**: NOAA Tides & Currents API (tidesandcurrents.noaa.gov)
- **Weather**: Open-Meteo (open-meteo.com)
- **Marine**: Open-Meteo Marine API
- **Alerts**: NWS API (api.weather.gov)
- **Datums**: NOAA Datums API (normalized to MLLW=0 reference)

## License

MIT
