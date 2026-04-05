#!/usr/bin/env python3
"""
dhan_api_server.py  —  NSE Options FastAPI Backend (FULL VERSION)
Run: uvicorn dhan_api_server:app --reload --port 8000
"""

import os
import csv
import io
import requests
import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ── CREDENTIALS ───────────────────────────────────────────────
CLIENT_ID    = os.environ.get("CLIENT_ID",    "YOUR_CLIENT_ID_HERE")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "YOUR_ACCESS_TOKEN_HERE")

BASE_URL = "https://api.dhan.co/v2"
HEADERS  = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "Content-Type": "application/json",
}

# ── LOAD ALL NSE F&O SYMBOLS FROM DHAN SCRIP MASTER ──────────
def load_symbol_map():
    """Downloads Dhan official scrip master CSV and builds symbol map for all F&O stocks"""
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    try:
        resp   = requests.get(url, timeout=30)
        reader = csv.DictReader(io.StringIO(resp.text))
        symbol_map = {}
        for row in reader:
            instrument  = row.get("SEM_INSTRUMENT_NAME", "")
            segment     = row.get("SEM_EXM_EXCH_ID", "")
            scrip_id    = row.get("SEM_SMST_SECURITY_ID", "")
            underlying  = row.get("UNDERLYING_SYMBOL", "")
            und_id      = row.get("UNDERLYING_SECURITY_ID", scrip_id)
            if instrument in ("OPTIDX", "OPTSTK") and scrip_id and underlying and und_id:
                seg = "IDX_I" if instrument == "OPTIDX" else "NSE_EQ"
                try:
                    symbol_map[underlying] = {
                        "scrip_code": int(float(und_id)),
                        "segment":    seg,
                    }
                except (ValueError, TypeError):
                    pass
        if symbol_map:
            print(f"[INFO] Loaded {len(symbol_map)} F&O symbols from Dhan scrip master")
            return symbol_map
        else:
            raise ValueError("Empty symbol map from CSV")
    except Exception as e:
        print(f"[WARNING] Could not load scrip master ({e}), using fallback map")
        return {
            "NIFTY":      {"scrip_code": 13,    "segment": "IDX_I"},
            "BANKNIFTY":  {"scrip_code": 25,    "segment": "IDX_I"},
            "FINNIFTY":   {"scrip_code": 27,    "segment": "IDX_I"},
            "MIDCPNIFTY": {"scrip_code": 442,   "segment": "IDX_I"},
            "RELIANCE":   {"scrip_code": 1333,  "segment": "NSE_EQ"},
            "SBIN":       {"scrip_code": 3045,  "segment": "NSE_EQ"},
            "INFY":       {"scrip_code": 1594,  "segment": "NSE_EQ"},
            "TCS":        {"scrip_code": 11536, "segment": "NSE_EQ"},
            "ICICIBANK":  {"scrip_code": 4963,  "segment": "NSE_EQ"},
            "HDFCBANK":   {"scrip_code": 1330,  "segment": "NSE_EQ"},
            "WIPRO":      {"scrip_code": 3787,  "segment": "NSE_EQ"},
            "AXISBANK":   {"scrip_code": 5900,  "segment": "NSE_EQ"},
            "BHARTIARTL": {"scrip_code": 10604, "segment": "NSE_EQ"},
            "BAJFINANCE": {"scrip_code": 317,   "segment": "NSE_EQ"},
            "TATAMOTORS": {"scrip_code": 3456,  "segment": "NSE_EQ"},
            "TATASTEEL":  {"scrip_code": 3499,  "segment": "NSE_EQ"},
            "ITC":        {"scrip_code": 1660,  "segment": "NSE_EQ"},
            "SUNPHARMA":  {"scrip_code": 3351,  "segment": "NSE_EQ"},
            "KOTAKBANK":  {"scrip_code": 1922,  "segment": "NSE_EQ"},
            "LT":         {"scrip_code": 11483, "segment": "NSE_EQ"},
            "ONGC":       {"scrip_code": 2475,  "segment": "NSE_EQ"},
            "NTPC":       {"scrip_code": 11630, "segment": "NSE_EQ"},
            "ADANIPORTS": {"scrip_code": 15083, "segment": "NSE_EQ"},
            "ASIANPAINT": {"scrip_code": 236,   "segment": "NSE_EQ"},
            "TECHM":      {"scrip_code": 13538, "segment": "NSE_EQ"},
            "HCLTECH":    {"scrip_code": 1348,  "segment": "NSE_EQ"},
            "MARUTI":     {"scrip_code": 10999, "segment": "NSE_EQ"},
            "DRREDDY":    {"scrip_code": 881,   "segment": "NSE_EQ"},
            "CIPLA":      {"scrip_code": 694,   "segment": "NSE_EQ"},
            "BPCL":       {"scrip_code": 526,   "segment": "NSE_EQ"},
        }

SYMBOL_MAP = load_symbol_map()

# ── FASTAPI APP ───────────────────────────────────────────────
app = FastAPI(title="NSE Options API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HTTP HELPER ───────────────────────────────────────────────
def dhan_post(endpoint, body):
    url  = BASE_URL + endpoint
    resp = requests.post(url, json=body, headers=HEADERS, timeout=30)
    if resp.status_code == 401:
        raise HTTPException(401, "Invalid Dhan credentials")
    if resp.status_code == 429:
        raise HTTPException(429, "Rate limited — wait 3s")
    if resp.status_code != 200:
        raise HTTPException(resp.status_code, resp.text)
    return resp.json()

# ── CALCULATION HELPERS ───────────────────────────────────────
def calc_pcr(rows):
    ce = sum(r["ce_oi"] for r in rows)
    pe = sum(r["pe_oi"] for r in rows)
    return round(pe / ce, 3) if ce else 0.0

def calc_max_pain(rows):
    if not rows:
        return 0
    rb      = {r["strike"]: r for r in rows}
    strikes = sorted(rb.keys())
    pain    = {}
    for i, k in enumerate(strikes):
        p  = sum(rb[s]["ce_oi"] * max(0.0, k - s) for s in strikes[:i])
        p += sum(rb[s]["pe_oi"] * max(0.0, s - k) for s in strikes[i+1:])
        pain[k] = p
    return min(pain, key=lambda x: pain[x])

def parse_chain(raw, symbol):
    data  = raw.get("data", {})
    spot  = data.get("last_price", 0)
    oc    = data.get("oc", {})
    rows  = []
    for strike_str, cp in oc.items():
        try:
            strike = float(strike_str)
        except ValueError:
            continue
        ce     = cp.get("ce", {}) or cp.get("CE", {})
        pe     = cp.get("pe", {}) or cp.get("PE", {})
        ceg    = ce.get("greeks", {}) or {}
        peg    = pe.get("greeks", {}) or {}
        rows.append({
            "strike":         strike,
            "ce_oi":          ce.get("oi", 0),
            "ce_prev_oi":     ce.get("previous_oi", 0),
            "ce_ltp":         ce.get("last_price", 0),
            "ce_prev_close":  ce.get("previous_close_price", 0),
            "ce_iv":          ce.get("implied_volatility", 0) or ce.get("iv", 0),
            "ce_volume":      ce.get("volume", 0),
            "ce_bid":         ce.get("top_bid_price", 0),
            "ce_ask":         ce.get("top_ask_price", 0),
            "ce_delta":       ceg.get("delta", 0),
            "ce_theta":       ceg.get("theta", 0),
            "ce_gamma":       ceg.get("gamma", 0),
            "ce_vega":        ceg.get("vega", 0),
            "pe_oi":          pe.get("oi", 0),
            "pe_prev_oi":     pe.get("previous_oi", 0),
            "pe_ltp":         pe.get("last_price", 0),
            "pe_prev_close":  pe.get("previous_close_price", 0),
            "pe_iv":          pe.get("implied_volatility", 0) or pe.get("iv", 0),
            "pe_volume":      pe.get("volume", 0),
            "pe_bid":         pe.get("top_bid_price", 0),
            "pe_ask":         pe.get("top_ask_price", 0),
            "pe_delta":       peg.get("delta", 0),
            "pe_theta":       peg.get("theta", 0),
            "pe_gamma":       peg.get("gamma", 0),
            "pe_vega":        peg.get("vega", 0),
        })
    rows.sort(key=lambda r: r["strike"])
    return spot, rows

# ═══════════════════════════════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════════════════════════════

# ── 1. HEALTH ─────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "symbols_loaded": len(SYMBOL_MAP),
            "time": datetime.datetime.now().isoformat()}

# ── 2. SYMBOLS ────────────────────────────────────────────────
@app.get("/api/symbols")
def get_symbols():
    """All available F&O symbols (100+ from Dhan scrip master)"""
    indices = [s for s, v in SYMBOL_MAP.items() if v["segment"] == "IDX_I"]
    stocks  = sorted([s for s, v in SYMBOL_MAP.items() if v["segment"] == "NSE_EQ"])
    return {
        "symbols":  sorted(indices) + stocks,
        "indices":  sorted(indices),
        "stocks":   stocks,
        "total":    len(SYMBOL_MAP),
    }

# ── 3. EXPIRY LIST ────────────────────────────────────────────
@app.get("/api/expiry/{symbol}")
def get_expiry(symbol: str):
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, f"Unknown symbol: {sym}")
    body = {
        "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
        "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
    }
    data = dhan_post("/optionchain/expirylist", body)
    return {"symbol": sym, "expiries": data.get("data", [])}

# ── 4. LIVE OPTION CHAIN ──────────────────────────────────────
@app.get("/api/chain/{symbol}/{expiry}")
def get_chain(symbol: str, expiry: str):
    """Live option chain with spot, OI, greeks, PCR, Max Pain, straddle"""
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, f"Unknown symbol: {sym}")
    body = {
        "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
        "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
        "Expiry":          expiry,
    }
    raw        = dhan_post("/optionchain", body)
    spot, rows = parse_chain(raw, sym)
    atm        = min((r["strike"] for r in rows), key=lambda x: abs(x - spot)) if rows else 0
    pcr        = calc_pcr(rows)
    max_pain   = calc_max_pain(rows)
    atm_row    = next((r for r in rows if r["strike"] == atm), {})
    straddle   = round(atm_row.get("ce_ltp", 0) + atm_row.get("pe_ltp", 0), 2)
    total_ce   = sum(r["ce_oi"] for r in rows)
    total_pe   = sum(r["pe_oi"] for r in rows)
    if   pcr > 1.3: bias = "STRONG BEARISH"
    elif pcr > 1.2: bias = "BEARISH"
    elif pcr < 0.5: bias = "STRONG BULLISH"
    elif pcr < 0.7: bias = "BULLISH"
    else:           bias = "NEUTRAL"
    return {
        "symbol":      sym,
        "expiry":      expiry,
        "spot":        spot,
        "atm_strike":  atm,
        "pcr":         pcr,
        "bias":        bias,
        "max_pain":    max_pain,
        "total_ce_oi": total_ce,
        "total_pe_oi": total_pe,
        "straddle":    straddle,
        "upper_be":    round(atm + straddle, 2),
        "lower_be":    round(atm - straddle, 2),
        "timestamp":   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows":        rows,
    }

# ── 5. HISTORICAL PRICE DATA (Pre-Market) ─────────────────────
@app.get("/api/history/{symbol}/{days}")
def get_history(symbol: str, days: int = 30):
    """Historical OHLCV candles — use before market opens to see past trends"""
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, f"Unknown symbol: {sym}")
    today     = datetime.date.today()
    from_date = (today - datetime.timedelta(days=int(days))).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    info      = SYMBOL_MAP[sym]
    body = {
        "securityId":      str(info["scrip_code"]),
        "exchangeSegment": info["segment"],
        "instrument":      "INDEX" if info["segment"] == "IDX_I" else "EQUITY",
        "expiryCode":      0,
        "oi":              False,
        "fromDate":        from_date,
        "toDate":          to_date,
    }
    raw     = dhan_post("/charts/historical", body)
    ts      = raw.get("timestamp", [])
    opens   = raw.get("open",   [])
    highs   = raw.get("high",   [])
    lows    = raw.get("low",    [])
    closes  = raw.get("close",  [])
    vols    = raw.get("volume", [])
    candles = []
    for i in range(len(ts)):
        candles.append({
            "date":   datetime.datetime.fromtimestamp(ts[i]).strftime("%Y-%m-%d"),
            "open":   opens[i]  if i < len(opens)  else 0,
            "high":   highs[i]  if i < len(highs)  else 0,
            "low":    lows[i]   if i < len(lows)   else 0,
            "close":  closes[i] if i < len(closes) else 0,
            "volume": vols[i]   if i < len(vols)   else 0,
        })
    prev         = candles[-2] if len(candles) >= 2 else {}
    latest       = candles[-1] if len(candles) >= 1 else {}
    change       = round(latest.get("close", 0) - prev.get("close", 0), 2)
    change_pct   = round((change / prev["close"]) * 100, 2) if prev.get("close") else 0
    week_high    = max((c["high"]  for c in candles[-5:]), default=0)
    week_low     = min((c["low"]   for c in candles[-5:]), default=0)
    month_high   = max((c["high"]  for c in candles), default=0)
    month_low    = min((c["low"]   for c in candles), default=0)
    return {
        "symbol":       sym,
        "from_date":    from_date,
        "to_date":      to_date,
        "candles":      candles,
        "prev_open":    prev.get("open",  0),
        "prev_high":    prev.get("high",  0),
        "prev_low":     prev.get("low",   0),
        "prev_close":   prev.get("close", 0),
        "today_change": change,
        "change_pct":   change_pct,
        "week_high":    week_high,
        "week_low":     week_low,
        "month_high":   month_high,
        "month_low":    month_low,
    }

# ── 6. PRE-MARKET OI ANALYSIS ─────────────────────────────────
@app.get("/api/premarket/{symbol}/{expiry}")
def get_premarket(symbol: str, expiry: str):
    """Compare current OI vs previous OI — ideal for pre-market analysis before 9:15 AM"""
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, f"Unknown symbol: {sym}")
    body = {
        "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
        "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
        "Expiry":          expiry,
    }
    raw        = dhan_post("/optionchain", body)
    spot, rows = parse_chain(raw, sym)
    result = []
    for r in rows:
        ce_oi_chg = r["ce_oi"] - r["ce_prev_oi"]
        pe_oi_chg = r["pe_oi"] - r["pe_prev_oi"]
        result.append({
            "strike":          r["strike"],
            "ce_oi":           r["ce_oi"],
            "ce_prev_oi":      r["ce_prev_oi"],
            "ce_oi_change":    ce_oi_chg,
            "ce_prev_close":   r["ce_prev_close"],
            "ce_ltp":          r["ce_ltp"],
            "pe_oi":           r["pe_oi"],
            "pe_prev_oi":      r["pe_prev_oi"],
            "pe_oi_change":    pe_oi_chg,
            "pe_prev_close":   r["pe_prev_close"],
            "pe_ltp":          r["pe_ltp"],
        })
    total_ce_chg = sum(r["ce_oi_change"] for r in result)
    total_pe_chg = sum(r["pe_oi_change"] for r in result)
    # Key support = strike with highest PE OI (put writers defend it)
    max_pe_row = max(result, key=lambda x: x["pe_oi"], default={})
    # Key resistance = strike with highest CE OI (call writers defend it)
    max_ce_row = max(result, key=lambda x: x["ce_oi"], default={})
    if   total_pe_chg > total_ce_chg * 1.2: bias = "BULLISH"
    elif total_ce_chg > total_pe_chg * 1.2: bias = "BEARISH"
    else:                                    bias = "NEUTRAL"
    return {
        "symbol":             sym,
        "expiry":             expiry,
        "spot":               spot,
        "total_ce_oi_change": total_ce_chg,
        "total_pe_oi_change": total_pe_chg,
        "bias":               bias,
        "key_resistance":     max_ce_row.get("strike", 0),
        "key_support":        max_pe_row.get("strike", 0),
        "strikes":            result,
    }
