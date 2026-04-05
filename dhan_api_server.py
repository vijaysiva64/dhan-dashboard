#!/usr/bin/env python3
"""
dhan_api_server.py  -  NSE Options FastAPI Backend v3 (All 215 F&O Symbols)
Run: uvicorn dhan_api_server:app --reload --port 8000
"""

import os
import requests
import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

CLIENT_ID    = os.environ.get("CLIENT_ID",    "YOUR_CLIENT_ID_HERE")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "YOUR_ACCESS_TOKEN_HERE")

BASE_URL = "https://api.dhan.co/v2"
HEADERS  = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "Content-Type": "application/json",
}

# ── ALL 215 F&O SYMBOLS ──────────────────────────────────────
SYMBOL_MAP = {
    # ── INDICES ──
    "NIFTY":      {"scrip_code": 13,    "segment": "IDX_I"},
    "BANKNIFTY":  {"scrip_code": 25,    "segment": "IDX_I"},
    "FINNIFTY":   {"scrip_code": 27,    "segment": "IDX_I"},
    "MIDCPNIFTY": {"scrip_code": 442,   "segment": "IDX_I"},
    # ── F&O STOCKS ──
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
    "POWERGRID":  {"scrip_code": 14978, "segment": "NSE_EQ"},
    "COALINDIA":  {"scrip_code": 11670, "segment": "NSE_EQ"},
    "SBILIFE":    {"scrip_code": 2188,  "segment": "NSE_EQ"},
    "HDFCLIFE":   {"scrip_code": 11543, "segment": "NSE_EQ"},
    "TITAN":      {"scrip_code": 3544,  "segment": "NSE_EQ"},
    "BAJAJFINSV": {"scrip_code": 16675, "segment": "NSE_EQ"},
    "JSWSTEEL":   {"scrip_code": 11723, "segment": "NSE_EQ"},
    "HINDALCO":   {"scrip_code": 3832,  "segment": "NSE_EQ"},
    "VEDL":       {"scrip_code": 3595,  "segment": "NSE_EQ"},
    "NESTLEIND":  {"scrip_code": 17967, "segment": "NSE_EQ"},
    "ULTRACEMCO": {"scrip_code": 17014, "segment": "NSE_EQ"},
    "ADANIENT":   {"scrip_code": 15025, "segment": "NSE_EQ"},
    "ADANIGREEN": {"scrip_code": 15141, "segment": "NSE_EQ"},
    "ADANIPOWER": {"scrip_code": 15142, "segment": "NSE_EQ"},
    "AMBUJACEM":  {"scrip_code": 1580,  "segment": "NSE_EQ"},
    "AUROPHARMA": {"scrip_code": 269,   "segment": "NSE_EQ"},
    "BAJAJ-AUTO": {"scrip_code": 317,   "segment": "NSE_EQ"},
    "BERGEPAINT": {"scrip_code": 404,   "segment": "NSE_EQ"},
    "BIOCON":     {"scrip_code": 515,   "segment": "NSE_EQ"},
    "BOSCHLTD":   {"scrip_code": 687,   "segment": "NSE_EQ"},
    "BRITANNIA":  {"scrip_code": 707,   "segment": "NSE_EQ"},
    "CANBK":      {"scrip_code": 1079,  "segment": "NSE_EQ"},
    "CASTROLIND": {"scrip_code": 1133,  "segment": "NSE_EQ"},
    "CHOLAFIN":   {"scrip_code": 1221,  "segment": "NSE_EQ"},
    "COLPAL":     {"scrip_code": 1514,  "segment": "NSE_EQ"},
    "CONCOR":     {"scrip_code": 1230,  "segment": "NSE_EQ"},
    "CUMMINSIND": {"scrip_code": 1901,  "segment": "NSE_EQ"},
    "DABUR":      {"scrip_code": 1926,  "segment": "NSE_EQ"},
    "DEEPAKNTR":  {"scrip_code": 1996,  "segment": "NSE_EQ"},
    "DIVISLAB":   {"scrip_code": 10940, "segment": "NSE_EQ"},
    "DLF":        {"scrip_code": 14332, "segment": "NSE_EQ"},
    "DMART":      {"scrip_code": 14785, "segment": "NSE_EQ"},
    "EICHERMOT":  {"scrip_code": 2048,  "segment": "NSE_EQ"},
    "ESCORTS":    {"scrip_code": 2151,  "segment": "NSE_EQ"},
    "FEDERALBNK": {"scrip_code": 2243,  "segment": "NSE_EQ"},
    "GAIL":       {"scrip_code": 12804, "segment": "NSE_EQ"},
    "GLENMARK":   {"scrip_code": 2456,  "segment": "NSE_EQ"},
    "GODREJCP":   {"scrip_code": 2474,  "segment": "NSE_EQ"},
    "GRASIM":     {"scrip_code": 2510,  "segment": "NSE_EQ"},
    "HAVELLS":    {"scrip_code": 2556,  "segment": "NSE_EQ"},
    "HEROMOTOCO": {"scrip_code": 2782,  "segment": "NSE_EQ"},
    "HINDUNILVR": {"scrip_code": 1394,  "segment": "NSE_EQ"},
    "IBULHSGFIN": {"scrip_code": 3017,  "segment": "NSE_EQ"},
    "ICICIPRULI": {"scrip_code": 18652, "segment": "NSE_EQ"},
    "IGL":        {"scrip_code": 3025,  "segment": "NSE_EQ"},
    "INDUSTOWER": {"scrip_code": 12431, "segment": "NSE_EQ"},
    "INDUSINDBK": {"scrip_code": 3048,  "segment": "NSE_EQ"},
    "IOB":        {"scrip_code": 3095,  "segment": "NSE_EQ"},
    "IOC":        {"scrip_code": 3122,  "segment": "NSE_EQ"},
    "JINDALSTEL": {"scrip_code": 6734,  "segment": "NSE_EQ"},
    "JUBLFOOD":   {"scrip_code": 3123,  "segment": "NSE_EQ"},
    "KALYAN":     {"scrip_code": 3142,  "segment": "NSE_EQ"},
    "L&TFH":      {"scrip_code": 11783, "segment": "NSE_EQ"},
    "LICHSGFIN":  {"scrip_code": 29395, "segment": "NSE_EQ"},
    "LTIM":       {"scrip_code": 10987, "segment": "NSE_EQ"},
    "LUPIN":      {"scrip_code": 3370,  "segment": "NSE_EQ"},
    "M&M":        {"scrip_code": 3383,  "segment": "NSE_EQ"},
    "M&MFIN":     {"scrip_code": 3391,  "segment": "NSE_EQ"},
    "MANKIND":    {"scrip_code": 10793, "segment": "NSE_EQ"},
    "MARICO":     {"scrip_code": 3395,  "segment": "NSE_EQ"},
    "METROPOLIS": {"scrip_code": 11762, "segment": "NSE_EQ"},
    "MGL":        {"scrip_code": 12433, "segment": "NSE_EQ"},
    "MOTHERSON":  {"scrip_code": 12077, "segment": "NSE_EQ"},
    "MPHASIS":    {"scrip_code": 11441, "segment": "NSE_EQ"},
    "MRF":        {"scrip_code": 3449,  "segment": "NSE_EQ"},
    "MUTHOOTFIN": {"scrip_code": 11654, "segment": "NSE_EQ"},
    "NAM-INDIA":  {"scrip_code": 11211, "segment": "NSE_EQ"},
    "NAUKRI":     {"scrip_code": 13751, "segment": "NSE_EQ"},
    "NMDC":       {"scrip_code": 10814, "segment": "NSE_EQ"},
    "OBEROIRLTY": {"scrip_code": 11983, "segment": "NSE_EQ"},
    "OIL":        {"scrip_code": 2483,  "segment": "NSE_EQ"},
    "PERSISTENT":{"scrip_code": 11765, "segment": "NSE_EQ"},
    "PETRONET":   {"scrip_code": 11152, "segment": "NSE_EQ"},
    "PFC":        {"scrip_code": 14263, "segment": "NSE_EQ"},
    "PIDILITIND": {"scrip_code": 3564,  "segment": "NSE_EQ"},
    "PIIND":      {"scrip_code": 12081, "segment": "NSE_EQ"},
    "PNB":        {"scrip_code": 3046,  "segment": "NSE_EQ"},
    "PVR":        {"scrip_code": 3616,  "segment": "NSE_EQ"},
    "RAMCOCEM":   {"scrip_code": 2710,  "segment": "NSE_EQ"},
    "RBLBANK":    {"scrip_code": 2598,  "segment": "NSE_EQ"},
    "RECLTD":     {"scrip_code": 15324, "segment": "NSE_EQ"},
    "SAIL":       {"scrip_code": 11683, "segment": "NSE_EQ"},
    "SHREECEM":   {"scrip_code": 3273,  "segment": "NSE_EQ"},
    "SIEMENS":    {"scrip_code": 3283,  "segment": "NSE_EQ"},
    "SRF":        {"scrip_code": 3266,  "segment": "NSE_EQ"},
    "STAR":       {"scrip_code": 11740, "segment": "NSE_EQ"},
    "SUNDARAMFIN":{"scrip_code": 3431,  "segment": "NSE_EQ"},
    "SUNTV":      {"scrip_code": 3591,  "segment": "NSE_EQ"},
    "SYNGENE":    {"scrip_code": 11763, "segment": "NSE_EQ"},
    "TATACONSUM": {"scrip_code": 3432,  "segment": "NSE_EQ"},
    "TATAELXSI":  {"scrip_code": 11764, "segment": "NSE_EQ"},
    "TATAPOWER":  {"scrip_code": 3426,  "segment": "NSE_EQ"},
    "TCNSBRANDS": {"scrip_code": 11786, "segment": "NSE_EQ"},
    "TECHNO":     {"scrip_code": 3506,  "segment": "NSE_EQ"},
    "TORNTPHARM": {"scrip_code": 3548,  "segment": "NSE_EQ"},
    "TRENT":      {"scrip_code": 3459,  "segment": "NSE_EQ"},
    "TVSMOTOR":   {"scrip_code": 3599,  "segment": "NSE_EQ"},
    "UBL":        {"scrip_code": 3614,  "segment": "NSE_EQ"},
    "UNITDSPR":   {"scrip_code": 14903, "segment": "NSE_EQ"},
    "UPL":        {"scrip_code": 2716,  "segment": "NSE_EQ"},
    "VBL":        {"scrip_code": 10083, "segment": "NSE_EQ"},
    "VEDANTF":    {"scrip_code": 11836, "segment": "NSE_EQ"},
    "VINATIS":    {"scrip_code": 11977, "segment": "NSE_EQ"},
    "VOLTAS":     {"scrip_code": 3686,  "segment": "NSE_EQ"},
    "WHIRLPOOL":  {"scrip_code": 18154, "segment": "NSE_EQ"},
    "WIPROLTD":   {"scrip_code": 3787,  "segment": "NSE_EQ"},
    "ZYDUSLIFE":  {"scrip_code": 3042,  "segment": "NSE_EQ"},
    "AARTIDRUGS": {"scrip_code": 10498, "segment": "NSE_EQ"},
    "ABB":        {"scrip_code": 10726, "segment": "NSE_EQ"},
    "ATUL":       {"scrip_code": 285,   "segment": "NSE_EQ"},
    "BANDHANBNK": {"scrip_code": 12096, "segment": "NSE_EQ"},
    "BANKBARODA": {"scrip_code": 4263,  "segment": "NSE_EQ"},
    "BEML":       {"scrip_code": 10888, "segment": "NSE_EQ"},
    "BEL":        {"scrip_code": 10865, "segment": "NSE_EQ"},
    "BHEL":       {"scrip_code": 10868, "segment": "NSE_EQ"},
    "CAMPUS":     {"scrip_code": 12115, "segment": "NSE_EQ"},
    "CDSL":       {"scrip_code": 12105, "segment": "NSE_EQ"},
    "CESC":       {"scrip_code": 11490, "segment": "NSE_EQ"},
    "CROMPTON":   {"scrip_code": 10660, "segment": "NSE_EQ"},
    "CYIENT":     {"scrip_code": 10986, "segment": "NSE_EQ"},
    "EQUITAS":    {"scrip_code": 11136, "segment": "NSE_EQ"},
    "FINOPTS":    {"scrip_code": 15074, "segment": "NSE_EQ"},
    "GMRINFRA":   {"scrip_code": 13584, "segment": "NSE_EQ"},
    "GODREJPROP": {"scrip_code": 14906, "segment": "NSE_EQ"},
    "GRINDWELL":  {"scrip_code": 11837, "segment": "NSE_EQ"},
    "HGINFRA":    {"scrip_code": 11889, "segment": "NSE_EQ"},
    "HINDPETRO":  {"scrip_code": 2828,  "segment": "NSE_EQ"},
    "HONAUT":     {"scrip_code": 2766,  "segment": "NSE_EQ"},
    "IDEA":       {"scrip_code": 14316, "segment": "NSE_EQ"},
    "IEX":        {"scrip_code": 12086, "segment": "NSE_EQ"},
    "IRB":        {"scrip_code": 14564, "segment": "NSE_EQ"},
    "IRCTC":      {"scrip_code": 13095, "segment": "NSE_EQ"},
    "ITI":        {"scrip_code": 11565, "segment": "NSE_EQ"},
    "JSL":        {"scrip_code": 12078, "segment": "NSE_EQ"},
    "KEI":        {"scrip_code": 11789, "segment": "NSE_EQ"},
    "LAURUSLABS": {"scrip_code": 12110, "segment": "NSE_EQ"},
    "LEMONTREE":  {"scrip_code": 11877, "segment": "NSE_EQ"},
    "MAHABANK":   {"scrip_code": 14324, "segment": "NSE_EQ"},
    "MAHINDCIE":  {"scrip_code": 14567, "segment": "NSE_EQ"},
    "MASTEK":     {"scrip_code": 12082, "segment": "NSE_EQ"},
    "MFSL":       {"scrip_code": 11833, "segment": "NSE_EQ"},
    "NBCC":       {"scrip_code": 11665, "segment": "NSE_EQ"},
    "NCC":        {"scrip_code": 12079, "segment": "NSE_EQ"},
    "NHPC":       {"scrip_code": 11678, "segment": "NSE_EQ"},
    "NLCINDIA":   {"scrip_code": 11712, "segment": "NSE_EQ"},
    "NRAIL":      {"scrip_code": 10872, "segment": "NSE_EQ"},
    "NTPC":       {"scrip_code": 11630, "segment": "NSE_EQ"},
    "ODISHCORP":  {"scrip_code": 12127, "segment": "NSE_EQ"},
    "PEL":        {"scrip_code": 11146, "segment": "NSE_EQ"},
    "PRINCEPIP":  {"scrip_code": 11887, "segment": "NSE_EQ"},
    "PRSMNHY":    {"scrip_code": 12111, "segment": "NSE_EQ"},
    "RATNAMANI":  {"scrip_code": 11796, "segment": "NSE_EQ"},
    "RBLBANK":    {"scrip_code": 2598,  "segment": "NSE_EQ"},
    "RJRINFRA":   {"scrip_code": 11908, "segment": "NSE_EQ"},
    "SCHAEFFLER": {"scrip_code": 10855, "segment": "NSE_EQ"},
    "SPAL":       {"scrip_code": 12118, "segment": "NSE_EQ"},
    "SUZLON":     {"scrip_code": 14382, "segment": "NSE_EQ"},
    "TATACHEM":   {"scrip_code": 3431,  "segment": "NSE_EQ"},
    "THOMASCOOK": {"scrip_code": 12103, "segment": "NSE_EQ"},
    "TIMKEN":     {"scrip_code": 10860, "segment": "NSE_EQ"},
    "UNIONBANK":  {"scrip_code": 14587, "segment": "NSE_EQ"},
    "VAIBHAV":    {"scrip_code": 12095, "segment": "NSE_EQ"},
    "VARROC":     {"scrip_code": 11871, "segment": "NSE_EQ"},
    "ZFCV":       {"scrip_code": 12121, "segment": "NSE_EQ"},
}

# ── FASTAPI APP ───────────────────────────────────────────────
app = FastAPI(title="NSE Options API", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── HTTP HELPER ───────────────────────────────────────────────
def dhan_post(endpoint, body):
    url  = BASE_URL + endpoint
    resp = requests.post(url, json=body, headers=HEADERS, timeout=30)
    if resp.status_code == 401:
        raise HTTPException(401, "Invalid Dhan credentials")
    if resp.status_code == 429:
        raise HTTPException(429, "Rate limited - wait 3s")
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
    rb = {r["strike"]: r for r in rows}
    strikes = sorted(rb.keys())
    pain = {}
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
        ce  = cp.get("ce", {}) or cp.get("CE", {})
        pe  = cp.get("pe", {}) or cp.get("PE", {})
        ceg = ce.get("greeks", {}) or {}
        peg = pe.get("greeks", {}) or {}
        rows.append({
            "strike":        strike,
            "ce_oi":         ce.get("oi", 0),
            "ce_prev_oi":    ce.get("previous_oi", 0),
            "ce_ltp":        ce.get("last_price", 0),
            "ce_prev_close": ce.get("previous_close_price", 0),
            "ce_iv":         ce.get("implied_volatility", 0) or ce.get("iv", 0),
            "ce_volume":     ce.get("volume", 0),
            "ce_bid":        ce.get("top_bid_price", 0),
            "ce_ask":        ce.get("top_ask_price", 0),
            "ce_delta":      ceg.get("delta", 0),
            "ce_theta":      ceg.get("theta", 0),
            "ce_gamma":      ceg.get("gamma", 0),
            "ce_vega":       ceg.get("vega", 0),
            "pe_oi":         pe.get("oi", 0),
            "pe_prev_oi":    pe.get("previous_oi", 0),
            "pe_ltp":        pe.get("last_price", 0),
            "pe_prev_close": pe.get("previous_close_price", 0),
            "pe_iv":         pe.get("implied_volatility", 0) or pe.get("iv", 0),
            "pe_volume":     pe.get("volume", 0),
            "pe_bid":        pe.get("top_bid_price", 0),
            "pe_ask":        pe.get("top_ask_price", 0),
            "pe_delta":      peg.get("delta", 0),
            "pe_theta":      peg.get("theta", 0),
            "pe_gamma":      peg.get("gamma", 0),
            "pe_vega":       peg.get("vega", 0),
        })
    rows.sort(key=lambda r: r["strike"])
    return spot, rows

# ═══════════════════════════════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {"status": "ok", "symbols_loaded": len(SYMBOL_MAP), "version": "3.0",
            "time": datetime.datetime.now().isoformat()}

@app.get("/api/symbols")
def get_symbols():
    indices = sorted([s for s, v in SYMBOL_MAP.items() if v["segment"] == "IDX_I"])
    stocks  = sorted([s for s, v in SYMBOL_MAP.items() if v["segment"] == "NSE_EQ"])
    return {"symbols": indices + stocks, "indices": indices, "stocks": stocks, "total": len(SYMBOL_MAP)}

@app.get("/api/expiry/{symbol}")
def get_expiry(symbol: str):
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, "Unknown symbol: " + sym)
    data = dhan_post("/optionchain/expirylist", {
        "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
        "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
    })
    return {"symbol": sym, "expiries": data.get("data", [])}

@app.get("/api/chain/{symbol}/{expiry}")
def get_chain(symbol: str, expiry: str):
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, "Unknown symbol: " + sym)
    raw        = dhan_post("/optionchain", {
        "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
        "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
        "Expiry":          expiry,
    })
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
        "symbol": sym, "expiry": expiry, "spot": spot, "atm_strike": atm,
        "pcr": pcr, "bias": bias, "max_pain": max_pain,
        "total_ce_oi": total_ce, "total_pe_oi": total_pe,
        "straddle": straddle,
        "upper_be": round(atm + straddle, 2),
        "lower_be": round(atm - straddle, 2),
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": rows,
    }

@app.get("/api/history/{symbol}/{days}")
def get_history(symbol: str, days: int = 30):
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, "Unknown symbol: " + sym)
    today     = datetime.date.today()
    from_date = (today - datetime.timedelta(days=int(days))).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    info      = SYMBOL_MAP[sym]
    raw = dhan_post("/charts/historical", {
        "securityId":      str(info["scrip_code"]),
        "exchangeSegment": info["segment"],
        "instrument":      "INDEX" if info["segment"] == "IDX_I" else "EQUITY",
        "expiryCode":      0, "oi": False,
        "fromDate":        from_date, "toDate": to_date,
    })
    ts     = raw.get("timestamp", [])
    opens  = raw.get("open",   [])
    highs  = raw.get("high",   [])
    lows   = raw.get("low",    [])
    closes = raw.get("close",  [])
    vols   = raw.get("volume", [])
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
    prev       = candles[-2] if len(candles) >= 2 else {}
    latest     = candles[-1] if len(candles) >= 1 else {}
    change     = round(latest.get("close", 0) - prev.get("close", 0), 2)
    change_pct = round((change / prev["close"]) * 100, 2) if prev.get("close") else 0
    return {
        "symbol": sym, "from_date": from_date, "to_date": to_date, "candles": candles,
        "prev_open":  prev.get("open",  0), "prev_high": prev.get("high", 0),
        "prev_low":   prev.get("low",   0), "prev_close": prev.get("close", 0),
        "today_change": change, "change_pct": change_pct,
        "week_high":  max((c["high"] for c in candles[-5:]),  default=0),
        "week_low":   min((c["low"]  for c in candles[-5:]),  default=0),
        "month_high": max((c["high"] for c in candles),       default=0),
        "month_low":  min((c["low"]  for c in candles),       default=0),
    }

@app.get("/api/premarket/{symbol}/{expiry}")
def get_premarket(symbol: str, expiry: str):
    sym = symbol.upper()
    if sym not in SYMBOL_MAP:
        raise HTTPException(404, "Unknown symbol: " + sym)
    raw        = dhan_post("/optionchain", {
        "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
        "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
        "Expiry":          expiry,
    })
    spot, rows = parse_chain(raw, sym)
    result = []
    for r in rows:
        result.append({
            "strike":        r["strike"],
            "ce_oi":         r["ce_oi"],
            "ce_prev_oi":    r["ce_prev_oi"],
            "ce_oi_change":  r["ce_oi"] - r["ce_prev_oi"],
            "ce_prev_close": r["ce_prev_close"],
            "ce_ltp":        r["ce_ltp"],
            "pe_oi":         r["pe_oi"],
            "pe_prev_oi":    r["pe_prev_oi"],
            "pe_oi_change":  r["pe_oi"] - r["pe_prev_oi"],
            "pe_prev_close": r["pe_prev_close"],
            "pe_ltp":        r["pe_ltp"],
        })
    total_ce_chg = sum(r["ce_oi_change"] for r in result)
    total_pe_chg = sum(r["pe_oi_change"] for r in result)
    max_pe_row   = max(result, key=lambda x: x["pe_oi"], default={})
    max_ce_row   = max(result, key=lambda x: x["ce_oi"], default={})
    if   total_pe_chg > total_ce_chg * 1.2: bias = "BULLISH"
    elif total_ce_chg > total_pe_chg * 1.2: bias = "BEARISH"
    else:                                    bias = "NEUTRAL"
    return {
        "symbol": sym, "expiry": expiry, "spot": spot,
        "total_ce_oi_change": total_ce_chg,
        "total_pe_oi_change": total_pe_chg,
        "bias": bias,
        "key_resistance": max_ce_row.get("strike", 0),
        "key_support":    max_pe_row.get("strike", 0),
        "strikes": result,
    }
