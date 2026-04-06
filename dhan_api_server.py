#!/usr/bin/env python3
"""
dhan_api_server.py  -  NSE Options FastAPI Backend v3 (All 215 F&O Symbols)
Run: uvicorn dhan_api_server:app --reload --port 8000
"""

import os
import io
import csv
import time
import requests
import datetime
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio

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
app = FastAPI(title="NSE Options API", version="4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def _scheduled_auto_scan():
    """Runs automatically at 9:09 AM every weekday."""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(auto_gap_scan(
            min_gap_pct=2.0,
            capital=50000,
            risk_pct=0.30,
            fetch_ltp=True
        ))
        print(f"[AUTO] Gap scan completed at {datetime.datetime.now()}")
    except Exception as e:
        print(f"[AUTO] Gap scan failed: {e}")
    finally:
        loop.close()

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
scheduler.add_job(
    _scheduled_auto_scan,
    trigger="cron",
    day_of_week="mon-fri",
    hour=9,
    minute=9,
    second=0,
)
scheduler.start()
print("[AUTO] Scheduler started — runs at 9:09 AM IST weekdays")

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


# ══════════════════════════════════════════════════════════════════
# GAP SCANNER
# ══════════════════════════════════════════════════════════════════

STRIKE_INTERVALS = {
    "360ONE":10,"ABB":50,"APLAPOLLO":20,"AUBANK":10,"ADANIENSOL":10,
    "ADANIENT":20,"ADANIGREEN":10,"ADANIPORTS":20,"ABCAPITAL":5,
    "ALKEM":50,"AMBER":100,"AMBUJACEM":5,"ANGELONE":5,"APOLLOHOSP":50,
    "ASHOKLEY":2.5,"ASIANPAINT":20,"ASTRAL":20,"AUROPHARMA":10,"DMART":50,
    "AXISBANK":10,"BSE":50,"BAJAJ-AUTO":100,"BAJFINANCE":10,"BAJAJFINSV":20,
    "BAJAJHLDNG":100,"BANDHANBNK":2.5,"BANKBARODA":2.5,"BANKINDIA":1,
    "BDL":20,"BEL":5,"BHARATFORG":20,"BHEL":2.5,"BPCL":5,"BHARTIARTL":10,
    "BIOCON":5,"BLUESTARCO":20,"BOSCHLTD":250,"BRITANNIA":50,"CGPOWER":10,
    "CANBK":1,"CDSL":20,"CHOLAFIN":20,"CIPLA":10,"COALINDIA":2.5,
    "COFORGE":20,"COLPAL":20,"CAMS":10,"CONCOR":5,"CROMPTON":2.5,
    "CUMMINSIND":50,"DLF":10,"DABUR":5,"DALBHARAT":20,"DELHIVERY":5,
    "DIVISLAB":50,"DIXON":100,"DRREDDY":10,"ETERNAL":5,"EICHERMOT":50,
    "EXIDEIND":2.5,"NYKAA":2.5,"FORTIS":10,"GAIL":1,"GMRAIRPORT":1,
    "GLENMARK":20,"GODREJCP":10,"GODREJPROP":20,"GRASIM":20,"HCLTECH":20,
    "HDFCAMC":20,"HDFCBANK":5,"HDFCLIFE":10,"HAVELLS":10,"HEROMOTOCO":50,
    "HINDALCO":10,"HAL":50,"HINDPETRO":5,"HINDUNILVR":20,"HINDZINC":5,
    "POWERINDIA":250,"HUDCO":2.5,"ICICIBANK":10,"ICICIGI":20,"ICICIPRULI":5,
    "IDFCFIRSTB":1,"ITC":2.5,"INDIANB":10,"IEX":1,"IOC":1,"IRFC":1,
    "IREDA":1,"INDUSTOWER":5,"INDUSINDBK":10,"NAUKRI":20,"INFY":20,
    "INOXWIND":2.5,"INDIGO":50,"JINDALSTEL":10,"JSWENERGY":5,"JSWSTEEL":10,
    "JIOFIN":2.5,"JUBLFOOD":5,"KEI":50,"KALYANKJIL":5,"KAYNES":50,
    "KFINTECH":20,"KOTAKBANK":2.5,"LTF":5,"LICHSGFIN":5,"LTM":50,"LT":20,
    "LAURUSLABS":10,"LICI":5,"LODHA":10,"LUPIN":20,"M&M":50,
    "MANAPPURAM":2.5,"MANKIND":20,"MARICO":5,"MARUTI":100,"MFSL":20,
    "MAXHEALTH":10,"MAZDOCK":20,"MPHASIS":50,"MCX":20,"MUTHOOTFIN":50,
    "NBCC":1,"NHPC":1,"NMDC":1,"NTPC":2.5,"NATIONALUM":2.5,"NESTLEIND":10,
    "NUVAMA":20,"OBEROIRLTY":20,"ONGC":1,"OIL":5,"PAYTM":20,"OFSS":100,
    "POLICYBZR":20,"PGEL":10,"PIIND":20,"PNBHOUSING":10,"PAGEIND":500,
    "PATANJALI":5,"PERSISTENT":100,"PETRONET":2.5,"PIDILITIND":10,
    "PPLPHARMA":2.5,"POLYCAB":100,"PFC":2.5,"POWERGRID":2.5,"PREMIERENE":10,
    "PRESTIGE":20,"PNB":1,"RBLBANK":5,"RECLTD":2.5,"RVNL":5,"RELIANCE":10,
    "SBICARD":10,"SBILIFE":20,"SHREECEM":250,"SRF":60,"SAMMAANCAP":2.5,
    "MOTHERSON":1,"SHRIRAMFIN":10,"SIEMENS":50,"SOLARINDS":100,"SONACOMS":5,
    "SBIN":5,"SAIL":1,"SUNPHARMA":10,"SUPREMEIND":50,"SUZLON":1,"SWIGGY":5,
    "TATACOMM":10,"TATACONSUM":5,"TATAMOTORS":1,"TATASTEEL":1,"TATAPOWER":2,
    "TCS":50,"TECHM":10,"TITAN":10,"TIINDIA":20,"TRENT":50,"TVSMOTOR":10,
    "UBL":20,"UNITDSPR":10,"UPL":5,"VBL":5,"VEDL":5,"VOLTAS":20,
    "WAAREEENER":50,"WIPRO":5,"YESBANK":1,"ZOMATO":5,"ZYDUSLIFE":10,
    "IDEA":1,"NBCC":1,"NMDC":1,"PNB":1,"SUZLON":1,"TATASTEEL":1,
    "HINDPETRO":5,"HCLTECH":20,"BPCL":5,"INDUSTOWER":5,"POLYCAB":100,
    "DIXON":100,"TRENT":50,"INDHOTEL":5,"TIINDIA":20,
    "TMPV":5,"BSE":50,"ANGELONE":5,"SHRIRAMFIN":10,"MARUTI":100,
    "RECLTD":2.5,"BLUESTARCO":20,"KFINTECH":20,"DALBHARAT":20,
    "INDIANB":10,"PNBHOUSING":10,"SWIGGY":5,"MUTHOOTFIN":50,
    "MANAPPURAM":2.5,"ABCAPITAL":5,"SAMMAANCAP":2.5,"LTF":5,
    "UNITDSPR":10,"BEL":5,"HINDZINC":5,"WAAREEENER":50,"CHOLAFIN":20,
    "VBL":5,"AMBUJACEM":5,"IOC":1,"OFSS":100,"JUBLFOOD":5,
    "LICHSGFIN":5,"COFORGE":20,"TATAPOWER":2,"HINDUNILVR":20,
    "INFY":20,"GODREJPROP":20,"PETRONET":2.5,"EICHERMOT":50,
    "COALINDIA":2.5,"POWERGRID":2.5,"NMDC":1,"RELIANCE":10,"DMART":50,
}

LOT_SIZES = {
    "NIFTY":75,"BANKNIFTY":30,"FINNIFTY":40,"MIDCPNIFTY":75,
    "RELIANCE":250,"SBIN":1500,"INFY":300,"TCS":150,"HDFCBANK":550,
    "ICICIBANK":700,"TATAMOTORS":900,"WIPRO":1500,"AXISBANK":1200,
    "BHARTIARTL":500,"BAJFINANCE":125,"KOTAKBANK":400,"SUNPHARMA":350,
    "TATASTEEL":5500,"ITC":3200,"ONGC":1925,"NTPC":2700,"LT":150,
    "MARUTI":75,"HCLTECH":300,"ADANIPORTS":1250,"ASIANPAINT":200,
    "TECHM":500,"BPCL":1800,"HINDUNILVR":300,"COALINDIA":2700,
    "POWERGRID":2700,"DLF":1650,"TITAN":375,"BAJAJFINSV":250,
    "JSWSTEEL":1350,"HINDALCO":2150,"DRREDDY":125,"CIPLA":650,
    "EICHERMOT":200,"DIVISLAB":200,"NESTLEIND":40,"ULTRACEMCO":100,
}
DEFAULT_LOT = 500

_last_scan_result = {}

def _atm_strike(iep, interval):
    return round(round(iep / interval) * interval, 2)

def _last_tuesday_of_month(year, month):
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    d = datetime.date(year, month, last_day)
    while d.weekday() != 1:
        d -= datetime.timedelta(days=1)
    return d

def _nearest_expiry_code():
    today = datetime.date.today()
    exp = _last_tuesday_of_month(today.year, today.month)
    if exp < today:
        if today.month == 12:
            exp = _last_tuesday_of_month(today.year + 1, 1)
        else:
            exp = _last_tuesday_of_month(today.year, today.month + 1)
    return exp.strftime("%y%m%d"), exp.strftime("%Y-%m-%d")

def _liquidity_score(oi, volume, ltp):
    s = 0
    if oi    >= 10000: s += 4
    elif oi  >=  1000: s += 2
    elif oi  >=   100: s += 1
    if volume >= 5000: s += 3
    elif volume >= 500: s += 2
    elif volume >= 50:  s += 1
    if 5 <= ltp <= 200: s += 2
    elif ltp <= 400:    s += 1
    return min(s, 10)

def _get_option_data(sym, atm, direction, expiry_iso):
    if sym not in SYMBOL_MAP:
        return None
    try:
        raw = dhan_post("/optionchain", {
            "UnderlyingScrip": SYMBOL_MAP[sym]["scrip_code"],
            "UnderlyingSeg":   SYMBOL_MAP[sym]["segment"],
            "Expiry":          expiry_iso,
        })
        spot, rows = parse_chain(raw, sym)
        if not rows:
            return None

        strikes = sorted(set(r["strike"] for r in rows))
        nearest = min(strikes, key=lambda x: abs(x - atm))
        diffs = [strikes[i+1]-strikes[i] for i in range(len(strikes)-1)]
        iv_guess = min(diffs) if diffs else 0

        candidates = [nearest]
        if iv_guess > 0:
            if direction == "CALL":
                candidates += [nearest + iv_guess, nearest + 2*iv_guess]
            else:
                candidates += [nearest - iv_guess, nearest - 2*iv_guess]

        row_map = {r["strike"]: r for r in rows}

        for c in candidates:
            best_k = min(row_map.keys(), key=lambda x: abs(x - c))
            r = row_map[best_k]
            ltp    = r["ce_ltp"]    if direction == "CALL" else r["pe_ltp"]
            oi_val = r["ce_oi"]     if direction == "CALL" else r["pe_oi"]
            volume = r["ce_volume"] if direction == "CALL" else r["pe_volume"]
            iv     = r["ce_iv"]     if direction == "CALL" else r["pe_iv"]

            if ltp < 5 or ltp > 500 or oi_val < 100:
                continue

            lots      = LOT_SIZES.get(sym, DEFAULT_LOT)
            cost_lot  = round(ltp * lots, 2)
            max_risk  = 50000 * 0.30
            affordable= int(max_risk // cost_lot) if cost_lot else 0
            bep       = round((r["ce_ltp"] + r["pe_ltp"]) / 2, 2)
            lscore    = _liquidity_score(oi_val, volume, ltp)

            return {
                "strike":         best_k,
                "ltp":            ltp,
                "oi":             oi_val,
                "volume":         volume,
                "iv":             iv,
                "ce_ltp":         r["ce_ltp"],
                "pe_ltp":         r["pe_ltp"],
                "bep":            bep,
                "liquidity":      lscore,
                "lot_size":       lots,
                "cost_per_lot":   cost_lot,
                "lots_affordable":affordable,
                "max_loss_rs":    int(affordable * cost_lot),
                "target_2x_rs":   int(affordable * cost_lot * 2),
                "sl_price":       round(ltp * 0.5, 2),
                "flag":           "OK" if lscore >= 4 else "LOW_LIQ",
            }
        return None
    except Exception:
        return None

def _parse_bhav_csv(content: str) -> dict:
    result = {}
    reader = csv.reader(io.StringIO(content))
    header = [h.strip().upper() for h in next(reader)]
    sym_i  = next((i for i,h in enumerate(header) if "SYMBOL" in h), 0)
    hi_i   = next((i for i,h in enumerate(header) if "HIGH"   in h and "PRICE" in h), 5)
    lo_i   = next((i for i,h in enumerate(header) if "LOW"    in h and "PRICE" in h), 6)
    cl_i   = next((i for i,h in enumerate(header) if "CLOSE"  in h), 7)
    for row in reader:
        if len(row) <= max(sym_i, hi_i, lo_i):
            continue
        sym = row[sym_i].strip().strip('"')
        try:
            result[sym] = {
                "HIGH":  float(row[hi_i].strip().replace(",","")),
                "LOW":   float(row[lo_i].strip().replace(",","")),
                "CLOSE": float(row[cl_i].strip().replace(",","")) if cl_i < len(row) else 0,
            }
        except (ValueError, IndexError):
            pass
    return result

def _parse_premarket_csv(content: str) -> dict:
    result = {}
    reader = csv.reader(io.StringIO(content))
    raw_h  = next(reader)
    header = [h.strip().replace("\n","").replace("\r","").upper() for h in raw_h]
    h_idx  = {col:i for i,col in enumerate(header)}
    sym_i  = h_idx.get("SYMBOL", 0)
    iep_i  = h_idx.get("IEP", 1)
    pct_i  = next((i for i, h in enumerate(header) if "CHNG" in h and "%" in h), 2)
    for row in reader:
        if len(row) <= max(sym_i, iep_i, pct_i):
            continue
        sym = row[sym_i].strip().strip('"')
        try:
            result[sym] = {
                "IEP": float(row[iep_i].strip().strip('"').replace(",","")),
                "PCT": float(row[pct_i].strip().strip('"')),
            }
        except (ValueError, IndexError):
            pass
    return result

@app.post("/api/gap-scan")
async def gap_scan(
    bhav_file:      UploadFile = File(...),
    premarket_file: UploadFile = File(...),
    min_gap_pct:    float      = Form(2.0),
    capital:        int        = Form(50000),
    risk_pct:       float      = Form(0.30),
    fetch_ltp:      bool       = Form(True),
):
    global _last_scan_result

    bhav_content = (await bhav_file.read()).decode("utf-8-sig")
    pm_content   = (await premarket_file.read()).decode("utf-8-sig")

    bhav      = _parse_bhav_csv(bhav_content)
    premarket = _parse_premarket_csv(pm_content)

    expiry_code, expiry_iso = _nearest_expiry_code()

    gap_up, gap_down, excluded, no_interval, low_liq = [], [], [], [], []

    for sym, pm in (premarket or {}).items():
        pct = pm["PCT"]
        iep = pm["IEP"]
        bh  = (bhav or {}).get(sym)
        if not bh:
            continue

        interval = STRIKE_INTERVALS.get(sym)
        if not interval:
            no_interval.append({"symbol": sym, "gap_pct": round(pct,2), "iep": iep})
            continue

        prev_high = bh["HIGH"]
        prev_low  = bh["LOW"]
        prev_close= bh.get("CLOSE", 0)

        if pct >= min_gap_pct and iep > prev_high:
            direction = "CALL"
            atm = _atm_strike(iep, interval)
            tv  = f"NSE:{sym}{expiry_code}C{int(atm)}"

            sdata = _get_option_data(sym, atm, direction, expiry_iso) if fetch_ltp else None
            if not sdata:
                low_liq.append({"symbol": sym, "gap_pct": round(pct,2)})
                time.sleep(0.35)

            row = {
                "symbol":          sym,
                "direction":       "CALL",
                "iep":             iep,
                "gap_pct":         round(pct,2),
                "prev_high":       prev_high,
                "prev_low":        None,
                "prev_close":      prev_close,
                "strike_interval": interval,
                "atm_strike":      sdata["strike"] if sdata else atm,
                "tv_symbol":       tv if not sdata else f"NSE:{sym}{expiry_code}C{int(sdata['strike'])}",
                "ce_ltp":          sdata["ce_ltp"]         if sdata else 0,
                "pe_ltp":          sdata["pe_ltp"]         if sdata else 0,
                "ltp":             sdata["ltp"]            if sdata else 0,
                "bep":             sdata["bep"]            if sdata else 0,
                "oi":              sdata["oi"]             if sdata else 0,
                "volume":          sdata["volume"]         if sdata else 0,
                "iv_pct":          sdata["iv"]             if sdata else 0,
                "liquidity":       sdata["liquidity"]      if sdata else 0,
                "lot_size":        sdata["lot_size"]       if sdata else LOT_SIZES.get(sym, DEFAULT_LOT),
                "cost_per_lot":    sdata["cost_per_lot"]   if sdata else 0,
                "lots_affordable": sdata["lots_affordable"]if sdata else 0,
                "max_loss_rs":     sdata["max_loss_rs"]    if sdata else 0,
                "target_2x_rs":    sdata["target_2x_rs"]  if sdata else 0,
                "sl_price":        sdata["sl_price"]       if sdata else 0,
                "flag":            sdata["flag"]           if sdata else "NO_DATA",
                "gap_tier":        "POWER" if pct>=5 else "STRONG" if pct>=3 else "NORMAL",
                "entry_condition": iep > prev_high,
            }
            gap_up.append(row)

        elif pct <= -min_gap_pct and iep < prev_low:
            direction = "PUT"
            atm = _atm_strike(iep, interval)
            tv  = f"NSE:{sym}{expiry_code}P{int(atm)}"

            sdata = _get_option_data(sym, atm, direction, expiry_iso) if fetch_ltp else None
            if not sdata:
                low_liq.append({"symbol": sym, "gap_pct": round(pct,2)})
                time.sleep(0.35)

            row = {
                "symbol":          sym,
                "direction":       "PUT",
                "iep":             iep,
                "gap_pct":         round(pct,2),
                "prev_high":       None,
                "prev_low":        prev_low,
                "prev_close":      prev_close,
                "strike_interval": interval,
                "atm_strike":      sdata["strike"] if sdata else atm,
                "tv_symbol":       tv if not sdata else f"NSE:{sym}{expiry_code}P{int(sdata['strike'])}",
                "ce_ltp":          sdata["ce_ltp"]         if sdata else 0,
                "pe_ltp":          sdata["pe_ltp"]         if sdata else 0,
                "ltp":             sdata["ltp"]            if sdata else 0,
                "bep":             sdata["bep"]            if sdata else 0,
                "oi":              sdata["oi"]             if sdata else 0,
                "volume":          sdata["volume"]         if sdata else 0,
                "iv_pct":          sdata["iv"]             if sdata else 0,
                "liquidity":       sdata["liquidity"]      if sdata else 0,
                "lot_size":        sdata["lot_size"]       if sdata else LOT_SIZES.get(sym, DEFAULT_LOT),
                "cost_per_lot":    sdata["cost_per_lot"]   if sdata else 0,
                "lots_affordable": sdata["lots_affordable"]if sdata else 0,
                "max_loss_rs":     sdata["max_loss_rs"]    if sdata else 0,
                "target_2x_rs":    sdata["target_2x_rs"]  if sdata else 0,
                "sl_price":        sdata["sl_price"]       if sdata else 0,
                "flag":            sdata["flag"]           if sdata else "NO_DATA",
                "gap_tier":        "POWER" if abs(pct)>=5 else "STRONG" if abs(pct)>=3 else "NORMAL",
                "entry_condition": iep < prev_low,
            }
            gap_down.append(row)

        elif abs(pct) >= min_gap_pct:
            excluded.append({
                "symbol": sym, "gap_pct": round(pct,2), "iep": iep,
                "prev_high": prev_high, "prev_low": prev_low,
                "reason": "IEP did not break prev high/low",
            })

    gap_up.sort(key=lambda x: (-x["gap_pct"], -x["liquidity"]))
    gap_down.sort(key=lambda x: (x["gap_pct"], -x["liquidity"]))

    watchlist_txt = "# GAP UP CALLS\n"
    for r in gap_up:
        tag = "" if r["flag"] == "OK" else "  # verify liquidity"
        watchlist_txt += f"{r['tv_symbol']},{tag}\n"
    watchlist_txt += "\n# GAP DOWN PUTS\n"
    for r in gap_down:
        tag = "" if r["flag"] == "OK" else "  # verify liquidity"
        watchlist_txt += f"{r['tv_symbol']},{tag}\n"

    result = {
        "scan_time":       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "expiry_code":     expiry_code,
        "expiry_iso":      expiry_iso,
        "total_gap_up":    len(gap_up),
        "total_gap_down":  len(gap_down),
        "top_picks":       [r for r in (gap_up + gap_down) if r["flag"] == "OK" and r["lots_affordable"] >= 1],
        "gap_up":          gap_up,
        "gap_down":        gap_down,
        "excluded":        excluded[:20],
        "no_interval":     no_interval[:20],
        "low_liquidity":   low_liq[:20],
        "watchlist_txt":   watchlist_txt,
        "capital":         capital,
        "risk_pct":        risk_pct,
    }

    _last_scan_result = result
    try:
        with open("last_gap_scan.json", "w") as f:
            json.dump(result, f)
    except Exception:
        pass
    return result

@app.get("/api/gap-scan/last")
def get_last_scan():
    """Return last scan result — empty response if no scan has run yet."""
    import json
    result_file = "last_gap_scan.json"
    if not os.path.exists(result_file):
        return {
            "scan_time": None,
            "total_gap_up": 0,
            "total_gap_down": 0,
            "top_picks": [],
            "gap_up": [],
            "gap_down": [],
            "excluded": [],
            "message": "No scan results yet. Auto-scan fires at 9:09 AM IST weekdays."
        }
    try:
        with open(result_file, "r") as f:
            return json.load(f)
    except Exception as e:
        return {
            "scan_time": None,
            "top_picks": [],
            "gap_up": [],
            "gap_down": [],
            "excluded": [],
            "message": f"Error reading scan: {str(e)}"
        }

@app.get("/api/gap-scan/watchlist")
def get_watchlist():
    if not _last_scan_result:
        raise HTTPException(404, "No scan run yet.")
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(_last_scan_result.get("watchlist_txt",""), media_type="text/plain")


# ══════════════════════════════════════════════════════════════════
# AUTO GAP SCAN
# ══════════════════════════════════════════════════════════════════

NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://www.nseindia.com/",
}

def _get_nse_session():
    """Create a requests session with NSE cookies."""
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com", timeout=15)
        time.sleep(1)
        s.get("https://www.nseindia.com/market-data/pre-open-market-fno", timeout=15)
        time.sleep(0.5)
    except Exception:
        pass
    return s

def _prev_trading_day():
    """Return previous trading day as DDMMYYYY string."""
    d = datetime.date.today() - datetime.timedelta(days=1)
    while d.weekday() >= 5:
        d -= datetime.timedelta(days=1)
    return d.strftime("%d%m%Y"), d

def _download_bhav(session=None):
    """Download NSE bhav copy for previous trading day."""
    date_str, date_obj = _prev_trading_day()
    url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
    headers = dict(NSE_HEADERS)
    headers["Referer"] = "https://www.nseindia.com/"
    try:
        if session:
            resp = session.get(url, timeout=20)
        else:
            resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return None, f"Bhav copy not found for {date_str} (HTTP {resp.status_code})"
        content = resp.content.decode("utf-8-sig")
        return _parse_bhav_csv(content), None
    except Exception as e:
        return None, f"Bhav download failed: {str(e)}"

def _download_premarket(session=None):
    """Download NSE pre-market F&O data via NSE API."""
    url = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
    try:
        if session:
            resp = session.get(url, timeout=20)
        else:
            s = _get_nse_session()
            resp = s.get(url, timeout=20)
        if resp.status_code != 200:
            return None, f"Pre-market API returned HTTP {resp.status_code}"
        data = resp.json()
        items = data.get("data", [])
        if not items:
            return None, "Pre-market data is empty — market may not have opened yet"
        result = {}
        for item in items:
            meta = item.get("metadata", {})
            sym  = meta.get("symbol", "").strip()
            iep  = meta.get("iep", 0)
            pct  = meta.get("pChange", 0)
            if sym and iep:
                try:
                    result[sym] = {
                        "IEP": float(str(iep).replace(",", "")),
                        "PCT": float(str(pct).replace(",", "")),
                    }
                except (ValueError, TypeError):
                    pass
        return result, None
    except Exception as e:
        return None, f"Pre-market download failed: {str(e)}"

@app.get("/api/gap-scan/auto")
async def auto_gap_scan(
    min_gap_pct: float = 2.0,
    capital:     int   = 50000,
    risk_pct:    float = 0.30,
    fetch_ltp:   bool  = True,
):
    """
    Fully automatic gap scan — no file uploads needed.
    Automatically downloads previous day Bhav Copy + today's pre-market data from NSE.
    Best called between 9:05 AM and 9:08 AM IST.
    """
    global _last_scan_result

    session = _get_nse_session()
    bhav, bhav_err = _download_bhav(session)
    if bhav_err:
        raise HTTPException(503, f"Bhav Copy Error: {bhav_err}")

    premarket, pm_err = _download_premarket(session)
    if pm_err:
        raise HTTPException(503, f"Pre-Market Error: {pm_err}")

    expiry_code, expiry_iso = _nearest_expiry_code()
    gap_up, gap_down, excluded, no_interval, low_liq = [], [], [], [], []
    date_str, prev_date = _prev_trading_day()

    for sym, pm in (premarket or {}).items():
        pct = pm["PCT"]
        iep = pm["IEP"]
        bh  = (bhav or {}).get(sym)
        if not bh:
            continue

        interval = STRIKE_INTERVALS.get(sym)
        if not interval:
            no_interval.append({"symbol": sym, "gap_pct": round(pct, 2), "iep": iep})
            continue

        prev_high  = bh["HIGH"]
        prev_low   = bh["LOW"]
        prev_close = bh.get("CLOSE", 0)

        if pct >= min_gap_pct and iep > prev_high:
            atm   = _atm_strike(iep, interval)
            tv    = f"NSE:{sym}{expiry_code}C{int(atm)}"
            sdata = _get_option_data(sym, atm, "CALL", expiry_iso) if fetch_ltp else None
            if not sdata:
                low_liq.append({"symbol": sym, "gap_pct": round(pct, 2)})
            else:
                time.sleep(0.35)
            gap_up.append({
                "symbol": sym, "direction": "CALL", "iep": iep,
                "gap_pct": round(pct, 2), "prev_high": prev_high,
                "prev_low": None, "prev_close": prev_close,
                "strike_interval": interval,
                "atm_strike":      sdata["strike"]         if sdata else atm,
                "tv_symbol":       f"NSE:{sym}{expiry_code}C{int(sdata['strike'])}" if sdata else tv,
                "ce_ltp":          sdata["ce_ltp"]         if sdata else 0,
                "pe_ltp":          sdata["pe_ltp"]         if sdata else 0,
                "ltp":             sdata["ltp"]            if sdata else 0,
                "bep":             sdata["bep"]            if sdata else 0,
                "oi":              sdata["oi"]             if sdata else 0,
                "volume":          sdata["volume"]         if sdata else 0,
                "iv_pct":          sdata["iv"]             if sdata else 0,
                "liquidity":       sdata["liquidity"]      if sdata else 0,
                "lot_size":        sdata["lot_size"]       if sdata else LOT_SIZES.get(sym, DEFAULT_LOT),
                "cost_per_lot":    sdata["cost_per_lot"]   if sdata else 0,
                "lots_affordable": sdata["lots_affordable"]if sdata else 0,
                "max_loss_rs":     sdata["max_loss_rs"]    if sdata else 0,
                "target_2x_rs":    sdata["target_2x_rs"]  if sdata else 0,
                "sl_price":        sdata["sl_price"]       if sdata else 0,
                "flag":            sdata["flag"]           if sdata else "NO_DATA",
                "gap_tier":        "POWER" if pct>=5 else "STRONG" if pct>=3 else "NORMAL",
                "entry_condition": iep > prev_high,
            })

        elif pct <= -min_gap_pct and iep < prev_low:
            atm   = _atm_strike(iep, interval)
            tv    = f"NSE:{sym}{expiry_code}P{int(atm)}"
            sdata = _get_option_data(sym, atm, "PUT", expiry_iso) if fetch_ltp else None
            if not sdata:
                low_liq.append({"symbol": sym, "gap_pct": round(pct, 2)})
            else:
                time.sleep(0.35)
            gap_down.append({
                "symbol": sym, "direction": "PUT", "iep": iep,
                "gap_pct": round(pct, 2), "prev_high": None,
                "prev_low": prev_low, "prev_close": prev_close,
                "strike_interval": interval,
                "atm_strike":      sdata["strike"]         if sdata else atm,
                "tv_symbol":       f"NSE:{sym}{expiry_code}P{int(sdata['strike'])}" if sdata else tv,
                "ce_ltp":          sdata["ce_ltp"]         if sdata else 0,
                "pe_ltp":          sdata["pe_ltp"]         if sdata else 0,
                "ltp":             sdata["ltp"]            if sdata else 0,
                "bep":             sdata["bep"]            if sdata else 0,
                "oi":              sdata["oi"]             if sdata else 0,
                "volume":          sdata["volume"]         if sdata else 0,
                "iv_pct":          sdata["iv"]             if sdata else 0,
                "liquidity":       sdata["liquidity"]      if sdata else 0,
                "lot_size":        sdata["lot_size"]       if sdata else LOT_SIZES.get(sym, DEFAULT_LOT),
                "cost_per_lot":    sdata["cost_per_lot"]   if sdata else 0,
                "lots_affordable": sdata["lots_affordable"]if sdata else 0,
                "max_loss_rs":     sdata["max_loss_rs"]    if sdata else 0,
                "target_2x_rs":    sdata["target_2x_rs"]  if sdata else 0,
                "sl_price":        sdata["sl_price"]       if sdata else 0,
                "flag":            sdata["flag"]           if sdata else "NO_DATA",
                "gap_tier":        "POWER" if abs(pct)>=5 else "STRONG" if abs(pct)>=3 else "NORMAL",
                "entry_condition": iep < prev_low,
            })

        elif abs(pct) >= min_gap_pct:
            excluded.append({
                "symbol": sym, "gap_pct": round(pct, 2), "iep": iep,
                "prev_high": prev_high, "prev_low": prev_low,
                "reason": "IEP did not break prev high/low",
            })

    gap_up.sort(key=lambda x: (-x["gap_pct"], -x["liquidity"]))
    gap_down.sort(key=lambda x: (x["gap_pct"], -x["liquidity"]))

    watchlist_txt = "# GAP UP CALLS\n"
    for r in gap_up:
        tag = "" if r["flag"] == "OK" else "  # verify liquidity"
        watchlist_txt += f"{r['tv_symbol']},{tag}\n"
    watchlist_txt += "\n# GAP DOWN PUTS\n"
    for r in gap_down:
        tag = "" if r["flag"] == "OK" else "  # verify liquidity"
        watchlist_txt += f"{r['tv_symbol']},{tag}\n"

    result = {
        "scan_time":        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source":          "AUTO (NSE direct download)",
        "bhav_date":       prev_date.strftime("%Y-%m-%d"),
        "bhav_symbols":    len(bhav or {}),
        "premarket_symbols": len(premarket or {}),
        "expiry_code":     expiry_code,
        "expiry_iso":      expiry_iso,
        "total_gap_up":    len(gap_up),
        "total_gap_down":  len(gap_down),
        "top_picks":       [r for r in (gap_up + gap_down) if r["flag"] == "OK" and r["lots_affordable"] >= 1],
        "gap_up":          gap_up,
        "gap_down":        gap_down,
        "excluded":        excluded[:20],
        "no_interval":     no_interval[:20],
        "low_liquidity":   low_liq[:20],
        "watchlist_txt":   watchlist_txt,
        "capital":         capital,
        "risk_pct":        risk_pct,
    }

    _last_scan_result = result
    try:
        with open("last_gap_scan.json", "w") as f:
            json.dump(result, f)
    except Exception:
        pass
    return result
