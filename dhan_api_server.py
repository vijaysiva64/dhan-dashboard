import os
import requests
import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ── PUT YOUR DHAN CREDENTIALS HERE ───────────────────────────
CLIENT_ID    = os.environ.get("CLIENT_ID",    "1110828392")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "YOUR_ACCESS_TOKEN_HERE")

BASE_URL = "https://api.dhan.co/v2"
HEADERS  = {
    "access-token": ACCESS_TOKEN,
    "client-id": CLIENT_ID,
    "Content-Type": "application/json",
}
SYMBOL_MAP = {
    "NIFTY":     {"scrip_code": 13,   "segment": "IDX_I"},
    "BANKNIFTY": {"scrip_code": 25,   "segment": "IDX_I"},
    "FINNIFTY":  {"scrip_code": 27,   "segment": "IDX_I"},
    "RELIANCE":  {"scrip_code": 1333, "segment": "NSE_EQ"},
    "SBIN":      {"scrip_code": 3045, "segment": "NSE_EQ"},
}

app = FastAPI(title="NSE Options API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        strike = float(strike_str)
        ce     = cp.get("ce", {}) or cp.get("CE", {})
        pe     = cp.get("pe", {}) or cp.get("PE", {})
        ceg    = ce.get("greeks", {}) or {}
        peg    = pe.get("greeks", {}) or {}
        rows.append({
            "strike":    strike,
            "ce_oi":     ce.get("oi", 0),
            "ce_ltp":    ce.get("last_price", 0),
            "ce_iv":     ce.get("implied_volatility", 0) or ce.get("iv", 0),
            "ce_volume": ce.get("volume", 0),
            "ce_bid":    ce.get("top_bid_price", 0),
            "ce_ask":    ce.get("top_ask_price", 0),
            "ce_delta":  ceg.get("delta", 0),
            "ce_theta":  ceg.get("theta", 0),
            "ce_gamma":  ceg.get("gamma", 0),
            "ce_vega":   ceg.get("vega", 0),
            "pe_oi":     pe.get("oi", 0),
            "pe_ltp":    pe.get("last_price", 0),
            "pe_iv":     pe.get("implied_volatility", 0) or pe.get("iv", 0),
            "pe_volume": pe.get("volume", 0),
            "pe_bid":    pe.get("top_bid_price", 0),
            "pe_ask":    pe.get("top_ask_price", 0),
            "pe_delta":  peg.get("delta", 0),
            "pe_theta":  peg.get("theta", 0),
            "pe_gamma":  peg.get("gamma", 0),
            "pe_vega":   peg.get("vega", 0),
        })
    rows.sort(key=lambda r: r["strike"])
    return spot, rows

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.datetime.now().isoformat()}

@app.get("/api/symbols")
def get_symbols():
    return {"symbols": list(SYMBOL_MAP.keys())}

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

@app.get("/api/chain/{symbol}/{expiry}")
def get_chain(symbol: str, expiry: str):
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