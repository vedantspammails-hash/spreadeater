#!/usr/bin/env python3
# bot.py - Full production-ready Binance + KuCoin Futures Arbitrage Bot
# FIXED: Better error handling for IP whitelist test (no more JSON parse crashes)

import time, hmac, hashlib, json, requests, base64, math, sys
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os

# -------------------- CONFIG --------------------
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET")
KUCOIN_API_KEY = os.environ.get("KUCOIN_API_KEY", "YOUR_KUCOIN_API_KEY")
KUCOIN_API_SECRET = os.environ.get("KUCOIN_API_SECRET", "YOUR_KUCOIN_API_SECRET")
KUCOIN_API_PASSPHRASE = os.environ.get("KUCOIN_API_PASSPHRASE", "YOUR_KUCOIN_API_PASSPHRASE")
SCAN_THRESHOLD = 0.25
TRADE_TRIGGER = 1.5          # Entry when spread >= ±1.5%
PROFIT_TARGET = 0.60         # Exit when we capture 0.60% profit
TRADE_SIZE = 80.0            # $80 notional per leg
LEVERAGE = 3.0
FEE_BINANCE = 0.0004
FEE_KUCOIN = 0.0006
POLL_INTERVAL = 0.8
MAX_WORKERS = 8
ORDER_POLL_INTERVAL = 0.2
ORDER_POLL_TIMEOUT = 10.0

# -------------------- ENDPOINTS --------------------
BINANCE_BASE = "https://fapi.binance.com"
KUCOIN_BASE = "https://api-futures.kucoin.com"
BINANCE_INFO_URL = f"{BINANCE_BASE}/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = f"{BINANCE_BASE}/fapi/v1/ticker/bookTicker"
BINANCE_FUNDING_URL = f"{BINANCE_BASE}/fapi/v1/fundingRate?symbol={{symbol}}&limit=100"

KUCOIN_ACTIVE_URL = f"{KUCOIN_BASE}/api/v1/contracts/active"
KUCOIN_TICKER_URL = f"{KUCOIN_BASE}/api/v1/ticker?symbol={{symbol}}"
KUCOIN_FUNDING_URL = f"{KUCOIN_BASE}/api/v1/funding-rate/history?symbol={{symbol}}&pageSize=50"

# -------------------- SESSIONS --------------------
session = requests.Session()
session.headers.update({"User-Agent": "LiveArbBot/1.0"})

# -------------------- UTIL --------------------
def now_ts():
    return int(time.time())

def ts_str(ts=None):
    return datetime.fromtimestamp(ts or time.time()).strftime("%Y-%m-%d %H:%M:%S")

def floor_to_precision(qty, precision):
    if precision < 0: precision = 0
    factor = 10 ** precision
    return math.floor(qty * factor) / factor

def calc_slippage(expected, filled):
    if expected == 0: return 0.0
    return (filled - expected) / expected * 100.0

# ==================== IP WHITELIST GATE (RAILWAY SAFE - FIXED JSON HANDLING) ====================
def get_public_ip():
    for url in ["https://api.ipify.org", "https://ifconfig.me", "https://icanhazip.com"]:
        try:
            ip = session.get(url, timeout=6).text.strip()
            if ip and len(ip.split('.')) == 4:
                return ip
        except:
            continue
    return None

def binance_test_authenticated():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET or 'YOUR' in BINANCE_API_KEY:
        return False, "ERROR: BINANCE_API_KEY or BINANCE_API_SECRET missing/invalid in Railway env vars. Set them now!"
    
    params = {'timestamp': int(time.time() * 1000), 'recvWindow': 5000}
    query = urlencode(params)
    signature = hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    params['signature'] = signature
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    try:
        r = session.get(f"{BINANCE_BASE}/fapi/v1/account", params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            # Valid JSON means success
            r.json()
            return True, "IP WHITELISTED & API KEY VALID"
        else:
            # Handle non-JSON errors (e.g., 401 text response)
            try:
                data = r.json()
                code, msg = data.get("code"), data.get("msg", "")
                if code == -2015:
                    return False, f"Invalid API key/secret (code -2015): {msg}. Recreate key with Futures permission."
                elif code == -1021:
                    return False, f"IP NOT WHITELISTED (code -1021): {msg}. Wait 15min or re-add IP {get_public_ip()}."
                else:
                    return False, f"Binance error {code}: {msg}"
            except json.JSONDecodeError:
                # Raw text response (common for 401)
                return False, f"HTTP {r.status_code}: {r.text.strip()}. Likely IP/key issue - recreate key & wait 15min."
    except requests.exceptions.Timeout:
        return False, "Timeout connecting to Binance. Check Railway network."
    except requests.exceptions.RequestException as e:
        return False, f"Connection failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"

def wait_for_binance_ip_whitelist():
    print("\n" + "="*70)
    print("      BINANCE FUTURES API - IP WHITELIST REQUIRED")
    print("="*70)
    ip = get_public_ip()
    if not ip:
        print("FATAL: Could not detect your public IP. Check internet.")
        sys.exit(1)
    print(f"Your server IP → \033[1;33m{ip}\033[0m")
    print(f"→ Go to: https://www.binance.com/en/my/settings/api-management")
    print(f"→ Edit/Create API key → Add this IP: \033[1;33m{ip}\033[0m")
    print(f"→ Enable 'Futures' trading permission")
    print("Bot will wait until you whitelist this IP...\n")

    debug_once = True  # Print raw response only once to avoid spam
    while True:
        ok, msg = binance_test_authenticated()
        if ok:
            print(f"\033[1;32m✓ {msg}\033[0m")
            print("Starting arbitrage bot...\n")
            time.sleep(2)
            return
        else:
            if debug_once:
                # Quick test public endpoint to confirm connectivity
                try:
                    public_r = session.get(BINANCE_INFO_URL, timeout=5)
                    print(f"[DEBUG] Public API works (status {public_r.status_code}). Issue is auth-only.")
                except:
                    print("[DEBUG] Even public API failing - check Railway internet/proxy.")
                debug_once = False
            print(f"[{ts_str()}] Waiting for whitelist... {msg}")
        time.sleep(3)

# BLOCK UNTIL IP IS WHITELISTED
wait_for_binance_ip_whitelist()

# -------------------- BINANCE HELPERS --------------------
def binance_sign(params):
    query = urlencode(params)
    sig = hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    return sig

def binance_request(method, endpoint, params=None):
    params = params or {}
    params['timestamp'] = int(time.time() * 1000)
    params['recvWindow'] = 5000
    sig = binance_sign(params)
    params['signature'] = sig
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    url = BINANCE_BASE + endpoint
    r = session.request(method, url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def binance_place_market(symbol, side, quantity):
    params = {"symbol": symbol, "side": side, "type": "MARKET", "quantity": str(quantity)}
    return binance_request("POST", "/fapi/v1/order", params)

def binance_get_order(symbol, orderId):
    params = {"symbol": symbol, "orderId": orderId}
    return binance_request("GET", "/fapi/v1/order", params)

def binance_get_position_amt(symbol):
    r = binance_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
    if isinstance(r, list):
        for p in r:
            if p.get("symbol") == symbol:
                return float(p.get("positionAmt", 0))
    return 0.0

def fetch_binance_funding_events(symbol):
    try:
        r = session.get(BINANCE_FUNDING_URL.format(symbol=symbol), timeout=8)
        data = safe_json(r)  # Use safe_json to avoid crashes
        events = [(int(d["fundingTime"])/1000.0, float(d["fundingRate"])) for d in data]
        return events
    except:
        return []

def get_binance_symbol_precision_map():
    out = {}
    try:
        r = session.get(BINANCE_INFO_URL, timeout=10)
        data = safe_json(r)
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL":
                out[s["symbol"]] = int(s.get("quantityPrecision", 8))
    except:
        pass
    return out

def safe_json(r):
    try:
        return r.json() if r.text.strip() else {}
    except:
        return {}

# -------------------- KUCOIN HELPERS --------------------
def kucoin_sign(method, endpoint, body=None):
    now = str(int(time.time()*1000))
    body_str = json.dumps(body) if body else ""
    str_to_sign = now + method.upper() + endpoint + body_str
    signature = base64.b64encode(hmac.new(KUCOIN_API_SECRET.encode(), str_to_sign.encode(), hashlib.sha256).digest()).decode()
    passphrase = base64.b64encode(hmac.new(KUCOIN_API_SECRET.encode(), KUCOIN_API_PASSPHRASE.encode(), hashlib.sha256).digest()).decode()
    return now, signature, passphrase

def kucoin_request(method, endpoint, body=None, params=None):
    now, sig, passph = kucoin_sign(method, endpoint, body)
    headers = {
        "KC-API-KEY": KUCOIN_API_KEY,
        "KC-API-SIGN": sig,
        "KC-API-TIMESTAMP": now,
        "KC-API-PASSPHRASE": passph,
        "KC-API-KEY-VERSION": "2",
        "Content-Type": "application/json"
    }
    url = KUCOIN_BASE + endpoint
    r = session.post(url, headers=headers, json=body, params=params, timeout=10) if method == "POST" else session.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def kucoin_place_market(symbol, side, size):
    body = {
        "clientOid": str(int(time.time()*1000000)),
        "symbol": symbol,
        "side": side.lower(),
        "type": "market",
        "size": str(size)
    }
    return kucoin_request("POST", "/api/v1/orders", body=body)

def kucoin_get_order(order_id):
    return kucoin_request("GET", f"/api/v1/orders/{order_id}")

def kucoin_get_position_qty(symbol):
    try:
        now, sig, passph = kucoin_sign("GET", "/api/v1/position/single")
        headers = {
            "KC-API-KEY": KUCOIN_API_KEY, "KC-API-SIGN": sig, "KC-API-TIMESTAMP": now,
            "KC-API-PASSPHRASE": passph, "KC-API-KEY-VERSION": "2", "Content-Type": "application/json"
        }
        r = session.get(KUCOIN_BASE + f"/api/v1/position/single?symbol={symbol}", headers=headers, timeout=8)
        return float(safe_json(r).get("data", {}).get("currentQty", 0))
    except:
        return 0.0

def fetch_kucoin_funding_events(symbol):
    try:
        r = session.get(KUCOIN_FUNDING_URL.format(symbol=symbol + "M"), timeout=8)
        data = safe_json(r).get("data", [])
        events = []
        for d in data:
            ft = float(d.get("fundingTime") or d.get("time") or 0) / 1000.0
            fr = float(d.get("fundingRate", 0))
            events.append((ft, fr))
        return events
    except:
        return []

def get_kucoin_contract_precisions():
    out = {}
    try:
        r = session.get(KUCOIN_ACTIVE_URL, timeout=10)
        data = safe_json(r)
        for c in data.get("data", []):
            sym = c["symbol"]
            if sym.endswith("M"):
                prec = c.get("baseIncrementPrecision", 3)
                out[sym] = int(prec) if prec else 3
    except:
        pass
    return out

# -------------------- MARKET DATA --------------------
def get_common_symbols():
    try:
        r_bin = session.get(BINANCE_INFO_URL, timeout=10)
        bin_data = safe_json(r_bin)
        bin_symbols = [s["symbol"] for s in bin_data.get("symbols", []) 
                      if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
        r_ku = session.get(KUCOIN_ACTIVE_URL, timeout=10)
        ku_data = safe_json(r_ku)
        ku_symbols = [c["symbol"] for c in ku_data.get("data", [])]
        ku_map = {s[:-1]: s for s in ku_symbols if s.endswith("M")}
        common = [s for s in bin_symbols if s in ku_map]
        return common, ku_map
    except Exception as e:
        print("get_common_symbols error:", e)
        return [], {}

def get_prices(symbols, ku_map):
    bin_book = {}
    try:
        r = session.get(BINANCE_BOOK_URL, timeout=10)
        data = safe_json(r)
        for d in data:
            bin_book[d["symbol"]] = {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
    except: pass

    ku_prices = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(symbols))) as ex:
        futures = {ex.submit(session.get, KUCOIN_TICKER_URL.format(symbol=ku_map[s]), timeout=5): s for s in symbols}
        for f in as_completed(futures):
            try:
                r = f.result()
                j = safe_json(r).get("data", {})
                sym = ku_map[futures[f]]
                ku_prices[sym] = {"bid": float(j.get("bestBidPrice") or 0), "ask": float(j.get("bestAskPrice") or 0)}
            except: pass
    return bin_book, ku_prices

def calc_diff(bin_bid, bin_ask, ku_bid, ku_ask):
    if not all([bin_bid, bin_ask, ku_bid, ku_ask]): return None
    pos = (ku_bid - bin_ask) / bin_ask * 100.0
    neg = (ku_ask - bin_bid) / bin_bid * 100.0
    if pos > TRADE_TRIGGER: return pos
    if neg < -TRADE_TRIGGER: return neg
    return None

# -------------------- ORDER UTILS --------------------
def wait_order_binance(symbol, orderId, timeout=ORDER_POLL_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            res = binance_get_order(symbol, orderId)
            if res.get("status") in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                return res
        except: pass
        time.sleep(ORDER_POLL_INTERVAL)
    return None

def wait_order_kucoin(orderId, timeout=ORDER_POLL_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            res = kucoin_get_order(orderId)
            if res.get("data", {}).get("status") in ("done", "canceled", "failed"):
                return res
        except: pass
        time.sleep(ORDER_POLL_INTERVAL)
    return None

def extract_fill_price_binance(order):
    fills = order.get("fills", [])
    if fills:
        num = den = 0
        for f in fills:
            num += float(f["price"]) * float(f["qty"])
            den += float(f["qty"])
        return num / den if den else 0.0
    return float(order.get("avgPrice", 0) or 0)

def extract_fill_price_kucoin(resp):
    data = resp.get("data", {})
    if data.get("filledAvgPrice"):
        return float(data["filledAvgPrice"])
    qty = float(data.get("filledSize") or 0)
    funds = float(data.get("dealFunds") or 0)
    return funds / qty if qty else 0.0

# -------------------- FUNDING --------------------
def compute_funding_impact(open_ts, close_ts, long_ex, short_ex, symbol):
    total = 0.0
    events = []
    try:
        if long_ex == "Binance":
            for ft, fr in fetch_binance_funding_events(symbol):
                if open_ts < ft <= close_ts:
                    total -= fr * TRADE_SIZE
                    events.append(("Binance_long", ft, fr, -fr * TRADE_SIZE))
        if short_ex == "Binance":
            for ft, fr in fetch_binance_funding_events(symbol):
                if open_ts < ft <= close_ts:
                    total += fr * TRADE_SIZE
                    events.append(("Binance_short", ft, fr, fr * TRADE_SIZE))
        if long_ex == "KuCoin":
            for ft, fr in fetch_kucoin_funding_events(symbol):
                if open_ts < ft <= close_ts:
                    total -= fr * TRADE_SIZE
                    events.append(("KuCoin_long", ft, fr, -fr * TRADE_SIZE))
        if short_ex == "KuCoin":
            for ft, fr in fetch_kucoin_funding_events(symbol):
                if open_ts < ft <= close_ts:
                    total += fr * TRADE_SIZE
                    events.append(("KuCoin_short", ft, fr, fr * TRADE_SIZE))
    except: pass
    return total, events

# -------------------- POSITION CLASS --------------------
class Position:
    def __init__(self, symbol, long_ex, short_ex, long_price, short_price, direction, opened_diff, bin_prec_map, ku_prec_map, ku_map):
        self.symbol = symbol
        self.short_symbol_ku = ku_map.get(symbol)
        self.long_ex = long_ex
        self.short_ex = short_ex
        self.long_price = float(long_price)
        self.short_price = float(short_price)
        self.direction = direction
        self.opened_diff = float(opened_diff)
        self.open_time = time.time()
        self.closed = False
        self.slippage_long_pct = 0.0
        self.slippage_short_pct = 0.0
        self.fill_price_long = 0.0
        self.fill_price_short = 0.0

        # -------------------- EXACT NOTIONAL MATCH LOGIC --------------------
        bin_prec = bin_prec_map.get(symbol, 6)
        ku_prec = ku_prec_map.get(self.short_symbol_ku, 3)
        qty_bin0 = floor_to_precision(TRADE_SIZE / self.long_price, bin_prec)
        qty_ku0 = floor_to_precision(TRADE_SIZE / self.short_price, ku_prec)
        not_bin = qty_bin0 * self.long_price
        not_ku = qty_ku0 * self.short_price
        qty_bin = qty_bin0
        qty_ku = qty_ku0
        while abs(not_bin - not_ku) > 0.01 and qty_bin > 0 and qty_ku > 0:
            if not_bin > not_ku:
                qty_bin -= 10 ** -bin_prec
                qty_bin = floor_to_precision(qty_bin, bin_prec)
                not_bin = qty_bin * self.long_price
            else:
                qty_ku -= 10 ** -ku_prec
                qty_ku = floor_to_precision(qty_ku, ku_prec)
                not_ku = qty_ku * self.short_price
        final_notional = min(not_bin, not_ku)
        qty_bin = floor_to_precision(final_notional / self.long_price, bin_prec)
        qty_ku = floor_to_precision(final_notional / self.short_price, ku_prec)
        not_bin = qty_bin * self.long_price
        not_ku = qty_ku * self.short_price
        if qty_bin <= 0 or qty_ku <= 0 or abs(not_bin - not_ku) > 0.01:
            raise ValueError(f"Could not produce exactly matched notional for {symbol}: qty_bin={qty_bin}, not_bin={not_bin}, qty_ku={qty_ku}, not_ku={not_ku}")
        self.qty_long = qty_bin if long_ex == "Binance" else qty_ku
        self.qty_short = qty_ku if short_ex == "KuCoin" else qty_bin
        print(f"[{ts_str()}] OPEN {symbol} dir={direction} opened_diff={opened_diff:.4f}% long_ex={long_ex} short_ex={short_ex}")
        print(f" target notional ${TRADE_SIZE:.2f}; final notional long={self.qty_long*self.long_price:.6f}, short={self.qty_short*self.short_price:.6f}")
        try:
            if direction == "+ve":
                order_bin = binance_place_market(self.symbol, "BUY", self.qty_long)
                order_bin_filled = wait_order_binance(self.symbol, order_bin.get("orderId"))
                self.fill_price_long = extract_fill_price_binance(order_bin_filled or order_bin)
                order_ku = kucoin_place_market(self.short_symbol_ku, "sell", self.qty_short)
                order_ku_id = (order_ku.get("data") or {}).get("orderId")
                order_ku_filled = wait_order_kucoin(order_ku_id) if order_ku_id else None
                self.fill_price_short = extract_fill_price_kucoin(order_ku_filled or order_ku)
            else:
                order_ku = kucoin_place_market(self.short_symbol_ku, "buy", self.qty_long)
                order_ku_id = (order_ku.get("data") or {}).get("orderId")
                order_ku_filled = wait_order_kucoin(order_ku_id) if order_ku_id else None
                self.fill_price_long = extract_fill_price_kucoin(order_ku_filled or order_ku)
                order_bin = binance_place_market(self.symbol, "SELL", self.qty_short)
                order_bin_filled = wait_order_binance(self.symbol, order_bin.get("orderId"))
                self.fill_price_short = extract_fill_price_binance(order_bin_filled or order_bin)
        except Exception as e:
            print("[ERROR] order placement:", e)
            raise
        self.slippage_long_pct = calc_slippage(self.long_price, self.fill_price_long) * 100.0 if self.fill_price_long else 0.0
        self.slippage_short_pct = calc_slippage(self.short_price, self.fill_price_short) * 100.0 if self.fill_price_short else 0.0
        print(f"[{ts_str()}] Filled long at {self.fill_price_long:.8f} (expected {self.long_price:.8f}), slippage {self.slippage_long_pct:.6f}%")
        print(f"[{ts_str()}] Filled short at {self.fill_price_short:.8f} (expected {self.short_price:.8f}), slippage {self.slippage_short_pct:.6f}%")

    def check_liquidation(self):
        try:
            b_amt = binance_get_position_amt(self.symbol)
        except:
            b_amt = 0.0
        try:
            k_amt = kucoin_get_position_qty(self.short_symbol_ku)
        except:
            k_amt = 0.0
        return abs(b_amt) < 1e-6 or abs(k_amt) < 1e-6

    def close(self, close_price_long, close_price_short, close_diff):
        if self.closed:
            return None
        self.close_time = time.time()
        spread_gain = (self.opened_diff - close_diff) if self.direction == "+ve" else - (self.opened_diff + close_diff)
        exposure = TRADE_SIZE
        gross_pnl = spread_gain / 100.0 * exposure * LEVERAGE
        fee_long_rate = FEE_BINANCE if self.long_ex == "Binance" else FEE_KUCOIN
        fee_short_rate = FEE_BINANCE if self.short_ex == "Binance" else FEE_KUCOIN
        fee_long_total = 2 * exposure * fee_long_rate
        fee_short_total = 2 * exposure * fee_short_rate
        total_fees = fee_long_total + fee_short_total
        funding_total, funding_events = compute_funding_impact(self.open_time, self.close_time, self.long_ex, self.short_ex, self.symbol)
        net_pnl = gross_pnl - total_fees + funding_total
        self.closed = True
        print("\n=== TRADE CLOSED ===")
        print(f"Symbol: {self.symbol}")
        print(f"Open time: {ts_str(self.open_time)}, Close time: {ts_str(self.close_time)}")
        print(f"Direction: {'Long Binance / Short KuCoin' if self.direction=='+ve' else 'Long KuCoin / Short Binance'}")
        print(f"Opened spread: {self.opened_diff:.6f}%, Close spread: {close_diff:.6f}% => Spread captured: {spread_gain:.6f}%")
        print(f"Exposure (per leg): ${exposure:.2f}, Leverage: {LEVERAGE}x => Gross PnL: {gross_pnl:.8f}")
        print(f"Trading fees (both legs open+close): {total_fees:.8f} (long total {fee_long_total:.8f}, short total {fee_short_total:.8f})")
        if funding_events:
            print("Applied funding events:")
            for ev in funding_events:
                sname, ft, fr, impact = ev
                print(f" - {sname} @ {ts_str(ft)} rate={fr:+.8f} impact=${impact:+.8f}")
        else:
            print("Applied funding events: None")
        print(f"Funding net: {funding_total:+.8f}")
        print(f"Slippage long %: {self.slippage_long_pct:.8f}, slippage short %: {self.slippage_short_pct:.8f}")
        print(f"Net PnL: {net_pnl:+.8f}")
        print("====================\n")
        try:
            if self.direction == "+ve":
                binance_place_market(self.symbol, "SELL", self.qty_long)
                kucoin_place_market(self.short_symbol_ku, "buy", self.qty_short)
            else:
                kucoin_place_market(self.short_symbol_ku, "sell", self.qty_long)
                binance_place_market(self.symbol, "BUY", self.qty_short)
        except Exception:
            pass
        return net_pnl

# -------------------- HEALTH CHECK --------------------
def startup_health_check():
    print("\n[Startup] Running connectivity checks...")
    try:
        r = session.get(BINANCE_INFO_URL, timeout=10)
        if r.status_code == 200:
            print("Binance public API: OK")
        else:
            print(f"Binance public API: FAIL (status {r.status_code})")
            return False
        r = session.get(KUCOIN_ACTIVE_URL, timeout=10)
        if r.status_code == 200:
            print("KuCoin public API: OK")
        else:
            print(f"KuCoin public API: FAIL (status {r.status_code})")
            return False
        print("All checks passed!\n")
        return True
    except Exception as e:
        print("Health check failed:", e)
        return False

# -------------------- MAIN --------------------
def main():
    print("Starting live arbitrage bot (production-ready) -", ts_str())
    # Health check
    if not startup_health_check():
        print("[Fatal] Startup health check failed. Please check your API credentials, exchange access, and server network, then restart.")
        sys.exit(1)
    symbols, ku_map = get_common_symbols()
    if not symbols:
        print("No common symbols found. Exiting.")
        return
    print(f"Found {len(symbols)} common symbols")
    bin_prec_map = get_binance_symbol_precision_map()
    ku_prec_map = get_kucoin_contract_precisions()
    open_pos = None
    try:
        while True:
            try:
                bin_book, ku_prices = get_prices(symbols, ku_map)
                if open_pos:
                    b = bin_book.get(open_pos.symbol)
                    k = ku_prices.get(open_pos.short_symbol_ku)
                    if not b or not k:
                        time.sleep(POLL_INTERVAL)
                        continue
                    diff_entry = calc_diff(b["bid"], b["ask"], k["bid"], k["ask"])
                    close_diff = ((k["bid"] - b["ask"]) / b["ask"] * 100.0) if open_pos.direction == "+ve" else ((k["ask"] - b["bid"]) / b["bid"] * 100.0)
                    exit_zero = (open_pos.direction == "+ve" and (diff_entry is None or diff_entry <= 0)) or (open_pos.direction == "-ve" and (diff_entry is None or diff_entry >= 0))
                    profit_hit = False
                    if open_pos.direction == "+ve" and diff_entry is not None and diff_entry <= (open_pos.opened_diff - PROFIT_TARGET):
                        profit_hit = True
                    if open_pos.direction == "-ve" and diff_entry is not None and diff_entry >= (open_pos.opened_diff + PROFIT_TARGET):
                        profit_hit = True
                    try:
                        liq_flag = open_pos.check_liquidation()
                    except:
                        liq_flag = False
                    if exit_zero or profit_hit or liq_flag:
                        print(f"[{ts_str()}] Closing position: exit_zero={exit_zero}, profit_hit={profit_hit}, liquidation={liq_flag}")
                        open_pos.close(b["bid"], k["ask"], close_diff)
                        open_pos = None
                        time.sleep(1.0)
                        continue
                    time.sleep(POLL_INTERVAL)
                    continue
                best_sym = None
                best_diff = 0.0
                best_pair = None
                for s in symbols:
                    b = bin_book.get(s)
                    k = ku_prices.get(ku_map[s])
                    if not b or not k:
                        continue
                    diff = calc_diff(b["bid"], b["ask"], k["bid"], k["ask"])
                    if diff is None:
                        continue
                    if abs(diff) > abs(best_diff):
                        best_diff = diff
                        best_sym = s
                        best_pair = (b, k)
                if best_sym and abs(best_diff) >= TRADE_TRIGGER:
                    b, k = best_pair
                    if best_diff > 0:
                        long_ex = "Binance"
                        short_ex = "KuCoin"
                        long_price = b["ask"]
                        short_price = k["bid"]
                    else:
                        long_ex = "KuCoin"
                        short_ex = "Binance"
                        long_price = k["ask"]
                        short_price = b["bid"]
                    try:
                        open_pos = Position(best_sym, long_ex, short_ex, long_price, short_price, "+ve" if best_diff > 0 else "-ve", best_diff, bin_prec_map, ku_prec_map, ku_map)
                    except Exception as e:
                        print("[ERROR] could not open position:", e)
                        open_pos = None
                        time.sleep(1.0)
                        continue
                else:
                    time.sleep(POLL_INTERVAL)
            except Exception as e:
                print("[MAIN LOOP ERROR]", e)
                time.sleep(2.0)
    except KeyboardInterrupt:
        print("Stopping by user")

if __name__ == "__main__":
    main()
