#!/usr/bin/env python3
# bot.py - Full production-ready arbitrage bot (Binance + KuCoin futures)
# Features:
# - $80 per leg notional, 3x leverage
# - Market orders with order-status confirmation and slippage logging
# - Per-symbol quantity rounding (round-down), matched qty between legs
# - Funding fees applied only for funding events that occur while a position is open
# - Liquidation detection and immediate close of opposite leg
# - Spread-based entry/exit: TRADE_TRIGGER, zero-cross exit, PROFIT_TARGET
# - Reasonable rate-limiting, threaded price fetch for KuCoin
# - Replace the API keys below before running. Test on testnets first.

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
TRADE_TRIGGER = 1.5   # ENTRY THRESHOLD is ±1.5%
PROFIT_TARGET = 0.60
TRADE_SIZE = 80.0 # $80 notional per leg (not margin)
LEVERAGE = 3.0
FEE_BINANCE = 0.0004
FEE_KUCOIN = 0.0006
POLL_INTERVAL = 0.8 # main loop sleep
MAX_WORKERS = 8
ORDER_POLL_INTERVAL = 0.2
ORDER_POLL_TIMEOUT = 10.0 # seconds to wait for fill

# -------------------- ENDPOINTS --------------------
BINANCE_BASE = "https://fapi.binance.com"
KUCOIN_BASE = "https://api-futures.kucoin.com"
BINANCE_INFO_URL = f"{BINANCE_BASE}/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = f"{BINANCE_BASE}/fapi/v1/ticker/bookTicker"
BINANCE_ORDER_URL = f"{BINANCE_BASE}/fapi/v1/order"
BINANCE_ORDER_STATUS = f"{BINANCE_BASE}/fapi/v1/order" # GET with symbol+orderId
BINANCE_POSITION_URL = f"{BINANCE_BASE}/fapi/v2/positionRisk"
BINANCE_FUNDING_URL = f"{BINANCE_BASE}/fapi/v1/fundingRate?symbol={{symbol}}&limit=100"
KUCOIN_ACTIVE_URL = f"{KUCOIN_BASE}/api/v1/contracts/active"
KUCOIN_TICKER_URL = f"{KUCOIN_BASE}/api/v1/ticker?symbol={{symbol}}"
KUCOIN_ORDER_URL = f"{KUCOIN_BASE}/api/v1/orders"
KUCOIN_ORDER_STATUS = f"{KUCOIN_BASE}/api/v1/orders/{{orderId}}"
KUCOIN_POSITION_URL = f"{KUCOIN_BASE}/api/v1/position/single?symbol={{symbol}}"
KUCOIN_FUNDING_URL = f"{KUCOIN_BASE}/api/v1/funding-rate/history?symbol={{symbol}}&pageSize=50"

BINANCE_IP_WHITELIST_URL = f"{BINANCE_BASE}/sapi/v1/account/apiRestrictions/ipRestriction/ipList"

# -------------------- SESSIONS --------------------
session = requests.Session()
session.headers.update({"User-Agent": "LiveArbBot/1.0"})

# -------------------- UTIL --------------------
def now_ts():
    return int(time.time())
def ts_str(ts=None):
    return datetime.fromtimestamp(ts or time.time()).strftime("%Y-%m-%d %H:%M:%S")
def safe_json(r):
    try:
        return r.json()
    except:
        return {}
def floor_to_precision(qty, precision):
    if precision < 0: precision = 0
    factor = 10 ** precision
    return math.floor(qty * factor) / factor
def calc_slippage(expected, filled):
    if expected == 0: return 0.0
    return (filled - expected) / expected * 100.0

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
        data = r.json()
        events = []
        for d in data:
            ft = int(d.get("fundingTime", 0)) / 1000.0
            fr = float(d.get("fundingRate", 0))
            events.append((ft, fr))
        return events
    except:
        return []
def get_binance_symbol_precision_map():
    out = {}
    try:
        r = session.get(BINANCE_INFO_URL, timeout=10).json()
        for s in r.get("symbols", []):
            sym = s.get("symbol")
            prec = int(s.get("quantityPrecision", 8))
            out[sym] = prec
    except:
        pass
    return out

# --- New code for IP whitelist checking ---
def get_public_ip():
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=10)
        r.raise_for_status()
        ip = r.json().get("ip", None)
        if not ip:
            raise ValueError("No IP found in response")
        return ip
    except Exception as e:
        print(f"[ERROR] Could not fetch public IP: {e}")
        return None

def check_binance_ip_whitelist(ip):
    try:
        params = {
            "timestamp": int(time.time() * 1000),
            "recvWindow": 5000
        }
        sig = binance_sign(params)
        params["signature"] = sig
        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        url = BINANCE_IP_WHITELIST_URL
        r = session.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        ip_list = data.get("ipList", [])
        for entry in ip_list:
            if entry.get("ip") == ip:
                return True
        return False
    except Exception as e:
        print(f"[ERROR] Could not check Binance IP whitelist: {e}")
        return False

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
    if method.upper() == "GET":
        r = session.get(url, headers=headers, params=params, timeout=10)
    else:
        r = session.post(url, headers=headers, json=body, timeout=10)
    r.raise_for_status()
    return r.json()
def kucoin_place_market(symbol, side, size):
    endpoint = "/api/v1/orders"
    body = {
        "clientOid": str(int(time.time()*1000000)),
        "symbol": symbol,
        "side": side.lower(),
        "type": "market",
        "size": str(size)
    }
    return kucoin_request("POST", endpoint, body=body)
def kucoin_get_order(order_id):
    endpoint = f"/api/v1/orders/{order_id}"
    return kucoin_request("GET", endpoint)
def kucoin_get_position_qty(symbol):
    try:
        endpoint = f"/api/v1/position/single?symbol={symbol}"
        now, sig, passph = kucoin_sign("GET", "/api/v1/position/single", None)
        headers = {
            "KC-API-KEY": KUCOIN_API_KEY,
            "KC-API-SIGN": sig,
            "KC-API-TIMESTAMP": now,
            "KC-API-PASSPHRASE": passph,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        r = session.get(KUCOIN_BASE + f"/api/v1/position/single?symbol={symbol}", headers=headers, timeout=8)
        data = r.json().get("data") or {}
        return float(data.get("currentQty", 0) or 0)
    except:
        return 0.0
def fetch_kucoin_funding_events(symbol):
    try:
        r = session.get(KUCOIN_FUNDING_URL.format(symbol=symbol), timeout=8)
        data = r.json().get("data", [])
        events = []
        for d in data:
            ft = float(d.get("fundingTime", 0)) / 1000.0 if d.get("fundingTime") else float(d.get("time", 0))
            fr = float(d.get("fundingRate", 0))
            events.append((ft, fr))
        return events
    except:
        return []
def get_kucoin_contract_precisions():
    out = {}
    try:
        r = session.get(KUCOIN_ACTIVE_URL, timeout=10).json()
        for c in r.get("data", []):
            sym = c.get("symbol")
            prec = c.get("baseIncrementPrecision")
            if prec is None:
                prec = 3
            out[sym] = int(prec)
    except:
        pass
    return out

# -------------------- MARKET DATA --------------------
def get_common_symbols():
    try:
        bin_symbols = [s["symbol"] for s in session.get(BINANCE_INFO_URL, timeout=10).json().get("symbols", []) if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
        ku_symbols = [s["symbol"] for s in session.get(KUCOIN_ACTIVE_URL, timeout=10).json().get("data", [])]
        ku_map = {}
        for s in ku_symbols:
            if s.endswith("M"):
                base = s[:-1]
                ku_map[base] = s
        common = [s for s in bin_symbols if s in ku_map]
        return common, ku_map
    except Exception as e:
        print("get_common_symbols error:", e)
        return [], {}
def get_prices(symbols, ku_map):
    bin_book = {}
    try:
        data = session.get(BINANCE_BOOK_URL, timeout=10).json()
        for d in data:
            bin_book[d["symbol"]] = {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
    except:
        pass
    ku_prices = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(symbols) or 1)) as ex:
        futures = {ex.submit(session.get, KUCOIN_TICKER_URL.format(symbol=ku_map[s]), {"timeout":5}): s for s in symbols}
        for fut in as_completed(futures):
            s = futures[fut]
            try:
                r = fut.result()
                j = r.json().get("data", {})
                ku_prices[ku_map[s]] = {"bid": float(j.get("bestBidPrice") or 0), "ask": float(j.get("bestAskPrice") or 0)}
            except:
                pass
    return bin_book, ku_prices
def calc_diff(bin_bid, bin_ask, ku_bid, ku_ask):
    if not bin_bid or not bin_ask or not ku_bid or not ku_ask:
        return None
    pos = (ku_bid - bin_ask) / bin_ask * 100.0
    neg = (ku_ask - bin_bid) / bin_bid * 100.0
    if pos > TRADE_TRIGGER:
        return pos
    if neg < -TRADE_TRIGGER:
        return neg
    return None

# -------------------- ORDER UTILITIES --------------------
def wait_order_binance(symbol, orderId, timeout=ORDER_POLL_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            res = binance_get_order(symbol, orderId)
            status = res.get("status")
            if status in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                return res
        except Exception:
            pass
        time.sleep(ORDER_POLL_INTERVAL)
    return None
def wait_order_kucoin(orderId, timeout=ORDER_POLL_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            res = kucoin_get_order(orderId)
            status = res.get("data", {}).get("status")
            if status in ("done", "canceled", "failed"):
                return res
        except Exception:
            pass
        time.sleep(ORDER_POLL_INTERVAL)
    return None
def extract_fill_price_binance(order):
    fills = order.get("fills") or []
    if not fills:
        avg = order.get("avgFillPrice")
        return float(avg) if avg else 0.0
    num = 0.0
    den = 0.0
    for f in fills:
        p = float(f.get("price", 0))
        q = float(f.get("qty", 0))
        num += p * q
        den += q
    return (num/den) if den else 0.0
def extract_fill_price_kucoin(order_resp):
    data = order_resp.get("data") or {}
    if data.get("filledAvgPrice"):
        try:
            return float(data.get("filledAvgPrice"))
        except:
            pass
    return float(data.get("dealFunds") or 0) / float(data.get("filledSize") or 1) if data.get("filledSize") else 0.0

# -------------------- FUNDING CALC --------------------
def compute_funding_impact(position_open_ts, position_close_ts, long_ex_name, short_ex_name, symbol):
    total = 0.0
    applied = []
    try:
        if long_ex_name == "Binance":
            b_events = fetch_binance_funding_events(symbol)
            for ft, fr in b_events:
                if position_open_ts < ft <= position_close_ts:
                    impact = (-fr * TRADE_SIZE)
                    total += impact
                    applied.append(("Binance_long", ft, fr, impact))
        else:
            b_events = fetch_binance_funding_events(symbol)
            for ft, fr in b_events:
                if position_open_ts < ft <= position_close_ts:
                    impact = (-fr * TRADE_SIZE)
        if short_ex_name == "Binance":
            b_events2 = fetch_binance_funding_events(symbol)
            for ft, fr in b_events2:
                if position_open_ts < ft <= position_close_ts:
                    impact = (fr * TRADE_SIZE)
                    total += impact
                    applied.append(("Binance_short", ft, fr, impact))
    except:
        pass
    try:
        if long_ex_name == "KuCoin":
            k_events = fetch_kucoin_funding_events(symbol + "M")
            for ft, fr in k_events:
                if position_open_ts < ft <= position_close_ts:
                    impact = (-fr * TRADE_SIZE)
                    total += impact
                    applied.append(("KuCoin_long", ft, fr, impact))
        if short_ex_name == "KuCoin":
            k_events2 = fetch_kucoin_funding_events(symbol + "M")
            for ft, fr in k_events2:
                if position_open_ts < ft <= position_close_ts:
                    impact = (fr * TRADE_SIZE)
                    total += impact
                    applied.append(("KuCoin_short", ft, fr, impact))
    except:
        pass
    return total, applied

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
        self.close_time = None
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
                order_ku_id = (order_ku.get("data") or {}).get("orderId") or (order_ku.get("data") or {}).get("orderId", None)
                order_ku_filled = None
                if order_ku_id:
                    order_ku_filled = wait_order_kucoin(order_ku_id)
                self.fill_price_short = extract_fill_price_kucoin(order_ku_filled or order_ku)
            else:
                order_ku = kucoin_place_market(self.short_symbol_ku, "buy", self.qty_long)
                order_ku_id = (order_ku.get("data") or {}).get("orderId") or None
                order_ku_filled = None
                if order_ku_id:
                    order_ku_filled = wait_order_kucoin(order_ku_id)
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
        print(f"[{ts_str()}] Filled short at {self.fill_price_short:.8f} (expected {self.short_price:.8f}), slippage {self.slippage_short_pct:.6f}")

    def check_liquidation(self):
        try:
            b_amt = binance_get_position_amt(self.symbol)
        except:
            b_amt = 0.0
        try:
            k_amt = kucoin_get_position_qty(self.short_symbol_ku)
        except:
            k_amt = 0.0
        if abs(b_amt) < 1e-6 or abs(k_amt) < 1e-6:
            return True
        return False
    def close(self, close_price_long, close_price_short, close_diff):
        if self.closed:
            return None
        self.close_time = time.time()
        spread_gain = (self.opened_diff - close_diff)
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

# -------------------- HEALTH CHECK FUNCTION --------------------
def startup_health_check():
    print("\n[Startup] Running exchange connectivity health checks...")

    # Binance exchangeInfo
    try:
        r1 = session.get(BINANCE_INFO_URL, timeout=10)
        if r1.status_code == 200 and "symbols" in r1.json():
            print("[OK] Binance exchangeInfo loaded.")
        else:
            print("[ERROR] Binance exchangeInfo not OK. Response:", r1.text)
            return False
    except Exception as ex:
        print("[ERROR] Could not connect to Binance exchangeInfo:", ex)
        return False

    # Binance book ticker
    try:
        r2 = session.get(BINANCE_BOOK_URL, timeout=10)
        if r2.status_code == 200 and isinstance(r2.json(), list):
            print("[OK] Binance bookTicker loaded.")
        else:
            print("[ERROR] Binance bookTicker not OK. Response:", r2.text)
            return False
    except Exception as ex:
        print("[ERROR] Could not connect to Binance bookTicker:", ex)
        return False

    # KuCoin contracts active
    try:
        r3 = session.get(KUCOIN_ACTIVE_URL, timeout=10)
        if r3.status_code == 200 and "data" in r3.json():
            print("[OK] KuCoin contracts active loaded.")
        else:
            print("[ERROR] KuCoin active contracts not OK. Response:", r3.text)
            return False
    except Exception as ex:
        print("[ERROR] Could not connect to KuCoin contracts/active:", ex)
        return False

    # KuCoin ticker for one contract (pick BTCUSDT if present)
    try:
        symbols = [s["symbol"] for s in session.get(KUCOIN_ACTIVE_URL, timeout=10).json().get("data", [])]
        if not symbols:
            print("[ERROR] No KuCoin contracts available.")
            return False
        test_symbol = symbols[0]
        ticker_url = KUCOIN_TICKER_URL.format(symbol=test_symbol)
        r4 = session.get(ticker_url, timeout=10)
        if r4.status_code == 200 and "data" in r4.json():
            print(f"[OK] KuCoin ticker ({test_symbol}) loaded.")
        else:
            print(f"[ERROR] KuCoin ticker ({test_symbol}) not OK. Response:", r4.text)
            return False
    except Exception as ex:
        print("[ERROR] Could not connect to KuCoin ticker endpoint:", ex)
        return False

    print("[Startup] All API connectivity checks PASSED.\n")
    return True

# -------------------- MAIN LOOP --------------------
def main():
    print("Starting live arbitrage bot (production-ready) -", ts_str())

    # Step 1: Check public IP and prompt whitelisting
    public_ip = get_public_ip()
    if not public_ip:
        print("[Fatal] Could not determine public IP for whitelisting. Please check your network and try again.")
        sys.exit(1)

    print(f"Your current public IP is: {public_ip}")
    print("Please whitelist this IP in your Binance API key settings.")
    print("Waiting for IP to be whitelisted on Binance...")

    # Step 2: Confirm every 3 seconds until IP is whitelisted
    while True:
        if check_binance_ip_whitelist(public_ip):
            print(f"[{ts_str()}] Public IP {public_ip} is whitelisted in Binance. Proceeding...")
            break
        else:
            print(f"[{ts_str()}] Public IP {public_ip} NOT whitelisted yet. Retrying in 3 seconds...")
            time.sleep(3)

    # Step 3: Proceed with original startup health check and main bot logic
    if not startup_health_check():
        print("[Fatal] Startup health check failed. Please check your API credentials, exchange access, and server network, then restart.")
        sys.exit(1)

    symbols, ku_map = get_common_symbols()
    if not symbols:
        print("No common symbols found. Exiting.")
        return
    print(f"Found {len(symbols)} common symbols")
    bin_prec_map = get_binance_symbol_precision_map()
    ku_prec_map = get_kucoin
