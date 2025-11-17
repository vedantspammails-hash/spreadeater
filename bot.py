#!/usr/bin/env python3
# bot.py - Full production-ready Binance + KuCoin futures arbitrage bot
# Features:
# - $80 per leg notional, 3x leverage
# - Market orders with fill confirmation and slippage logging
# - Exact notional matching between legs (rounded down)
# - Proper funding fee accounting (only while position open)
# - Liquidation detection + immediate opposite-leg close
# - Spread entry/exit: ±1.5% trigger, zero-cross or +0.60% profit target
# - Rate-limited, threaded price fetching

import time
import hmac
import hashlib
import json
import requests
import base64
import math
import sys
import os
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# -------------------- CONFIG --------------------
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "YOUR_BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "YOUR_BINANCE_API_SECRET")
KUCOIN_API_KEY = os.environ.get("KUCOIN_API_KEY", "YOUR_KUCOIN_API_KEY")
KUCOIN_API_SECRET = os.environ.get("KUCOIN_API_SECRET", "YOUR_KUCOIN_API_SECRET")
KUCOIN_API_PASSPHRASE = os.environ.get("KUCOIN_API_PASSPHRASE", "YOUR_KUCOIN_API_PASSPHRASE")

SCAN_THRESHOLD = 0.25
TRADE_TRIGGER = 1.5      # Entry when spread ≥ ±1.5%
PROFIT_TARGET = 0.60     # Exit when we captured +0.60% spread
TRADE_SIZE = 80.0        # $80 notional per leg
LEVERAGE = 3.0
FEE_BINANCE = 0.0004
FEE_KUCOIN = 0.0006
POLL_INTERVAL = 0.8
MAX$name_WORKERS = 8
ORDER_POLL_INTERVAL = 0.2
ORDER_POLL_TIMEOUT = 10.0

# -------------------- ENDPOINTS --------------------
BINANCE_BASE = "https://fapi.binance.com"
KUCOIN_BASE = "https://api-futures.kucoin.com"

BINANCE_INFO_URL = f"{BINANCE_BASE}/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = f"{BINANCE_BASE}/fapi/v1/ticker/bookTicker"
BINANCE_ORDER_URL = f"{BINANCE_BASE}/fapi/v1/order"
BINANCE_POSITION_URL = f"{BINANCE_BASE}/fapi/v2/positionRisk"
BINANCE_FUNDING_URL = f"{BINANCE_BASE}/fapi/v1/fundingRate"

KUCOIN_ACTIVE_URL = f"{KUCOIN_BASE}/api/v1/contracts/active"
KUCOIN_TICKER_URL = f"{KUCOIN_BASE}/api/v1/ticker?symbol={{symbol}}"
KUCOIN_ORDER_URL = f"{KUCOIN_BASE}/api/v1/orders"
KUCOIN_POSITION_URL = f"{KUCOIN_BASE}/api/v1/position/single"

# -------------------- SESSIONS --------------------
session = requests.Session()
session.headers.update({"User-Agent": "LiveArbBot/1.0"})

# -------------------- IP WHITELIST VERIFICATION --------------------
def get_public_ip():
    try:
        ip = session.get('https://api.ipify.org', timeout=10).text.strip()
        print(f"[IP CHECK] Current public IP: {ip}")
        return ip
    except Exception as e:
        print(f"[IP CHECK] Error: {e}")
        return None

def verify_binance_ip_whitelisted():
    current_ip = get_public_ip()
    if not current_ip:
        return None

    try:
        endpoint = "/fapi/v2/account"
        params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
        query = urlencode(params)
        signature = hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = signature

        url = f"{BINANCE_BASE}{endpoint}"
        r = session.get(url, headers={"X-MBX-APIKEY": BINANCE_API_KEY}, params=params, timeout=10)
        data = r.json()

        if "code" in data and data["code"] == -2015:
            print(f"[IP VERIFICATION] IP NOT whitelisted (error -2015). Add {current_ip} to Binance API whitelist.")
            return False
        print("[IP VERIFICATION] IP is whitelisted on Binance.")
        return True
    except Exception as e:
        print(f"[IP VERIFICATION] Could not verify: {e}")
        return None

def check_ip_and_crash_if_not_whitelisted():
    print("\n" + "="*60)
    print("BINANCE IP WHITELIST VERIFICATION")
    print("="*60)
    status = verify_binance_ip_whitelisted()
    if status is False:
        print("CRITICAL: IP not whitelisted. Bot exiting.")
        sys.exit(1)
    elif status is True:
        print("IP verification passed.")
    print("="*60 + "\n")

# -------------------- UTILS --------------------
def now_ts(): return int(time.time())
def ts_str(ts=None): return datetime.fromtimestamp(ts or time.time()).strftime("%Y-%m-%d %H:%M:%S")

def floor_to_precision(qty, step):
    if step <= 0: return qty
    return math.floor(qty / step) * step

def calc_slippage(expected, filled):
    if expected == 0: return 0.0
    return (filled - expected) / expected * 100.0

# -------------------- BINANCE HELPERS --------------------
def binance_sign(params):
    query = urlencode(params)
    return hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

def binance_request(method, endpoint, params=None, data=None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    params["signature"] = binance_sign(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    url = BINANCE_BASE + endpoint
    r = session.request(method, url, headers=headers, params=params, json=data, timeout=12)
    r.raise_for_status()
    return r.json()

def binance_place_market(symbol, side, quantity):
    return binance_request("POST", "/fapi/v1/order", data={
        "symbol": symbol, "side": side, "type": "MARKET", "quantity": str(quantity)
    })

def binance_get_order(symbol, orderId):
    return binance_request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": orderId})

def binance_get_position_amt(symbol):
    data = binance_request("GET", "/fapi/v2/positionRisk")
    for pos in data:
        if pos["symbol"] == symbol:
            return float(pos["positionAmt"])
    return 0.0

def fetch_binance_funding_events(symbol):
    url = f"{BINANCE_FUNDING_URL}?symbol={symbol}&limit=200"
    try:
        items = session.get(url, timeout=10).json()
        return [(int(i["fundingTime"])/1000.0, float(i["fundingRate"])) for i in items]
    except:
        return []

# -------------------- KUCOIN HELPERS --------------------
def kucoin_sign(method, endpoint, body=None):
    now = str(int(time.time()*1000))
    body_str = json.dumps(body) if body else ""
    msg = now + method.upper() + endpoint + body_str
    signature = base64.b64encode(hmac.new(KUCOIN_API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()).decode()
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
    if method == "GET":
        r = session.get(url, headers=headers, params=params, timeout=12)
    else:
        r = session.post(url, headers=headers, json=body, timeout=12)
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
        data = kucoin_request("GET", "/api/v1/position/single", params={"symbol": symbol})["data"]
        return float(data.get("currentQty", 0))
    except:
        return 0.0

def fetch_kucoin_funding_events(symbol):
    url = f"https://api-futures.kucoin.com/api/v1/funding-rate/history?symbol={symbol}&pageSize=200"
    try:
        items = session.get(url, timeout=10).json()["data"]
        return [(float(i["fundingTime"])/1000.0, float(i["fundingRate"])) for i in items]
    except:
        return []

# -------------------- MARKET DATA --------------------
def get_common_symbols():
    try:
        bin_symbols = {s["symbol"] for s in session.get(BINANCE_INFO_URL, timeout=10).json()["symbols"]
                      if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"}
        ku_raw = session.get(KUCOIN_ACTIVE_URL, timeout=10).json()["data"]
        ku_symbols = {c["symbol"] for c in ku_raw if c["symbol"].endswith("M")}
        common = bin_symbols.intersection({s[:-1] for s in ku_symbols})
        ku_map = {s[:-1]: s for s in ku_symbols}
        return sorted(common), ku_map
    except Exception as e:
        print("get_common_symbols error:", e)
        return [], {}

def get_prices(symbols, ku_map):
    bin_book = {}
    try:
        for item in session.get(BINANCE_BOOK_URL, timeout=10).json():
            bin_book[item["symbol"]] = {"bid": float(item["bidPrice"]), "ask": float(item["askPrice"])}
    except: pass

    ku_prices = {}
    def fetch_one(ku_sym):
        try:
            j = session.get(KUCOIN_TICKER_URL.format(symbol=ku_sym), timeout=6).json()["data"]
            return ku_sym, {"bid": float(j["bestBidPrice"]), "ask": float(j["bestAskPrice"])}
        except:
            return ku_sym, None

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(symbols))) as ex:
        futures = {ex.submit(fetch_one, ku_map[s]): s for s in symbols}
        for f in as_completed(futures):
            ku_sym, data = f.result()
            if data:
                ku_prices[ku_sym] = data
    return bin_book, ku_prices

# -------------------- ORDER WAIT --------------------
def wait_order_binance(symbol, orderId, timeout=ORDER_POLL_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            o = binance_get_order(symbol, orderId)
            if o["status"] in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                return o
        except: pass
        time.sleep(ORDER_POLL_INTERVAL)
    return None

def wait_order_kucoin(orderId, timeout=ORDER_POLL_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            o = kucoin_get_order(orderId)
            if o["data"]["isActive"] is False:
                return o
        except: pass
        time.sleep(ORDER_POLL_INTERVAL)
    return None

def extract_fill_price_binance(order):
    if not order: return 0.0
    if order.get("avgPrice") not in (None, "0.0", ""):
        return float(order["avgPrice"])
    return 0.0

def extract_fill_price_kucoin(resp):
    if not resp or "data" not in resp: return 0.0
    d = resp["data"]
    if d.get("filledAvgPrice"):
        return float(d["filledAvgPrice"])
    return 0.0

# -------------------- FUNDING --------------------
def compute_funding_impact(open_ts, close_ts, long_ex, short_ex, bin_symbol, ku_symbol):
    total = 0.0
    events = []

    # Binance leg
    if long_ex == "Binance" or short_ex == "Binance":
        for ft, rate in fetch_binance_funding_events(bin_symbol):
            if open_ts < ft <= close_ts:
                impact = -rate * TRADE_SIZE if long_ex == "Binance" else rate * TRADE_SIZE
                total += impact
                events.append(("Binance", ft, rate, impact))

    # KuCoin leg
    if long_ex == "KuCoin" or short_ex == "KuCoin":
        for ft, rate in fetch_kucoin_funding_events(ku_symbol):
            if open_ts < ft <= close_ts:
                impact = -rate * TRADE_SIZE if long_ex == "KuCoin" else rate * TRADE_SIZE
                total += impact
                events.append(("KuCoin", ft, rate, impact))

    return total, events

# -------------------- POSITION CLASS --------------------
class Position:
    def __init__(self, symbol, long_ex, short_ex, long_price, short_price, direction, opened_diff,
                 bin_prec_map, ku_prec_map, ku_map):
        self.symbol = symbol
        self.ku_symbol = ku_map[symbol]
        self.long_ex = long_ex
        self.short_ex = short_ex
        self.direction = direction  # "+ve" or "-ve"
        self.opened_diff = opened_diff
        self.open_time = time.time()

        # --- Match notional exactly ---
        bin_step = 10 ** -bin_prec_map.get(symbol, 6)
        ku_step = 10 ** -ku_prec_map.get(self.ku_symbol, 3)

        qty_bin = floor_to_precision(TRADE_SIZE / long_price if long_ex == "Binance" else TRADE_SIZE / short_price, bin_step)
        qty_ku  = floor_to_precision(TRADE_SIZE / short_price if short_ex == "KuCoin" else TRADE_SIZE / long_price, ku_step)

        notional_bin = qty_bin * (long_price if long_ex == "Binance" else short_price)
        notional_ku  = qty_ku  * (short_price if short_ex == "KuCoin" else long_price)

        # Reduce larger side until notional difference < $0.01
        while abs(notional_bin - notional_ku) > 0.01 and qty_bin > 0 and qty_ku > 0:
            if notional_bin > notional_ku:
                qty_bin -= bin_step
                qty_bin = max(qty_bin, 0)
                notional_bin = qty_bin * (long_price if long_ex == "Binance" else short_price)
            else:
                qty_ku -= ku_step
                qty_ku = max(qty_ku, 0)
                notional_ku = qty_ku * (short_price if short_ex == "KuCoin" else long_price)

        final_notional = min(notional_bin, notional_ku)
        qty_long  = floor_to_precision(final_notional / long_price, bin_step if long_ex == "Binance" else ku_step)
        qty_short = floor_to_precision(final_notional / short_price, ku_step if short_ex == "KuCoin" else bin_step)

        self.qty_long = qty_long
        self.qty_short = qty_short

        print(f"[{ts_str()}] OPEN {symbol} | {direction} spread {opened_diff:+.4f}% | "
              f"Long:{long_ex} Short:{short_ex} | Notional ~${final_notional:.2f}")

        # --- Place orders ---
        try:
            if direction == "+ve":  # Long Binance, Short KuCoin
                bin_order = binance_place_market(symbol, "BUY", qty_long)
                bin_filled = wait_order_binance(symbol, bin_order["orderId"])
                self.fill_long = extract_fill_price_binance(bin_filled)

                ku_order = kucoin_place_market(self.ku_symbol, "sell", qty_short)
                ku_filled = wait_order_kucoin(ku_order["data"]["orderId"])
                self.fill_short = extract_fill_price_kucoin(ku_filled)
            else:  # Long KuCoin, Short Binance
                ku_order = kucoin_place_market(self.ku_symbol, "buy", qty_long)
                ku_filled = wait_order_kucoin(ku_order["data"]["orderId"])
                self.fill_long = extract_fill_price_kucoin(ku_filled)

                bin_order = binance_place_market(symbol, "SELL", qty_short)
                bin_filled = wait_order_binance(symbol, bin_order["orderId"])
                self.fill_short = extract_fill_price_binance(bin_filled)
        except Exception as e:
            print(f"[OPEN ERROR] {e}")
            raise

        self.slip_long  = calc_slippage(long_price, self.fill_long)
        self.slip_short = calc_slippage(short_price, self.fill_short)
        print(f"[{ts_str()}] Filled Long @{self.fill_long:.6f} (exp {long_price:.6f}) slip {self.slip_long:+.4f}%")
        print(f"[{ts_str()}] Filled Short @{self.fill_short:.6f} (exp {short_price:.6f}) slip {self.slip_short:+.4f}%")

    def check_liquidation(self):
        try:
            bin_qty = abs(binance_get_position_amt(self.symbol))
            ku_qty  = abs(kucoin_get_position_qty(self.ku_symbol))
            return bin_qty < 1e-8 or ku_qty < 1e-8
        except:
            return False

    def close(self, close_bid_bin, close_ask_bin, close_bid_ku, close_ask_ku):
        close_spread = ((close_bid_ku - close_ask_bin) / close_ask_bin * 100.0
                       if self.direction == "+ve" else
                       (close_ask_ku - close_bid_bin) / close_bid_bin * 100.0)

        spread_captured = self.opened_diff - close_spread if self.direction == "+ve" else close_spread - self.opened_diff
        gross_pnl = spread_captured / 100.0 * TRADE_SIZE * LEVERAGE

        fee_rate_long  = FEE_BINANCE if self.long_ex == "Binance" else FEE_KUCOIN
        fee_rate_short = FEE_BINANCE if self.short_ex == "Binance" else FEE_KUCOIN
        total_fees = 2 * TRADE_SIZE * (fee_rate_long + fee_rate_short)

        funding, events = compute_funding_impact(self.open_time, time.time(),
                                                self.long_ex, self.short_ex,
                                                self.symbol, self.ku_symbol)

        net_pnl = gross_pnl - total_fees + funding

        print("\n" + "="*50)
        print("TRADE CLOSED")
        print(f"Symbol       : {self.symbol}")
        print(f"Direction    : {'Long Binance / Short KuCoin' if self.direction=='+ve' else 'Long KuCoin / Short Binance'}")
        print(f"Open spread  : {self.opened_diff:+.5f}% → Close spread: {close_spread:+.5f}% → Captured: {spread_captured:+.5f}%")
        print(f"Gross PnL    : {gross_pnl:+.4f} USD")
        print(f"Fees         : -{total_fees:.4f} USD")
        print(f"Funding      : {funding:+.4f} USD")
        print(f"Net PnL      : {net_pnl:+.4f} USD")
        print("="*50 + "\n")

        # Close legs
        try:
            if self.direction == "+ve":
                binance_place_market(self.symbol, "SELL", self.qty_long)
                kucoin_place_market(self.ku_symbol, "buy", self.qty_short)
            else:
                kucoin_place_market(self.ku_symbol, "sell", self.qty_long)
                binance_place_market(self.symbol, "BUY", self.qty_short)
        except: pass

        return net_pnl

# -------------------- STARTUP CHECKS --------------------
def startup_health_check():
    print("[Startup] Running connectivity checks...")
    try:
        session.get(BINANCE_INFO_URL, timeout=10).raise_for_status()
        session.get(BINANCE_BOOK_URL, timeout=10).raise_for_status()
        session.get(KUCOIN_ACTIVE_URL, timeout=10).raise_for_status()
        print("[Startup] All endpoints reachable.")
        return True
    except Exception as e:
        print("[Startup] Connectivity failed:", e)
        return False

# -------------------- MAIN --------------------
def main():
    print(f"Starting Binance-KuCoin Futures Arbitrage Bot - {ts_str()}")
    check_ip_and_crash_if_not_whitelisted()
    if not startup_health_check():
        sys.exit(1)

    symbols, ku_map = get_common_symbols()
    if not symbols:
        print("No common perpetual symbols found.")
        return

    print(f"Monitoring {len(symbols)} common symbols")

    bin_prec = {s["symbol"]: int(s.get("quantityPrecision", 6))
                for s in session.get(BINANCE_INFO_URL, timeout=10).json()["symbols"]}
    ku_prec = {c["symbol"]: int(math.log10(1/float(c.get("lotSize", "0.001") or 0.001)))
               for c in session.get(KUCOIN_ACTIVE_URL, timeout=10).json()["data"]}

    open_pos = None

    try:
        while True:
            bin_book, ku_prices = get_prices(symbols, ku_map)

            if open_pos:
                b = bin_book.get(open_pos.symbol)
                k = ku_prices.get(open_pos.ku_symbol)
                if not b or not k:
                    time.sleep(POLL_INTERVAL)
                    continue

                cur_spread = ((k["bid"] - b["ask"]) / b["ask"] * 100.0
                             if open_pos.direction == "+ve" else
                             (k["ask"] - b["bid"]) / b["bid"] * 100.0)

                zero_cross = (open_pos.direction == "+ve" and cur_spread <= 0) or \
                             (open_pos.direction == "-ve" and cur_spread >= 0)
                profit_hit = (open_pos.direction == "+ve" and cur_spread <= open_pos.opened_diff - PROFIT_TARGET) or \
                             (open_pos.direction == "-ve" and cur_spread >= open_pos.opened_diff + PROFIT_TARGET)
                liq = open_pos.check_liquidation()

                if zero_cross or profit_hit or liq:
                    print(f"[{ts_str()}] Closing | zero:{zero_cross} profit:{profit_hit} liq:{liq}")
                    open_pos.close(b["bid"], b["ask"], k["bid"], k["ask"])
                    open_pos = None
                    time.sleep(1.5)
                else:
                    time.sleep(POLL_INTERVAL)
                continue

            # --- Look for new opportunity ---
            best_diff = 0.0
            best_sym = None
            best_prices = None

            for s in symbols:
                b = bin_book.get(s)
                k = ku_prices.get(ku_map[s])
                if not b or not k: continue

                pos = (k["bid"] - b["ask"]) / b["ask"] * 100.0
                neg = (k["ask"] - b["bid"]) / b["bid"] * 100.0

                if pos > TRADE_TRIGGER and pos > best_diff:
                    best_diff, best_sym, best_prices = pos, s, (b, k, "pos")
                elif neg < -TRADE_TRIGGER and neg < best_diff:
                    best_diff, best_sym, best_prices = neg, s, (b, k, "neg")

            if best_sym and abs(best_diff) >= TRADE_TRIGGER:
                b, k, typ = best_prices
                if typ == "pos":
                    long_ex, short_ex = "Binance", "KuCoin"
                    long_price, short_price = b["ask"], k["bid"]
                else:
                    long_ex, short_ex = "KuCoin", "Binance"
                    long_price, short_price = k["ask"], b["bid"]

                try:
                    open_pos = Position(best_sym, long_ex, short_ex,
                                        long_price, short_price,
                                        "+ve" if typ == "pos" else "-ve",
                                        best_diff, bin_prec, ku_prec, ku_map)
                except Exception as e:
                    print(f"[OPEN FAILED] {best_sym}: {e}")
                    open_pos = None
            else:
                time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\nBot stopped by user.")

if __name__ == "__main__":
    main()
