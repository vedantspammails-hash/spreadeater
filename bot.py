#!/usr/bin/env python3
# Integrated Scanner + Trading bot (Updated per request)
# - Scanner selects candidate symbols each minute and monitors them.
# - Uses exact original 3x consecutive confirmations for entry and enforces a single global open position at a time.
# - When a candidate shows ±SPREAD >= ENTRY_SPREAD for 3 consecutive checks (as in the original trading bot),
#   the trading engine is triggered and executes Case A or Case B WITHOUT modifying any trading logic inside those cases.
# - All trading logic (parallel order execution, rebalance, liquidation watcher, exit logic, slippage/latency logs) is preserved.
# - Telegram removed. Logging kept. Rate error handling included.
#
# Requirements: .env must contain API keys for Binance & KuCoin:
# BINANCE_API_KEY, BINANCE_API_SECRET, KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE
#
# Ready to run on Railway (ensure environment variables are set)

import os
import sys
import time
import math
import requests
import threading
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import traceback

try:
    import ccxt
except Exception:
    print("ccxt required. pip install ccxt")
    raise

from dotenv import load_dotenv
load_dotenv()

# ------------------------- REQUIRED KEYS CHECK -------------------------
REQUIRED = ['BINANCE_API_KEY','BINANCE_API_SECRET','KUCOIN_API_KEY','KUCOIN_API_SECRET','KUCOIN_API_PASSPHRASE']
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print(f"ERROR: Missing .env keys: {', '.join(missing)}")
    sys.exit(1)

# ======================= TRADING CONFIG (from original trading bot) =======================
NOTIONAL = float(os.getenv('NOTIONAL', "10.0"))
LEVERAGE = int(os.getenv('LEVERAGE', "5"))
ENTRY_SPREAD = float(os.getenv('ENTRY_SPREAD', "1.5"))
PROFIT_TARGET = float(os.getenv('PROFIT_TARGET', "0.05"))
MARGIN_BUFFER = float(os.getenv('MARGIN_BUFFER', "1.02"))

# Liquidation watcher config
WATCHER_POLL_INTERVAL = float(os.getenv('WATCHER_POLL_INTERVAL', "0.5"))
WATCHER_DETECT_CONFIRM = int(os.getenv('WATCHER_DETECT_CONFIRM', "2"))

# Notional / rebalance config
MAX_NOTIONAL_MISMATCH_PCT = float(os.getenv('MAX_NOTIONAL_MISMATCH_PCT', "0.5"))
REBALANCE_MIN_DOLLARS = float(os.getenv('REBALANCE_MIN_DOLLARS', "0.5"))

print(f"\n{'='*72}")
print(f"INTEGRATED SCANNER+TRADER | NOTIONAL ${NOTIONAL} @ {LEVERAGE}x | ENTRY >= {ENTRY_SPREAD}% | PROFIT TARGET {PROFIT_TARGET}%")
print(f"NOTIONAL mismatch tolerance: {MAX_NOTIONAL_MISMATCH_PCT}% | REBALANCE_MIN_DOLLARS: ${REBALANCE_MIN_DOLLARS}")
print(f"{'='*72}\n")

# ======================= SCANNER CONFIG (from spreadwatcher) =======================
SCAN_THRESHOLD = 0.25
ALERT_THRESHOLD = 5.0
ALERT_COOLDOWN = 60
SUMMARY_INTERVAL = 300
MAX_WORKERS = 12

MONITOR_DURATION = 60
MONITOR_POLL = 2
CONFIRM_RETRY_DELAY = 0.5
CONFIRM_RETRIES = 2

# API endpoints used by scanner price fetches
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
KUCOIN_ACTIVE_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
KUCOIN_TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"

# -------------------- Logging setup --------------------
logger = logging.getLogger("integrated_arb")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

fh = RotatingFileHandler("integrated_arb.log", maxBytes=10_000_000, backupCount=5)
fh.setLevel(logging.DEBUG)
fh_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ======================= Exchanges (ccxt) =======================
binance = ccxt.binance({
    'apiKey': os.getenv('BINANCE_API_KEY'),
    'secret': os.getenv('BINANCE_API_SECRET'),
    'options': {'defaultType':'future'},
    'enableRateLimit': True
})
kucoin = ccxt.kucoinfutures({
    'apiKey': os.getenv('KUCOIN_API_KEY'),
    'secret': os.getenv('KUCOIN_API_SECRET'),
    'password': os.getenv('KUCOIN_API_PASSPHRASE'),
    'enableRateLimit': True
})

# Time sync
def fix_time_offset():
    try:
        server = requests.get("https://fapi.binance.com/fapi/v1/time", timeout=5).json().get('serverTime')
        if server: binance.options['timeDifference'] = int(time.time()*1000) - int(server)
    except Exception:
        pass
    try:
        server = requests.get("https://api-futures.kucoin.com/api/v1/timestamp", timeout=5).json().get('data')
        if server: kucoin.options['timeDifference'] = int(time.time()*1000) - int(server)
    except Exception:
        pass

fix_time_offset()

def ensure_markets_loaded():
    for ex in [binance, kucoin]:
        try:
            ex.load_markets(reload=False)
        except Exception:
            try:
                ex.load_markets(reload=True)
            except Exception:
                pass

def get_market(exchange, symbol):
    ensure_markets_loaded()
    m = exchange.markets.get(symbol)
    if not m:
        try:
            exchange.load_markets(reload=True)
        except Exception:
            pass
        m = exchange.markets.get(symbol)
    return m

def resolve_kucoin_trade_symbol(exchange, raw_id):
    try:
        exchange.load_markets(reload=True)
    except Exception:
        pass
    raw_id = (raw_id or "").upper()
    for sym, m in (exchange.markets or {}).items():
        if (m.get('id') or "").upper() == raw_id:
            return sym
    for sym, m in (exchange.markets or {}).items():
        if raw_id in (m.get('id') or "").upper():
            return sym
    return None

def set_leverage_and_margin_for_symbol(bin_sym, kc_ccxt_sym):
    try:
        binance.set_leverage(LEVERAGE, bin_sym)
        print(f"Binance leverage set to {LEVERAGE}x for {bin_sym}")
    except Exception as e:
        print(f"Binance leverage error for {bin_sym}: {e}")
    if kc_ccxt_sym:
        try:
            kucoin.set_leverage(LEVERAGE, kc_ccxt_sym, {'marginMode':'cross'})
            print(f"KuCoin leverage set to {LEVERAGE}x CROSS for {kc_ccxt_sym}")
        except Exception as e:
            print(f"KuCoin leverage error for {kc_ccxt_sym}: {e}")

ensure_markets_loaded()

# ======================= Shared State =======================
positions = {}  # Binance symbol -> None / 'caseA' / 'caseB'
trade_start_balances = {}
entry_spreads = {}
entry_prices = {}
entry_actual = {}
entry_confirm_count = {}
exit_confirm_count = {}

KUCOIN_RAW_MAP = {}   # bin_sym -> ku_raw (like "BTCUSDTM")
KUCOIN_CCXT_MAP = {}  # bin_sym -> ku_ccxt_sym (ccxt market symbol)
TRADED_BINANCE_SYMBOLS = []

_liquidation_watchers = {}
closing_in_progress = False
terminate_bot = False

# -------------------- Helpers from trading bot (unchanged behavior) --------------------
def round_down(value, precision):
    if precision is None: return float(value)
    factor = 10**precision
    return math.floor(value*factor)/factor

def compute_amount_for_notional(exchange, symbol, desired_usdt, price):
    market = get_market(exchange, symbol)
    amount_precision = None
    contract_size = 1.0
    if market:
        prec = market.get('precision')
        if isinstance(prec, dict): amount_precision = prec.get('amount')
        contract_size = float(market.get('contractSize') or market.get('info', {}).get('contractSize') or 1.0)
    if price <= 0: return 0.0, 0.0, contract_size, amount_precision
    if exchange.id == 'binance':
        base = desired_usdt / price
        amt = round_down(base, amount_precision)
        implied = amt * contract_size * price
        return float(amt), float(implied), contract_size, amount_precision
    else:
        base = desired_usdt / price
        contracts = base / contract_size if contract_size else base
        contracts = round_down(contracts, amount_precision)
        implied = contracts * contract_size * price
        return float(contracts), float(implied), contract_size, amount_precision

def _get_signed_from_binance_pos(pos):
    info = pos.get('info') or {}
    for fld in ('positionAmt',):
        v = pos.get(fld)
        if v not in (None, ''):
            try:
                return float(v)
            except Exception:
                try:
                    return float(str(v).replace(',', ''))
                except Exception:
                    pass
    for fld in ('positionAmt', 'position_amount', 'amount'):
        v = info.get(fld)
        if v not in (None, ''):
            try:
                return float(v)
            except Exception:
                try:
                    return float(str(v).replace(',', ''))
                except Exception:
                    pass
    magnitude = 0.0
    for k in ('contracts', 'amount', 'size'):
        v = pos.get(k) or info.get(k)
        if v not in (None, ''):
            try:
                magnitude = float(v)
                break
            except Exception:
                pass
    side_field = ''
    for candidate in (pos.get('side'), info.get('side'), info.get('positionSide'), info.get('type')):
        if candidate:
            side_field = str(candidate).lower()
            break
    if side_field in ('short', 'sell', 'shortside'):
        return -abs(magnitude)
    if side_field in ('long', 'buy'):
        return abs(magnitude)
    return float(magnitude or 0.0)

def _get_signed_from_kucoin_pos(pos):
    info = pos.get('info') or {}
    for fld in ('currentQty',):
        v = info.get(fld)
        if v not in (None, ''):
            try:
                return float(v)
            except Exception:
                try:
                    return float(str(v).replace(',', ''))
                except Exception:
                    pass
    magnitude = 0.0
    for k in ('contracts', 'size', 'positionAmt', 'amount'):
        v = pos.get(k) or info.get(k)
        if v not in (None, ''):
            try:
                magnitude = float(v)
                break
            except Exception:
                pass
    side_field = ''
    for candidate in (pos.get('side'), info.get('side'), info.get('positionSide'), info.get('type')):
        if candidate:
            side_field = str(candidate).lower()
            break
    if side_field in ('short', 'sell', 'shortside'):
        return -abs(magnitude)
    if side_field in ('long', 'buy'):
        return abs(magnitude)
    return float(magnitude or 0.0)

def _get_signed_position_amount(pos):
    try:
        info = pos.get('info') or {}
        if any(k in pos for k in ('positionAmt',)) or 'positionAmt' in info:
            return _get_signed_from_binance_pos(pos)
        if 'currentQty' in info or 'contracts' in pos:
            return _get_signed_from_kucoin_pos(pos)
    except Exception:
        pass
    try:
        v = _get_signed_from_binance_pos(pos)
        if v != 0:
            return v
    except Exception:
        pass
    try:
        return _get_signed_from_kucoin_pos(pos)
    except Exception:
        pass
    return 0.0

def get_total_futures_balance():
    try:
        bal_bin = binance.fetch_balance(params={'type':'future'})
        bin_usdt = float(bal_bin.get('USDT', {}).get('total', 0.0))
        bal_kc = kucoin.fetch_balance()
        kc_usdt = float(bal_kc.get('USDT', {}).get('total', 0.0))
        total_balance = bin_usdt + kc_usdt
        return total_balance, bin_usdt, kc_usdt
    except Exception as e:
        print(f"Error fetching balances: {e}")
        return 0.0, 0.0, 0.0

def has_open_positions():
    try:
        for sym in list(TRADED_BINANCE_SYMBOLS):
            pos = None
            try:
                pos = binance.fetch_positions([sym])
            except Exception:
                try:
                    pos = binance.fetch_positions()
                except Exception:
                    pos = None
            if pos:
                p = pos[0]
                raw = p.get('positionAmt') or p.get('contracts') or 0
                try:
                    if abs(float(raw or 0)) > 0:
                        return True
                except Exception:
                    pass
        for bin_sym, kc_ccxt_sym in KUCOIN_CCXT_MAP.items():
            if not kc_ccxt_sym:
                continue
            pos = None
            try:
                pos = kucoin.fetch_positions([kc_ccxt_sym])
            except Exception:
                try:
                    allp = kucoin.fetch_positions()
                    for p in allp:
                        if p.get('symbol') == kc_ccxt_sym:
                            pos = [p]
                            break
                except Exception:
                    pos = None
            if pos:
                raw = None
                try:
                    raw = pos[0].get('contracts') or pos[0].get('positionAmt') or pos[0].get('info', {}).get('currentQty') or 0
                except Exception:
                    raw = 0
                try:
                    if abs(float(raw or 0)) > 0:
                        return True
                except Exception:
                    pass
        return False
    except Exception:
        return True

def close_single_exchange_position(exchange, symbol):
    try:
        pos_list = []
        try:
            pos_list = exchange.fetch_positions([symbol])
        except Exception:
            try:
                pos_list = exchange.fetch_positions()
            except Exception:
                pos_list = []
        if not pos_list:
            print(f"{datetime.now().isoformat()} No positions returned for {exchange.id} {symbol} (treated as already closed).")
            return True
        pos = pos_list[0]
        raw_signed = None
        try:
            if exchange.id == 'binance':
                raw_signed = _get_signed_from_binance_pos(pos)
            else:
                raw_signed = _get_signed_from_kucoin_pos(pos)
        except Exception:
            raw_signed = _get_signed_position_amount(pos)

        raw_signed = float(raw_signed or 0.0)
        if abs(raw_signed) == 0:
            print(f"{datetime.now().isoformat()} No open qty to close for {exchange.id} {symbol}.")
            return True

        side = 'sell' if raw_signed > 0 else 'buy'
        qty = abs(raw_signed)
        market = get_market(exchange, symbol)
        prec = market.get('precision', {}).get('amount') if market else None
        qty_rounded = round_down(qty, prec) if prec is not None else qty
        if qty_rounded > 0:
            try:
                print(f"{datetime.now().isoformat()} Submitting targeted reduceOnly market close on {exchange.id} {symbol} -> {side} {qty_rounded}")
                try:
                    exchange.create_market_order(symbol, side, qty_rounded, params={'reduceOnly': True, 'marginMode': 'cross'})
                except TypeError:
                    exchange.create_order(symbol=symbol, type='market', side=side, amount=qty_rounded, params={'reduceOnly': True})
                print(f"{datetime.now().isoformat()} Targeted reduceOnly close submitted on {exchange.id} {symbol}")
                return True
            except Exception as e:
                err_text = str(e)
                print(f"{datetime.now().isoformat()} Targeted reduceOnly close failed on {exchange.id} {symbol}: {err_text}")
                try:
                    print(f"{datetime.now().isoformat()} Trying closePosition fallback on {exchange.id} {symbol}")
                    try:
                        exchange.create_order(symbol=symbol, type='market', side=side, amount=None, params={'closePosition': True})
                    except TypeError:
                        exchange.create_order(symbol, 'market', side, params={'closePosition': True})
                    print(f"{datetime.now().isoformat()} closePosition fallback submitted on {exchange.id} {symbol}")
                    return True
                except Exception as e2:
                    print(f"{datetime.now().isoformat()} closePosition fallback failed on {exchange.id} {symbol}: {e2}")
                    return False
        else:
            try:
                print(f"{datetime.now().isoformat()} qty rounded to 0, using closePosition fallback on {exchange.id} {symbol}")
                try:
                    exchange.create_order(symbol=symbol, type='market', side=side, amount=None, params={'closePosition': True})
                except TypeError:
                    exchange.create_order(symbol, 'market', side, params={'closePosition': True})
                print(f"{datetime.now().isoformat()} closePosition fallback submitted on {exchange.id} {symbol}")
                return True
            except Exception as e:
                print(f"{datetime.now().isoformat()} closePosition fallback failed on {exchange.id} {symbol}: {e}")
                return False
    except Exception as e:
        print(f"{datetime.now().isoformat()} Error in close_single_exchange_position({exchange.id},{symbol}): {e}")
        return False

def close_all_and_wait(timeout_s=20, poll_interval=0.5):
    global closing_in_progress
    closing_in_progress = True
    print("\n" + "="*72)
    print("Closing all positions...")
    print("="*72)

    for sym in list(TRADED_BINANCE_SYMBOLS):
        try:
            positions_bin = binance.fetch_positions([sym])
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance fetched positions for {sym}: {positions_bin}")
        except Exception as e:
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance fetch_positions error for {sym}: {e}")
            positions_bin = None

        if positions_bin:
            pos = positions_bin[0]
            try:
                raw_info = pos.get('info') if isinstance(pos, dict) else None
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance position raw info for {sym}: {raw_info}")
            except Exception:
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance position raw info unavailable for {sym}")

            raw_signed = _get_signed_position_amount(pos)
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance raw positionAmt (signed) for {sym}: {raw_signed}")
            if abs(raw_signed) > 0:
                side = 'sell' if raw_signed > 0 else 'buy'
                market = get_market(binance, sym)
                prec = market.get('precision', {}).get('amount') if market else None
                qty = round_down(abs(raw_signed), prec) if prec is not None else abs(raw_signed)
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance qty to close for {sym}: {qty} (precision={prec})")

                if qty > 0:
                    try:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Attempting Binance qty-based reduceOnly close for {sym} -> {side} {qty}")
                        try:
                            binance.create_market_order(sym, side, qty, params={'reduceOnly': True})
                        except TypeError:
                            binance.create_order(symbol=sym, type='market', side=side, amount=qty, params={'reduceOnly': True})
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance qty-based reduceOnly close submitted for {sym}")
                    except Exception as e:
                        err_text = str(e)
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE qty close failed for {sym}: {err_text}")
                        if 'ReduceOnly' in err_text or 'reduceOnly' in err_text.lower() or 'Reduce only' in err_text or '"code":-2022' in err_text or '-2022' in err_text:
                            try:
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Detected ReduceOnly rejection, attempting Binance closePosition=True fallback for {sym}")
                                try:
                                    binance.create_order(symbol=sym, type='market', side=side, amount=None, params={'closePosition': True})
                                except TypeError:
                                    binance.create_order(symbol=sym, type='market', side=side, params={'closePosition': True})
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE closePosition fallback submitted for {sym}")
                            except Exception as e2:
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE closePosition fallback failed for {sym}: {e2}")
                        else:
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE close failed for {sym} with unexpected error: {e}")
                else:
                    try:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} qty<=0, using closePosition=True for Binance {sym}")
                        try:
                            binance.create_order(symbol=sym, type='market', side=side, amount=None, params={'closePosition': True})
                        except TypeError:
                            binance.create_order(symbol=sym, type='market', side=side, params={'closePosition': True})
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE closePosition submitted for {sym}")
                    except Exception as e:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE closePosition failed for {sym}: {e}")

        time.sleep(0.15)

    all_kc_positions = []
    try:
        kc_syms = list(set([s for s in KUCOIN_CCXT_MAP.values() if s]))
        if kc_syms:
            all_kc_positions = kucoin.fetch_positions(symbols=kc_syms)
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} KuCoin fetched all positions: {all_kc_positions}")
    except Exception as e:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Error fetching KuCoin positions via ccxt: {e}")

    if not all_kc_positions:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} No open positions found on KuCoin via ccxt.")
    else:
        for pos in all_kc_positions:
            ccxt_sym = pos.get('symbol')
            raw_qty_signed = 0.0
            if isinstance(pos.get('info'), dict):
                try:
                    raw_qty_signed = float(pos.get('info').get('currentQty') or 0)
                except Exception:
                    raw_qty_signed = 0.0
            if abs(raw_qty_signed) == 0:
                size = float(pos.get('size') or pos.get('contracts') or pos.get('positionAmt') or 0)
                if size > 0:
                    side_norm = pos.get('side')
                    raw_qty_signed = size * (-1 if side_norm == 'short' else 1)
            if abs(raw_qty_signed) == 0:
                continue
            if ccxt_sym not in list(KUCOIN_CCXT_MAP.values()):
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Skipping KuCoin position for untracked symbol: {ccxt_sym}")
                continue
            side = 'sell' if raw_qty_signed > 0 else 'buy'
            qty = abs(raw_qty_signed)
            market = get_market(kucoin, ccxt_sym)
            prec = market.get('precision', {}).get('amount') if market else None
            qty = round_down(qty, prec) if prec is not None else qty
            if qty > 0:
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Closing KuCoin {ccxt_sym} {side} {qty} (raw_qty_signed={raw_qty_signed})")
                try:
                    kucoin.create_market_order(ccxt_sym, side, qty, params={'reduceOnly': True, 'marginMode': 'cross'})
                    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} KuCoin close order submitted for {ccxt_sym}")
                except Exception as e:
                    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} KUCOIN close order failed for {ccxt_sym}: {e}")

    start = time.time()
    while time.time() - start < timeout_s:
        open_now = has_open_positions()
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Checking open positions... has_open_positions() => {open_now}")
        if not open_now:
            closing_in_progress = False
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} All positions closed and confirmed.")
            total_bal, bin_bal, kc_bal = get_total_futures_balance()
            print(f"*** POST-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
            print("="*72)
            return True
        time.sleep(poll_interval)
    closing_in_progress = False
    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Timeout waiting for positions to close.")
    print("="*72)
    return False

def extract_executed_price_and_time(exchange, symbol, order_obj):
    try:
        if isinstance(order_obj, dict):
            avg = order_obj.get('average') or order_obj.get('price')
            info = order_obj.get('info') or {}
            if not avg:
                avg = info.get('avgPrice') or info.get('avg_price') or info.get('price')
            if avg:
                ts = order_obj.get('timestamp') or info.get('transactTime') or info.get('time') or info.get('tradeTime')
                if ts:
                    try:
                        ts_int = int(ts)
                        if ts_int > 1e12:
                            ts_ms = ts_int
                        elif ts_int > 1e9:
                            ts_ms = ts_int * 1000
                        else:
                            ts_ms = int(time.time() * 1000)
                        iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + 'Z'
                        return float(avg), iso
                    except Exception:
                        pass
                return float(avg), datetime.utcnow().isoformat() + 'Z'
    except Exception:
        pass
    try:
        now_ms = int(time.time() * 1000)
        trades = exchange.fetch_my_trades(symbol, since=now_ms-60000, limit=20)
        if trades:
            t = sorted(trades, key=lambda x: x.get('timestamp') or 0)[-1]
            px = t.get('price') or (t.get('info') or {}).get('price')
            ts = t.get('timestamp') or (t.get('info') or {}).get('time')
            if px:
                if ts:
                    try:
                        ts_ms = int(ts)
                        if ts_ms < 1e12 and ts_ms > 1e9:
                            ts_ms = ts_ms * 1000
                        iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + 'Z'
                        return float(px), iso
                    except Exception:
                        pass
                return float(px), datetime.utcfromtimestamp((t.get('timestamp') or int(time.time() * 1000))/1000.0).isoformat() + 'Z'
    except Exception:
        pass
    try:
        t = exchange.fetch_ticker(symbol)
        mid = None
        if t:
            bid = t.get('bid') or t.get('bidPrice')
            ask = t.get('ask') or t.get('askPrice')
            if bid and ask:
                mid = (float(bid) + float(ask)) / 2.0
            elif t.get('last'):
                mid = float(t.get('last'))
        if mid:
            return float(mid), datetime.utcnow().isoformat() + 'Z'
    except Exception:
        pass
    return None, None

def safe_create_order(exchange, side, notional, price, symbol, trigger_time=None, trigger_price=None):
    amt, _, _, prec = compute_amount_for_notional(exchange, symbol, notional, price)
    amt = round_down(amt, prec) if prec is not None else amt
    if amt <= 0:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} computed amt <=0, skipping order for {exchange.id} {symbol} (notional=${notional} price={price})")
        return False, None, None
    try:
        sent_time = datetime.utcnow()
        if exchange.id == 'binance':
            if side.lower() == 'buy':
                order = exchange.create_market_buy_order(symbol, amt)
            else:
                order = exchange.create_market_sell_order(symbol, amt)
        else:
            params = {'leverage': LEVERAGE, 'marginMode': 'cross'}
            if side.lower() == 'buy':
                order = exchange.create_market_buy_order(symbol, amt, params=params)
            else:
                order = exchange.create_market_sell_order(symbol, amt, params=params)
        exec_price, exec_time = extract_executed_price_and_time(exchange, symbol, order)
        slippage = None
        latency_ms = None
        if trigger_price is not None and exec_price is not None:
            slippage = exec_price - float(trigger_price)
        if trigger_time is not None and exec_time is not None:
            try:
                t0_ms = int(trigger_time.timestamp() * 1000)
                t1_ms = int(datetime.fromisoformat(exec_time.replace('Z', '')).timestamp() * 1000)
                latency_ms = t1_ms - t0_ms
            except Exception:
                latency_ms = None
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {exchange.id.upper()} ORDER EXECUTED | {side.upper()} {amt} {symbol} at market | exec_price={exec_price} exec_time={exec_time} slippage={slippage} latency_ms={latency_ms}")
        return True, exec_price, exec_time
    except Exception as e:
        print(f"{exchange.id.upper()} order failed: {e}")
        return False, None, None

def match_base_exposure_per_exchange(bin_exchange, kc_exchange, bin_symbol, kc_symbol, desired_usdt, bin_price, kc_price):
    m_bin = get_market(bin_exchange, bin_symbol)
    m_kc = get_market(kc_exchange, kc_symbol)
    bin_prec = None
    kc_prec = None
    bin_contract_size = 1.0
    kc_contract_size = 1.0
    try:
        if m_bin:
            prec = m_bin.get('precision')
            if isinstance(prec, dict): bin_prec = prec.get('amount')
            bin_contract_size = float(m_bin.get('contractSize') or m_bin.get('info', {}).get('contractSize') or 1.0)
    except Exception:
        pass
    try:
        if m_kc:
            prec = m_kc.get('precision')
            if isinstance(prec, dict): kc_prec = prec.get('amount')
            kc_contract_size = float(m_kc.get('contractSize') or m_kc.get('info', {}).get('contractSize') or 1.0)
    except Exception:
        pass
    try:
        ref_price = (float(bin_price) + float(kc_price)) / 2.0
        if ref_price <= 0:
            ref_price = float(bin_price) or float(kc_price) or 1.0
    except Exception:
        ref_price = float(bin_price) or float(kc_price) or 1.0
    target_base = desired_usdt / ref_price
    bin_base_amount = round_down(target_base, bin_prec) if bin_prec is not None else target_base
    kc_contracts = 0.0
    if kc_contract_size and kc_contract_size > 0:
        kc_contracts = round_down(target_base / kc_contract_size, kc_prec) if kc_prec is not None else (target_base / kc_contract_size)
    else:
        kc_contracts = round_down(target_base, kc_prec) if kc_prec is not None else target_base
    notional_bin = bin_base_amount * float(bin_price)
    notional_kc = kc_contracts * float(kc_contract_size) * float(kc_price)
    if bin_base_amount <= 0:
        step_bin = (bin_contract_size * bin_price) if bin_contract_size and bin_price else (desired_usdt * 0.001)
        if bin_prec is not None:
            bin_base_amount = round_down(step_bin / bin_price, bin_prec)
        else:
            bin_base_amount = step_bin / bin_price
        notional_bin = bin_base_amount * float(bin_price)
    if kc_contracts <= 0:
        step_kc = (kc_contract_size * kc_price) if kc_contract_size and kc_price else (desired_usdt * 0.001)
        if kc_prec is not None:
            kc_contracts = round_down((step_kc / kc_contract_size) if kc_contract_size else step_kc, kc_prec)
        else:
            kc_contracts = (step_kc / kc_contract_size) if kc_contract_size else step_kc
        notional_kc = kc_contracts * float(kc_contract_size) * float(kc_price)
    return float(notional_bin), float(notional_kc), float(bin_base_amount), float(kc_contracts)

def get_prices_for_symbols(bin_symbols, kucoin_raw_symbols):
    bin_prices = {}
    kc_prices = {}
    try:
        data = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
        for s in bin_symbols:
            for item in data:
                if item['symbol'] == s:
                    bin_prices[s] = (float(item['bidPrice']), float(item['askPrice']))
                    break
        for raw_id in kucoin_raw_symbols:
            resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={raw_id}", timeout=5).json()
            d = resp.get('data', {})
            kc_prices[raw_id] = (float(d.get('bestBidPrice', '0') or 0), float(d.get('bestAskPrice', '0') or 0))
    except Exception:
        pass
    return bin_prices, kc_prices

# -------------------- Liquidation watcher (kept) --------------------
def _fetch_signed_binance(sym):
    try:
        p = binance.fetch_positions([sym])
        if not p:
            return 0.0
        pos = p[0]
        return _get_signed_from_binance_pos(pos)
    except Exception as e:
        print(f"{datetime.utcnow().isoformat()} BINANCE fetch error for {sym}: {e}")
        return None

def _fetch_signed_kucoin(ccxt_sym):
    try:
        p = None
        try:
            p = kucoin.fetch_positions([ccxt_sym])
        except Exception:
            allp = kucoin.fetch_positions()
            p = []
            for pos in allp:
                if pos.get('symbol') == ccxt_sym:
                    p.append(pos)
                    break
        if not p:
            return 0.0
        pos = p[0]
        return _get_signed_from_kucoin_pos(pos)
    except Exception as e:
        print(f"{datetime.utcnow().isoformat()} KUCOIN fetch error for {ccxt_sym}: {e}")
        return None

def _start_liquidation_watcher_for_symbol(sym, bin_sym, kc_sym):
    if _liquidation_watchers.get(sym):
        return

    stop_flag = threading.Event()
    _liquidation_watchers[sym] = stop_flag

    def monitor():
        print(f"{datetime.now().isoformat()} Liquidation watcher STARTED for {sym} (bin:{bin_sym} kc:{kc_sym})")
        prev_bin = None
        prev_kc = None
        zero_cnt_bin = 0
        zero_cnt_kc = 0
        seen_nonzero_bin = False
        seen_nonzero_kc = False

        wb = _fetch_signed_binance(bin_sym)
        wk = _fetch_signed_kucoin(kc_sym) if kc_sym else 0.0
        prev_bin = wb if wb is not None else 0.0
        prev_kc = wk if wk is not None else 0.0
        if abs(prev_bin) > 1e-8:
            seen_nonzero_bin = True
        if abs(prev_kc) > 1e-8:
            seen_nonzero_kc = True

        ZERO_ABS_THRESHOLD = 1e-8

        while not stop_flag.is_set():
            try:
                if closing_in_progress or positions.get(sym) is None:
                    zero_cnt_bin = zero_cnt_kc = 0
                    if positions.get(sym) is None:
                        print(f"{datetime.now().isoformat()} Liquidation watcher stopping for {sym} because positions[sym] is None")
                        break
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue

                cur_bin = _fetch_signed_binance(bin_sym)
                cur_kc = _fetch_signed_kucoin(kc_sym) if kc_sym else 0.0

                if cur_bin is None or cur_kc is None:
                    print(f"{datetime.now().isoformat()} WATCHER SKIP (transient fetch error) prev_bin={prev_bin} prev_kc={prev_kc} cur_bin={cur_bin} cur_kc={cur_kc} zero_cnt_bin={zero_cnt_bin} zero_cnt_kc={zero_cnt_kc}")
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue

                cur_bin_abs = abs(float(cur_bin))
                cur_kc_abs = abs(float(cur_kc))

                if cur_bin_abs > ZERO_ABS_THRESHOLD:
                    seen_nonzero_bin = True
                    zero_cnt_bin = 0
                else:
                    if seen_nonzero_bin:
                        zero_cnt_bin += 1

                if cur_kc_abs > ZERO_ABS_THRESHOLD:
                    seen_nonzero_kc = True
                    zero_cnt_kc = 0
                else:
                    if seen_nonzero_kc:
                        zero_cnt_kc += 1

                print(f"{datetime.now().isoformat()} WATCHER {sym} prev_bin={prev_bin:.6f} cur_bin={cur_bin:.6f} prev_kc={prev_kc:.6f} cur_kc={cur_kc:.6f} zero_cnt_bin={zero_cnt_bin}/{WATCHER_DETECT_CONFIRM} zero_cnt_kc={zero_cnt_kc}/{WATCHER_DETECT_CONFIRM}")

                if zero_cnt_bin >= WATCHER_DETECT_CONFIRM:
                    print(f"{datetime.now().isoformat()} Detected sustained ZERO on Binance for {bin_sym} -> attempting targeted close of KuCoin and full cleanup.")
                    try:
                        ok = close_single_exchange_position(kucoin, kc_sym)
                        if not ok:
                            print(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} Error when closing after Binance zero: {e}")
                    global terminate_bot
                    terminate_bot = True
                    break

                if zero_cnt_kc >= WATCHER_DETECT_CONFIRM:
                    print(f"{datetime.now().isoformat()} Detected sustained ZERO on KuCoin for {kc_sym} -> attempting targeted close of Binance and full cleanup.")
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            print(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} Error when closing after KuCoin zero: {e}")
                    terminate_bot = True
                    break

                prev_bin = cur_bin
                prev_kc = cur_kc
                time.sleep(WATCHER_POLL_INTERVAL)
            except Exception as e:
                print(f"{datetime.now().isoformat()} Liquidation watcher exception for {sym}: {e}")
                time.sleep(0.5)

        _liquidation_watchers.pop(sym, None)
        print(f"{datetime.now().isoformat()} Liquidation watcher EXIT for {sym}")

    t = threading.Thread(target=monitor, daemon=True)
    t.start()

# ===================== SCANNER Utilities (kept/merged) =====================
def normalize(sym):
    if not sym:
        return sym
    s = sym.upper()
    if s.endswith("USDTM"):
        return s[:-1]
    if s.endswith("USDTP"):
        return s[:-1]
    if s.endswith("M"):
        return s[:-1]
    return s

def get_binance_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BINANCE_INFO_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            syms = [s["symbol"] for s in data.get("symbols", [])
                    if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
            logger.debug("[BINANCE] fetched %d symbols (sample: %s)", len(syms), syms[:6])
            return syms
        except Exception as e:
            logger.warning("[BINANCE] attempt %d error: %s", attempt, str(e))
            if attempt == retries:
                logger.exception("[BINANCE] final failure fetching symbols")
                return []
            time.sleep(0.7)

def get_kucoin_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(KUCOIN_ACTIVE_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            raw = data.get("data", []) if isinstance(data, dict) else []
            syms = [s["symbol"] for s in raw if s.get("status", "").lower() == "open"]
            logger.debug("[KUCOIN] fetched %d symbols (sample: %s)", len(syms), syms[:6])
            return syms
        except Exception as e:
            logger.warning("[KUCOIN] attempt %d error: %s", attempt, str(e))
            if attempt == retries:
                logger.exception("[KUCOIN] final failure fetching symbols")
                return []
            time.sleep(0.7)

def get_common_symbols():
    bin_syms = get_binance_symbols()
    ku_syms = get_kucoin_symbols()
    bin_set = {normalize(s) for s in bin_syms}
    ku_set = {normalize(s) for s in ku_syms}
    common = bin_set.intersection(ku_set)
    ku_map = {}
    dup_count = 0
    for s in ku_syms:
        n = normalize(s)
        if n in ku_map and ku_map[n] != s:
            dup_count += 1
        else:
            ku_map[n] = s
    if dup_count:
        logger.warning("Duplicate normalized KuCoin symbols detected: %d (kept first)", dup_count)
    logger.info("Common symbols: %d (sample: %s)", len(common), list(common)[:8])
    return common, ku_map

def get_binance_book(retries=1):
    for attempt in range(1, retries+1):
        try:
            r = requests.get(BINANCE_BOOK_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            out = {}
            for d in data:
                try:
                    out[d["symbol"]] = {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
                except Exception:
                    continue
            logger.debug("[BINANCE_BOOK] entries: %d", len(out))
            return out
        except Exception:
            logger.exception("[BINANCE_BOOK] fetch error")
            if attempt == retries:
                return {}
            time.sleep(0.5)

def get_binance_price(symbol, session, retries=1):
    for attempt in range(1, retries+1):
        try:
            url = BINANCE_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                logger.debug("Binance ticker non-200 %s for %s: %s", r.status_code, symbol, r.text[:200])
                return None, None
            d = r.json()
            bid = float(d.get("bidPrice") or 0)
            ask = float(d.get("askPrice") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
            logger.debug("Binance price fetch failed for %s (attempt %d)", symbol, attempt)
            if attempt == retries:
                logger.exception("Binance price final failure for %s", symbol)
                return None, None
            time.sleep(0.2)

def get_kucoin_price_once(symbol, session, retries=1):
    for attempt in range(1, retries+1):
        try:
            url = KUCOIN_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                logger.debug("KuCoin ticker non-200 %s for %s: %s", r.status_code, symbol, r.text[:200])
                return None, None
            data = r.json()
            d = data.get("data", {}) if isinstance(data, dict) else {}
            bid = float(d.get("bestBidPrice") or d.get("bid") or 0)
            ask = float(d.get("bestAskPrice") or d.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
            logger.debug("KuCoin price fetch failed for %s (attempt %d)", symbol, attempt)
            if attempt == retries:
                logger.exception("KuCoin price final failure for %s", symbol)
                return None, None
            time.sleep(0.2)

def threaded_kucoin_prices(symbols):
    prices = {}
    if not symbols:
        return prices
    workers = min(MAX_WORKERS, max(4, len(symbols)))
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(get_kucoin_price_once, s, session): s for s in symbols}
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    bid, ask = fut.result()
                    if bid and ask:
                        prices[s] = {"bid": bid, "ask": ask}
                except Exception:
                    logger.exception("threaded_kucoin_prices: future error for %s", s)
    logger.debug("[KUCOIN_BATCH] fetched %d/%d", len(prices), len(symbols))
    return prices

def calculate_spread(bin_bid, bin_ask, ku_bid, ku_ask):
    try:
        if not all([bin_bid, bin_ask, ku_bid, ku_ask]) or bin_ask <= 0 or bin_bid <= 0:
            return None
        pos = ((ku_bid - bin_ask) / bin_ask) * 100
        neg = ((ku_ask - bin_bid) / bin_bid) * 100
        if pos > 0.01:
            return pos
        if neg < -0.01:
            return neg
        return None
    except Exception:
        logger.exception("calculate_spread error")
        return None

# ===================== TRADING: Case A & Case B (kept identical to original trading logic) =====================
# NOTE: Per request, trading logic inside Case A and Case B is not modified. These functions replicate original behavior.
def execute_caseA(bin_sym, kc_raw_sym, kc_ccxt_sym, trigger_time, bin_ask, kc_bid):
    """
    Case A: Binance ask < KuCoin bid -> Buy Binance, Sell KuCoin
    (Original trading logic preserved)
    """
    print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE A CONFIRMED  -> EXECUTING PARALLEL ORDERS for {bin_sym}/{kc_raw_sym}")

    notional_bin = NOTIONAL
    notional_kc = NOTIONAL

    results = {}
    def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_ccxt_sym, trigger_time=trigger_time, trigger_price=kc_bid)
    def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, bin_sym, trigger_time=trigger_time, trigger_price=bin_ask)
    t1 = threading.Thread(target=exec_kc)
    t2 = threading.Thread(target=exec_bin)
    t1.start(); t2.start(); t1.join(); t2.join()
    ok_kc, exec_price_kc, exec_time_kc = results.get('kc', (False, None, None))
    ok_bin, exec_price_bin, exec_time_bin = results.get('bin', (False, None, None))
    if ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None:
        try:
            implied_bin = compute_amount_for_notional(binance, bin_sym, notional_bin, exec_price_bin)[1]
        except Exception:
            implied_bin = 0.0
        try:
            implied_kc = compute_amount_for_notional(kucoin, kc_ccxt_sym, notional_kc, exec_price_kc)[1]
        except Exception:
            implied_kc = 0.0

        mismatch_pct = abs(implied_bin - implied_kc) / max(implied_bin, implied_kc) * 100 if max(implied_bin, implied_kc) > 0 else 100
        print(f"IMPLIED NOTIONALS | Binance: ${implied_bin:.6f} | KuCoin: ${implied_kc:.6f} | mismatch={mismatch_pct:.3f}%")

        if mismatch_pct <= MAX_NOTIONAL_MISMATCH_PCT:
            real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
            final_entry_spread = real_entry_spread if real_entry_spread >= (100 * (kc_bid - bin_ask) / bin_ask) else real_entry_spread
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Spread: Real({real_entry_spread:.3f}%) {'≥' if real_entry_spread >= (100 * (kc_bid - bin_ask) / bin_ask) else '<'} Trigger({(100 * (kc_bid - bin_ask) / bin_ask):.3f}%). Using {'Trigger' if real_entry_spread >= (100 * (kc_bid - bin_ask) / bin_ask) else 'Real'} Spread as profit basis.")
            entry_prices[bin_sym] = {'kucoin': exec_price_kc, 'binance': exec_price_bin}
            entry_actual[bin_sym] = {'kucoin': {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}, 'binance': {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}, 'trigger_time': trigger_time, 'trigger_price': {'binance': bin_ask, 'kucoin': kc_bid}}
            entry_spreads[bin_sym] = final_entry_spread
            positions[bin_sym] = 'caseA'
            trade_start_balances[bin_sym] = start_total_balance
            entry_confirm_count[bin_sym] = 0
            sl_kc = exec_price_kc - kc_bid
            sl_bin = exec_price_bin - bin_ask
            lat_kc = lat_bin = None
            try:
                t0_ms = int(trigger_time.timestamp() * 1000)
                if exec_time_kc:
                    lat_kc = int(datetime.fromisoformat(exec_time_kc.replace('Z', '')).timestamp() * 1000) - t0_ms
                if exec_time_bin:
                    lat_bin = int(datetime.fromisoformat(exec_time_bin.replace('Z', '')).timestamp() * 1000) - t0_ms
            except Exception:
                pass
            try:
                implied_bin_logged = compute_amount_for_notional(binance, bin_sym, notional_bin, exec_price_bin)[1]
                implied_kc_logged = compute_amount_for_notional(kucoin, kc_ccxt_sym, notional_kc, exec_price_kc)[1]
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} MATCHED NOTIONALS | bin_notional=${notional_bin:.6f} implied=${implied_bin_logged:.8f} | kc_notional=${notional_kc:.6f} implied=${implied_kc_logged:.8f}")
            except Exception:
                pass
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ENTRY SUMMARY | trigger_time={trigger_time.strftime('%H:%M:%S.%f')[:-3]} | trigger_prices bin:{bin_ask} kc:{kc_bid}")
            print(f" KuCoin executed: price={exec_price_kc} exec_time={exec_time_kc} slippage={sl_kc:.8f} latency_ms={lat_kc}")
            print(f" Binance executed: price={exec_price_bin} exec_time={exec_time_bin} slippage={sl_bin:.8f} latency_ms={lat_bin}")
            print(f" REAL Entry Spread: {real_entry_spread:.3f}% | PROFIT BASIS Spread: {final_entry_spread:.3f}%")

            try:
                _start_liquidation_watcher_for_symbol(bin_sym, bin_sym, kc_ccxt_sym)
            except Exception as e:
                print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")

        else:
            diff_dollars = abs(implied_bin - implied_kc)
            print(f"NOTIONAL MISMATCH {mismatch_pct:.3f}% -> diff ${diff_dollars:.6f}")
            if diff_dollars >= REBALANCE_MIN_DOLLARS:
                print("Attempting rebalance...")
                reb_ok = False
                reb_exec_price = None
                if implied_bin < implied_kc:
                    print("Rebalance -> BUY on Binance for diff")
                    reb_ok, reb_exec_price, _ = safe_create_order(binance, 'buy', diff_dollars, exec_price_bin, bin_sym)
                else:
                    print("Rebalance -> SELL on KuCoin for diff")
                    reb_ok, reb_exec_price, _ = safe_create_order(kucoin, 'sell', diff_dollars, exec_price_kc, kc_ccxt_sym)
                if reb_ok:
                    try:
                        added_bin = compute_amount_for_notional(binance, bin_sym, diff_dollars, reb_exec_price or exec_price_bin)[1]
                    except Exception:
                        added_bin = 0.0
                    try:
                        added_kc = compute_amount_for_notional(kucoin, kc_ccxt_sym, diff_dollars, reb_exec_price or exec_price_kc)[1]
                    except Exception:
                        added_kc = 0.0
                    new_implied_bin = implied_bin + added_bin
                    new_implied_kc = implied_kc + added_kc
                    new_mismatch = abs(new_implied_bin - new_implied_kc) / max(new_implied_bin, new_implied_kc) * 100 if max(new_implied_bin, new_implied_kc) > 0 else 100
                    print(f"Post-rebalance implieds | bin:${new_implied_bin:.6f} kc:${new_implied_kc:.6f} | mismatch={new_mismatch:.3f}%")
                    if new_mismatch <= MAX_NOTIONAL_MISMATCH_PCT:
                        real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                        final_entry_spread = real_entry_spread if real_entry_spread >= (100 * (kc_bid - bin_ask) / bin_ask) else real_entry_spread
                        entry_prices[bin_sym] = {'kucoin': exec_price_kc, 'binance': exec_price_bin}
                        entry_actual[bin_sym] = {'kucoin': {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}, 'binance': {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}, 'trigger_time': trigger_time, 'trigger_price': {'binance': bin_ask, 'kucoin': kc_bid}}
                        entry_spreads[bin_sym] = final_entry_spread
                        positions[bin_sym] = 'caseA'
                        trade_start_balances[bin_sym] = start_total_balance
                        entry_confirm_count[bin_sym] = 0
                        print("Rebalance succeeded — trade accepted and watcher will start.")
                        try:
                            _start_liquidation_watcher_for_symbol(bin_sym, bin_sym, kc_ccxt_sym)
                        except Exception as e:
                            print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
                    else:
                        print("Rebalance insufficient — closing both sides to avoid naked exposure")
                        close_all_and_wait()
                        entry_confirm_count[bin_sym] = 0
                else:
                    print("Rebalance order failed — closing both sides to avoid naked exposure")
                    close_all_and_wait()
                    entry_confirm_count[bin_sym] = 0
            else:
                print("Mismatch below REBALANCE_MIN_DOLLARS — accepting small residual exposure and proceeding.")
                real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                final_entry_spread = real_entry_spread if real_entry_spread >= (100 * (kc_bid - bin_ask) / bin_ask) else real_entry_spread
                entry_prices[bin_sym] = {'kucoin': exec_price_kc, 'binance': exec_price_bin}
                entry_actual[bin_sym] = {'kucoin': {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}, 'binance': {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}, 'trigger_time': trigger_time, 'trigger_price': {'binance': bin_ask, 'kucoin': kc_bid}}
                entry_spreads[bin_sym] = final_entry_spread
                positions[bin_sym] = 'caseA'
                trade_start_balances[bin_sym] = start_total_balance
                entry_confirm_count[bin_sym] = 0
                try:
                    _start_liquidation_watcher_for_symbol(bin_sym, bin_sym, kc_ccxt_sym)
                except Exception as e:
                    print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
    else:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} WARNING: Partial or failed execution in Case A. Closing positions if any.")
        close_all_and_wait()
        entry_confirm_count[bin_sym] = 0

def execute_caseB(bin_sym, kc_raw_sym, kc_ccxt_sym, trigger_time, bin_bid, kc_ask):
    """
    Case B: Binance bid > KuCoin ask -> Sell Binance, Buy KuCoin
    (Original trading logic preserved)
    """
    print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE B CONFIRMED  -> EXECUTING PARALLEL ORDERS for {bin_sym}/{kc_raw_sym}")

    notional_bin = NOTIONAL
    notional_kc = NOTIONAL

    results = {}
    def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_ccxt_sym, trigger_time=trigger_time, trigger_price=kc_ask)
    def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, bin_sym, trigger_time=trigger_time, trigger_price=bin_bid)
    t1 = threading.Thread(target=exec_kc)
    t2 = threading.Thread(target=exec_bin)
    t1.start(); t2.start(); t1.join(); t2.join()
    ok_kc, exec_price_kc, exec_time_kc = results.get('kc', (False, None, None))
    ok_bin, exec_price_bin, exec_time_bin = results.get('bin', (False, None, None))
    if ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None:
        try:
            implied_bin = compute_amount_for_notional(binance, bin_sym, notional_bin, exec_price_bin)[1]
        except Exception:
            implied_bin = 0.0
        try:
            implied_kc = compute_amount_for_notional(kucoin, kc_ccxt_sym, notional_kc, exec_price_kc)[1]
        except Exception:
            implied_kc = 0.0

        mismatch_pct = abs(implied_bin - implied_kc) / max(implied_bin, implied_kc) * 100 if max(implied_bin, implied_kc) > 0 else 100
        print(f"IMPLIED NOTIONALS | Binance: ${implied_bin:.6f} | KuCoin: ${implied_kc:.6f} | mismatch={mismatch_pct:.3f}%")

        if mismatch_pct <= MAX_NOTIONAL_MISMATCH_PCT:
            real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
            final_entry_spread = real_entry_spread if real_entry_spread >= (100 * (bin_bid - kc_ask) / kc_ask) else real_entry_spread
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Spread: Real({real_entry_spread:.3f}%) {'≥' if real_entry_spread >= (100 * (bin_bid - kc_ask) / kc_ask) else '<'} Trigger({(100 * (bin_bid - kc_ask) / kc_ask):.3f}%). Using {'Trigger' if real_entry_spread >= (100 * (bin_bid - kc_ask) / kc_ask) else 'Real'} Spread as profit basis.")
            entry_prices[bin_sym] = {'kucoin': exec_price_kc, 'binance': exec_price_bin}
            entry_actual[bin_sym] = {'kucoin': {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}, 'binance': {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}, 'trigger_time': trigger_time, 'trigger_price': {'binance': bin_bid, 'kucoin': kc_ask}}
            entry_spreads[bin_sym] = final_entry_spread
            positions[bin_sym] = 'caseB'
            trade_start_balances[bin_sym] = start_total_balance
            entry_confirm_count[bin_sym] = 0
            sl_kc = exec_price_kc - kc_ask
            sl_bin = exec_price_bin - bin_bid
            lat_kc = lat_bin = None
            try:
                t0_ms = int(trigger_time.timestamp() * 1000)
                if exec_time_kc:
                    lat_kc = int(datetime.fromisoformat(exec_time_kc.replace('Z', '')).timestamp() * 1000) - t0_ms
                if exec_time_bin:
                    lat_bin = int(datetime.fromisoformat(exec_time_bin.replace('Z', '')).timestamp() * 1000) - t0_ms
            except Exception:
                pass
            try:
                implied_bin_logged = compute_amount_for_notional(binance, bin_sym, notional_bin, exec_price_bin)[1]
                implied_kc_logged = compute_amount_for_notional(kucoin, kc_ccxt_sym, notional_kc, exec_price_kc)[1]
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} MATCHED NOTIONALS | bin_notional=${notional_bin:.6f} implied=${implied_bin_logged:.8f} | kc_notional=${notional_kc:.6f} implied=${implied_kc_logged:.8f}")
            except Exception:
                pass
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ENTRY SUMMARY | trigger_time={trigger_time.strftime('%H:%M:%S.%f')[:-3]} | trigger_prices bin:{bin_bid} kc:{kc_ask}")
            print(f" KuCoin executed: price={exec_price_kc} exec_time={exec_time_kc} slippage={sl_kc:.8f} latency_ms={lat_kc}")
            print(f" Binance executed: price={exec_price_bin} exec_time={exec_time_bin} slippage={sl_bin:.8f} latency_ms={lat_bin}")
            print(f" REAL Entry Spread: {real_entry_spread:.3f}% | PROFIT BASIS Spread: {final_entry_spread:.3f}%")

            try:
                _start_liquidation_watcher_for_symbol(bin_sym, bin_sym, kc_ccxt_sym)
            except Exception as e:
                print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")

        else:
            diff_dollars = abs(implied_bin - implied_kc)
            print(f"NOTIONAL MISMATCH {mismatch_pct:.3f}% -> diff ${diff_dollars:.6f}")
            if diff_dollars >= REBALANCE_MIN_DOLLARS:
                print("Attempting rebalance...")
                reb_ok = False
                reb_exec_price = None
                if implied_bin < implied_kc:
                    print("Rebalance -> SELL on Binance for diff")
                    reb_ok, reb_exec_price, _ = safe_create_order(binance, 'sell', diff_dollars, exec_price_bin, bin_sym)
                else:
                    print("Rebalance -> BUY on KuCoin for diff")
                    reb_ok, reb_exec_price, _ = safe_create_order(kucoin, 'buy', diff_dollars, exec_price_kc, kc_ccxt_sym)
                if reb_ok:
                    try:
                        added_bin = compute_amount_for_notional(binance, bin_sym, diff_dollars, reb_exec_price or exec_price_bin)[1]
                    except Exception:
                        added_bin = 0.0
                    try:
                        added_kc = compute_amount_for_notional(kucoin, kc_ccxt_sym, diff_dollars, reb_exec_price or exec_price_kc)[1]
                    except Exception:
                        added_kc = 0.0
                    new_implied_bin = implied_bin + added_bin
                    new_implied_kc = implied_kc + added_kc
                    new_mismatch = abs(new_implied_bin - new_implied_kc) / max(new_implied_bin, new_implied_kc) * 100 if max(new_implied_bin, new_implied_kc) > 0 else 100
                    print(f"Post-rebalance implieds | bin:${new_implied_bin:.6f} kc:${new_implied_kc:.6f} | mismatch={new_mismatch:.3f}%")
                    if new_mismatch <= MAX_NOTIONAL_MISMATCH_PCT:
                        real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                        final_entry_spread = real_entry_spread if real_entry_spread >= (100 * (bin_bid - kc_ask) / kc_ask) else real_entry_spread
                        entry_prices[bin_sym] = {'kucoin': exec_price_kc, 'binance': exec_price_bin}
                        entry_actual[bin_sym] = {'kucoin': {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}, 'binance': {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}, 'trigger_time': trigger_time, 'trigger_price': {'binance': bin_bid, 'kucoin': kc_ask}}
                        entry_spreads[bin_sym] = final_entry_spread
                        positions[bin_sym] = 'caseB'
                        trade_start_balances[bin_sym] = start_total_balance
                        entry_confirm_count[bin_sym] = 0
                        print("Rebalance succeeded — trade accepted and watcher will start.")
                        try:
                            _start_liquidation_watcher_for_symbol(bin_sym, bin_sym, kc_ccxt_sym)
                        except Exception as e:
                            print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
                    else:
                        print("Rebalance insufficient — closing both sides to avoid naked exposure")
                        close_all_and_wait()
                        entry_confirm_count[bin_sym] = 0
                else:
                    print("Rebalance order failed — closing both sides to avoid naked exposure")
                    close_all_and_wait()
                    entry_confirm_count[bin_sym] = 0
            else:
                print("Mismatch below REBALANCE_MIN_DOLLARS — accepting small residual exposure and proceeding.")
                real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                final_entry_spread = real_entry_spread if real_entry_spread >= (100 * (bin_bid - kc_ask) / kc_ask) else real_entry_spread
                entry_prices[bin_sym] = {'kucoin': exec_price_kc, 'binance': exec_price_bin}
                entry_actual[bin_sym] = {'kucoin': {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}, 'binance': {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}, 'trigger_time': trigger_time, 'trigger_price': {'binance': bin_bid, 'kucoin': kc_ask}}
                entry_spreads[bin_sym] = final_entry_spread
                positions[bin_sym] = 'caseB'
                trade_start_balances[bin_sym] = start_total_balance
                entry_confirm_count[bin_sym] = 0
                try:
                    _start_liquidation_watcher_for_symbol(bin_sym, bin_sym, kc_ccxt_sym)
                except Exception as e:
                    print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
    else:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} WARNING: Partial or failed execution in Case B. Closing positions if any.")
        close_all_and_wait()
        entry_confirm_count[bin_sym] = 0

# ===================== EXIT MONITOR (keeps original exit logic) =====================
def exit_monitor_loop():
    print("Exit monitor thread started.")
    session = requests.Session()
    while True:
        try:
            if terminate_bot:
                print("Exit monitor: termination requested, exiting monitor loop.")
                break
            if not TRADED_BINANCE_SYMBOLS:
                time.sleep(0.5)
                continue
            bin_symbols = list(set(TRADED_BINANCE_SYMBOLS))
            ku_raw_symbols = [KUCOIN_RAW_MAP.get(sym) for sym in bin_symbols if KUCOIN_RAW_MAP.get(sym)]
            bin_prices, kc_prices = get_prices_for_symbols(bin_symbols, ku_raw_symbols)
            for sym in list(bin_symbols):
                try:
                    if positions.get(sym) is None:
                        continue
                    kc_raw = KUCOIN_RAW_MAP.get(sym)
                    kc_ccxt = KUCOIN_CCXT_MAP.get(sym)
                    bin_tick = bin_prices.get(sym)
                    kc_tick = kc_prices.get(kc_raw) if kc_raw else None
                    if not bin_tick or not kc_tick:
                        continue
                    bin_bid, bin_ask = bin_tick
                    kc_bid, kc_ask = kc_tick
                    if positions.get(sym) == 'caseA':
                        current_exit_spread = 100 * (kc_ask - bin_bid) / entry_prices[sym]['binance']
                        captured = entry_spreads[sym] - current_exit_spread
                        current_entry_spread = 100 * (kc_bid - bin_ask) / bin_ask
                    elif positions.get(sym) == 'caseB':
                        current_exit_spread = 100 * (bin_bid - kc_ask) / entry_prices[sym]['kucoin']
                        captured = entry_spreads[sym] - current_exit_spread
                        current_entry_spread = 100 * (bin_bid - kc_ask) / kc_ask
                    else:
                        continue

                    print(f"{datetime.now().strftime('%H:%M:%S')} POSITION OPEN {sym} | Entry Spread (Trigger): {current_entry_spread:.3f}% | Entry Basis: {entry_spreads[sym]:.3f}% | Exit Spread: {current_exit_spread:.3f}% | Captured: {captured:.3f}% | Exit Confirm: {exit_confirm_count.get(sym,0) + (1 if (captured >= PROFIT_TARGET or abs(current_exit_spread) < 0.02) else 0)}/3")
                    exit_condition = captured >= PROFIT_TARGET or abs(current_exit_spread) < 0.02
                    if exit_condition:
                        exit_confirm_count[sym] = exit_confirm_count.get(sym, 0) + 1
                        if exit_confirm_count[sym] >= 3:
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} EXIT TRIGGERED 3/3 | Captured: {captured:.3f}% | Current spread: {current_exit_spread:.3f}% | Case: {positions[sym].upper()}")
                            try:
                                et = entry_actual[sym].get('trigger_time')
                                tp = entry_actual[sym].get('trigger_price')
                                print(f" ENTRY TRIGGER TIME: {et.strftime('%H:%M:%S.%f')[:-3] if et else 'N/A'} | trigger_prices: {tp}")
                                print(f" ENTRY EXECUTED DETAILS: binance={entry_actual[sym].get('binance')} kucoin={entry_actual[sym].get('kucoin')}")
                            except Exception:
                                pass
                            close_all_and_wait()
                            positions[sym] = None
                            entry_confirm_count[sym] = 0
                            exit_confirm_count[sym] = 0
                            try:
                                if sym in TRADED_BINANCE_SYMBOLS:
                                    TRADED_BINANCE_SYMBOLS.remove(sym)
                                KUCOIN_RAW_MAP.pop(sym, None)
                                KUCOIN_CCXT_MAP.pop(sym, None)
                                entry_prices.pop(sym, None)
                                entry_actual.pop(sym, None)
                                entry_spreads.pop(sym, None)
                                trade_start_balances.pop(sym, None)
                            except Exception:
                                pass
                        else:
                            print(f"{datetime.now().strftime('%H:%M:%S')} → Exit condition met, confirming {exit_confirm_count[sym]}/3...")
                    else:
                        exit_confirm_count[sym] = 0
                except Exception as e:
                    print("Exit monitor per-symbol error:", e)
            time.sleep(0.1)
        except Exception:
            logger.exception("Fatal error in exit monitor loop, sleeping briefly")
            time.sleep(1)

# ===================== SCANNER MAIN (now uses original 3x consecutive confirms + single-position guard) =====================
start_total_balance, start_bin_balance, start_kc_balance = get_total_futures_balance()
print(f"Starting total balance approx: ${start_total_balance:.2f} (Binance: ${start_bin_balance:.2f} | KuCoin: ${start_kc_balance:.2f})\n")
print(f"{datetime.now()} INTEGRATED BOT STARTED\n")

_exit_thread = threading.Thread(target=exit_monitor_loop, daemon=True)
_exit_thread.start()

def scanner_main_loop():
    last_alert = {}
    heartbeat_counter = 0
    http_session = requests.Session()

    while True:
        window_start = time.time()
        try:
            common_symbols, ku_map = get_common_symbols()
            if not common_symbols:
                logger.warning("No common symbols — retrying after short sleep")
                time.sleep(5)
                continue

            bin_book = get_binance_book()
            ku_symbols = [ku_map.get(sym, sym + "M") for sym in common_symbols]
            ku_prices = threaded_kucoin_prices(ku_symbols)

            # prepare and shortlist candidates (unique symbols only)
            candidates = {}
            for sym in common_symbols:
                bin_tick = bin_book.get(sym)
                ku_sym = ku_map.get(sym, sym + "M")
                ku_tick = ku_prices.get(ku_sym)
                if not bin_tick or not ku_tick:
                    continue
                spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], ku_tick["bid"], ku_tick["ask"])
                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    candidates[sym] = {
                        "ku_sym": ku_sym,
                        "max_spread": spread,
                        "min_spread": spread,
                        "last_bin": bin_tick,
                        "last_ku": ku_tick
                    }

            logger.info("[%s] Start window: shortlisted %d candidate(s): %s", timestamp(), len(candidates), list(candidates.keys())[:12])

            if not candidates:
                elapsed = time.time() - window_start
                to_sleep = max(1, MONITOR_DURATION - elapsed)
                logger.info("No candidates this minute — sleeping %.1fs before next full scan", to_sleep)
                time.sleep(to_sleep)
                continue

            # Initialize per-candidate entry_confirm_count if missing
            for sym in candidates.keys():
                entry_confirm_count.setdefault(sym, 0)
                exit_confirm_count.setdefault(sym, 0)
                positions.setdefault(sym, None)
                entry_prices.setdefault(sym, {'binance': 0, 'kucoin': 0})
                entry_actual.setdefault(sym, {'binance': None, 'kucoin': None, 'trigger_time': None, 'trigger_price': None})
                entry_spreads.setdefault(sym, 0.0)
                trade_start_balances.setdefault(sym, 0.0)

            # Focused monitoring for MONITOR_DURATION seconds using original 3x consecutive confirm logic
            window_end = window_start + MONITOR_DURATION
            while time.time() < window_end and candidates:
                round_start = time.time()

                # Fetch latest prices for each candidate in parallel
                workers = min(MAX_WORKERS, max(4, len(candidates)))
                latest = {s: {"bin": None, "ku": None} for s in list(candidates.keys())}

                with ThreadPoolExecutor(max_workers=workers) as ex:
                    fut_map = {}
                    for sym, info in list(candidates.items()):
                        ku_sym = info["ku_sym"]
                        b_symbol = sym
                        fut_map[ex.submit(get_binance_price, b_symbol, http_session)] = ("bin", sym)
                        fut_map[ex.submit(get_kucoin_price_once, ku_sym, http_session)] = ("ku", sym)

                    for fut in as_completed(fut_map):
                        typ, sym = fut_map[fut]
                        try:
                            bid, ask = fut.result()
                        except Exception:
                            bid, ask = None, None
                        if bid and ask:
                            latest[sym][typ] = {"bid": bid, "ask": ask}

                # Evaluate each candidate and apply original entry-confirm logic + single-position guard
                for sym in list(candidates.keys()):
                    info = candidates.get(sym)
                    if not info:
                        continue
                    b = latest[sym].get("bin")
                    k = latest[sym].get("ku")
                    if not b or not k:
                        # If prices missing, do not change confirmation count (stable approach)
                        continue

                    bin_bid, bin_ask = b['bid'], b['ask']
                    kc_bid, kc_ask = k['bid'], k['ask']

                    # Reset confirmation counter if spread condition breaks (as original)
                    if bin_ask < kc_bid:
                        trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                        if trigger_spread < ENTRY_SPREAD:
                            entry_confirm_count[sym] = 0
                    elif bin_bid > kc_ask:
                        trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                        if trigger_spread < ENTRY_SPREAD:
                            entry_confirm_count[sym] = 0
                    else:
                        # neither side; reset
                        entry_confirm_count[sym] = 0
                        continue

                    # CASE A: Binance ask < KuCoin bid (Buy Binance, Sell KuCoin)
                    if bin_ask < kc_bid:
                        trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                        logger.info("CASE A %s | Trigger Spread: %.3f%% | Confirm: %d/3", sym, trigger_spread, entry_confirm_count[sym] + 1)
                        # Only attempt entry if no position open anywhere and this symbol not currently marked as having a position
                        if positions.get(sym) is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions():
                            entry_confirm_count[sym] += 1
                            if entry_confirm_count[sym] >= 3:
                                # EXACT original pre-trade actions (kept)
                                total_bal, bin_bal, kc_bal = get_total_futures_balance()
                                print(f"*** PRE-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
                                trigger_time = datetime.utcnow()
                                entry_actual[sym]['trigger_time'] = trigger_time
                                entry_actual[sym]['trigger_price'] = {'binance': bin_ask, 'kucoin': kc_bid}
                                print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE A CONFIRMED 3/3 -> EXECUTING PARALLEL ORDERS for {sym}")

                                # Resolve kucoin ccxt symbol
                                kc_ccxt = resolve_kucoin_trade_symbol(kucoin, info["ku_sym"])
                                if not kc_ccxt:
                                    logger.warning("Could not resolve KuCoin ccxt symbol for %s (raw %s) - skipping", sym, info["ku_sym"])
                                    entry_confirm_count[sym] = 0
                                    candidates.pop(sym, None)
                                    continue

                                # Prepare TRADED lists and set leverage (idempotent)
                                if sym not in TRADED_BINANCE_SYMBOLS:
                                    TRADED_BINANCE_SYMBOLS.append(sym)
                                KUCOIN_RAW_MAP[sym] = info["ku_sym"]
                                KUCOIN_CCXT_MAP[sym] = kc_ccxt
                                set_leverage_and_margin_for_symbol(sym, kc_ccxt)

                                # CALL original trading logic for Case A (unchanged)
                                try:
                                    execute_caseA(sym, info["ku_sym"], kc_ccxt, trigger_time, bin_ask, kc_bid)
                                except Exception as e:
                                    logger.exception("Case A execution error for %s: %s", sym, e)
                                    try:
                                        close_all_and_wait()
                                    except Exception:
                                        pass
                                # remove symbol from monitoring for remainder of minute
                                candidates.pop(sym, None)
                        else:
                            # If condition does not hold (e.g. position open elsewhere), do not increment and reset to 0
                            if not (positions.get(sym) is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions()):
                                entry_confirm_count[sym] = 0

                    # CASE B: Binance bid > KuCoin ask (Sell Binance, Buy KuCoin)
                    elif bin_bid > kc_ask:
                        trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                        logger.info("CASE B %s | Trigger Spread: %.3f%% | Confirm: %d/3", sym, trigger_spread, entry_confirm_count[sym] + 1)
                        if positions.get(sym) is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions():
                            entry_confirm_count[sym] += 1
                            if entry_confirm_count[sym] >= 3:
                                total_bal, bin_bal, kc_bal = get_total_futures_balance()
                                print(f"*** PRE-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
                                trigger_time = datetime.utcnow()
                                entry_actual[sym]['trigger_time'] = trigger_time
                                entry_actual[sym]['trigger_price'] = {'binance': bin_bid, 'kucoin': kc_ask}
                                print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE B CONFIRMED 3/3 -> EXECUTING PARALLEL ORDERS for {sym}")

                                kc_ccxt = resolve_kucoin_trade_symbol(kucoin, info["ku_sym"])
                                if not kc_ccxt:
                                    logger.warning("Could not resolve KuCoin ccxt symbol for %s (raw %s) - skipping", sym, info["ku_sym"])
                                    entry_confirm_count[sym] = 0
                                    candidates.pop(sym, None)
                                    continue

                                if sym not in TRADED_BINANCE_SYMBOLS:
                                    TRADED_BINANCE_SYMBOLS.append(sym)
                                KUCOIN_RAW_MAP[sym] = info["ku_sym"]
                                KUCOIN_CCXT_MAP[sym] = kc_ccxt
                                set_leverage_and_margin_for_symbol(sym, kc_ccxt)

                                try:
                                    execute_caseB(sym, info["ku_sym"], kc_ccxt, trigger_time, bin_bid, kc_ask)
                                except Exception as e:
                                    logger.exception("Case B execution error for %s: %s", sym, e)
                                    try:
                                        close_all_and_wait()
                                    except Exception:
                                        pass
                                candidates.pop(sym, None)
                        else:
                            if not (positions.get(sym) is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions()):
                                entry_confirm_count[sym] = 0

                # sleep until next poll round (but cap to window_end)
                elapsed = time.time() - round_start
                sleep_for = MONITOR_POLL - elapsed
                if sleep_for > 0:
                    if time.time() + sleep_for > window_end:
                        sleep_for = max(0, window_end - time.time())
                    if sleep_for > 0:
                        time.sleep(sleep_for)

            # End of minute summary (optional)
            overall_max = None; overall_max_sym = None
            overall_min = None; overall_min_sym = None
            for sym, info in candidates.items():
                if overall_max is None or info["max_spread"] > overall_max:
                    overall_max, overall_max_sym = info["max_spread"], sym
                if overall_min is None or info["min_spread"] < overall_min:
                    overall_min, overall_min_sym = info["min_spread"], sym

            logger.info("Minute summary: candidates monitored %d", len(candidates))
            if overall_max_sym:
                logger.info("Max +ve -> %s: +%.4f%%", overall_max_sym, overall_max)
            if overall_min_sym:
                logger.info("Max -ve -> %s: %.4f%%", overall_min_sym, overall_min)

            elapsed_total = time.time() - window_start
            if elapsed_total < MONITOR_DURATION:
                time.sleep(max(0.2, MONITOR_DURATION - elapsed_total))

            heartbeat_counter += 1
            if heartbeat_counter % 20 == 0:
                logger.info("Scanner alive — %s", timestamp())

        except KeyboardInterrupt:
            print("Stopping integrated bot (KeyboardInterrupt)...")
            try:
                close_all_and_wait()
            except Exception as e:
                print("Error during graceful shutdown close:", e)
            break

        except Exception:
            logger.exception("Fatal error in scanner main loop, sleeping briefly before retry")
            time.sleep(5)

if __name__ == "__main__":
    try:
        scanner_main_loop()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    except Exception:
        logger.exception("Unhandled exception at top level")

