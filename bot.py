#!/usr/bin/env python3
# MULTI-COIN 4x LIVE ARB BOT – Single coin TRUSTUSDT
import os, sys, time, math, requests
from datetime import datetime
from dotenv import load_dotenv
import ccxt
import threading
load_dotenv()

REQUIRED = ['BINANCE_API_KEY','BINANCE_API_SECRET','KUCOIN_API_KEY','KUCOIN_API_SECRET','KUCOIN_API_PASSPHRASE']
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print(f"ERROR: Missing .env keys: {', '.join(missing)}")
    sys.exit(1)

# CONFIG
SYMBOLS = ["AIAUSDT"]
KUCOIN_SYMBOLS = ["AIAUSDTM"]
NOTIONAL = 10.0
LEVERAGE = 5
ENTRY_SPREAD = 1.0
PROFIT_TARGET = 0.05
MARGIN_BUFFER = 1.02

print(f"\n{'='*72}")
print(f"SINGLE COIN 4x LIVE ARB BOT | NOTIONAL ${NOTIONAL} @ {LEVERAGE}x | ENTRY >= {ENTRY_SPREAD}% | PROFIT TARGET {PROFIT_TARGET}%")
print(f"Tracking symbol: {SYMBOLS[0]}")
print(f"{'='*72}\n")

# Exchanges
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

# Helpers (mostly unchanged)
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

KUCOIN_TRADE_SYMBOLS = [resolve_kucoin_trade_symbol(kucoin, k) for k in KUCOIN_SYMBOLS]

# Set leverage and margin mode CROSS for both exchanges
def set_leverage_and_margin():
    for i, sym in enumerate(SYMBOLS):
        try:
            binance.set_leverage(LEVERAGE, sym)
            print(f"Binance leverage set to {LEVERAGE}x for {sym}")
        except Exception as e:
            print(f"Binance leverage error for {sym}: {e}")
        kc_sym = KUCOIN_TRADE_SYMBOLS[i]
        if kc_sym:
            try:
                kucoin.set_leverage(LEVERAGE, kc_sym, {'marginMode':'cross'})
                print(f"KuCoin leverage set to {LEVERAGE}x CROSS for {kc_sym}")
            except Exception as e:
                print(f"KuCoin leverage error for {kc_sym}: {e}")

ensure_markets_loaded()
set_leverage_and_margin()

# State
closing_in_progress = False
positions = {s: None for s in SYMBOLS}
trade_start_balances = {s: 0.0 for s in SYMBOLS}
entry_spreads = {s: 0.0 for s in SYMBOLS}
entry_prices = {s: {'binance': 0, 'kucoin': 0} for s in SYMBOLS}
entry_actual = {s: {'binance': None, 'kucoin': None, 'trigger_time': None, 'trigger_price': None} for s in SYMBOLS}
entry_confirm_count = {s: 0 for s in SYMBOLS}
exit_confirm_count = {s: 0 for s in SYMBOLS}

# --- NEW: Get Total Futures Balance in USDT ---
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
        for sym in SYMBOLS:
            pos = binance.fetch_positions([sym])
            if pos and abs(float(pos[0].get('positionAmt') or pos[0].get('contracts') or 0)) > 0:
                return True
        for kc_sym in KUCOIN_TRADE_SYMBOLS:
            if not kc_sym: continue
            pos = kucoin.fetch_positions([kc_sym])
            if pos:
                raw = None
                try:
                    raw = pos[0].get('contracts') or pos[0].get('positionAmt') or 0
                except Exception:
                    raw = 0
                try:
                    if (not raw or float(raw) == 0) and isinstance(pos[0].get('info'), dict):
                        info = pos[0].get('info') or {}
                        if 'currentQty' in info:
                            raw = info.get('currentQty')
                        elif 'data' in info and isinstance(info.get('data'), dict) and 'currentQty' in info.get('data'):
                            raw = info.get('data', {}).get('currentQty')
                except Exception:
                    pass
                try:
                    if raw is None:
                        raw = 0
                    if abs(float(raw)) > 0:
                        return True
                except Exception:
                    pass
        return False
    except Exception:
        return True

# --- ROBUST signed-qty extractor for debug & reliable closes (FIXED) ---
def _get_signed_position_amount(pos):
    """
    Robustly extract a signed position amount from a ccxt position structure.
    Prioritizes truly signed fields (positionAmt) over unsigned 'contracts' to preserve sign.
    Returns a float (positive for long, negative for short) or 0.0 if not found.
    Extensive logs included to pinpoint parsing decisions.
    """
    info = pos.get('info') or {}

    # 1) Prefer signed fields from pos.info (these are authoritative for sign)
    for fld in ('positionAmt', 'positionAmt', 'currentQty', 'currentQty', 'amount'):
        v = info.get(fld)
        if v not in (None, ''):
            try:
                fv = float(v)
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Parsed signed qty from pos.info.{fld}: {fv}")
                return fv
            except Exception:
                try:
                    fv = float(str(v).replace(',', ''))
                    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Parsed signed qty (cleanup) from pos.info.{fld}: {fv}")
                    return fv
                except Exception:
                    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Could not parse pos.info.{fld} val={v}")

    # 2) Then check top-level signed fields (pos.positionAmt etc.)
    for fld in ('positionAmt', 'amount', 'position_amount'):
        v = pos.get(fld)
        if v not in (None, ''):
            try:
                fv = float(v)
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Parsed signed qty from pos.{fld}: {fv}")
                return fv
            except Exception:
                try:
                    fv = float(str(v).replace(',', ''))
                    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Parsed signed qty (cleanup) from pos.{fld}: {fv}")
                    return fv
                except Exception:
                    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Could not parse pos.{fld} val={v}")

    # 3) Fallback: if no signed fields, attempt to infer sign using side fields and use unsigned magnitude (contracts/size)
    magnitude = 0.0
    for k in ('contracts', 'size', 'amount'):
        v = pos.get(k)
        if v not in (None, ''):
            try:
                magnitude = float(v)
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Fallback magnitude from pos.{k}: {magnitude}")
                break
            except Exception:
                pass
        vinfo = info.get(k)
        if vinfo not in (None, ''):
            try:
                magnitude = float(vinfo)
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Fallback magnitude from pos.info.{k}: {magnitude}")
                break
            except Exception:
                pass

    # Check side fields to infer sign
    side_field = ''
    for candidate in (pos.get('side'), info.get('side'), info.get('positionSide'), info.get('type')):
        if candidate:
            side_field = str(candidate).lower()
            break
    if side_field:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Inferred side field: {side_field}")
    if side_field in ('short', 'sell', 'shortside', 'sell_long'):
        return -abs(magnitude)
    if side_field in ('long', 'buy'):
        return abs(magnitude)

    # Last resort: return unsigned magnitude (best-effort) but warn in logs
    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Unable to determine signed qty precisely, returning unsigned magnitude: {magnitude}")
    return float(magnitude or 0.0)

# --- FIXED close_all_and_wait: robust Binance close + verbose logs (only this section changed) ---
def close_all_and_wait(timeout_s=20, poll_interval=0.5):
    global closing_in_progress
    closing_in_progress = True
    print("\n" + "="*72)
    print("Closing all positions...")
    print("="*72)

    # Determine which case we are in (unchanged logic)
    current_case = None
    for sym in SYMBOLS:
        if positions.get(sym) == 'caseA':
            current_case = 'caseA'
        elif positions.get(sym) == 'caseB':
            current_case = 'caseB'

    for sym in SYMBOLS:
        # BINANCE close logic (robust with detailed logs)
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
                side = 'sell' if raw_signed > 0 else 'buy'  # long->sell to close, short->buy to close
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance side to close for {sym}: {side}")
                market = get_market(binance, sym)
                prec = market.get('precision', {}).get('amount') if market else None
                qty = round_down(abs(raw_signed), prec) if prec is not None else abs(raw_signed)
                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance qty to close for {sym}: {qty} (precision={prec})")

                # Attempt qty-based reduceOnly close first
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
                    # qty <= 0 (precision or rounding) — use closePosition=True
                    try:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} qty<=0, using closePosition=True for Binance {sym}")
                        try:
                            binance.create_order(symbol=sym, type='market', side=side, amount=None, params={'closePosition': True})
                        except TypeError:
                            binance.create_order(symbol=sym, type='market', side=side, params={'closePosition': True})
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE closePosition submitted for {sym}")
                    except Exception as e:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE closePosition failed for {sym}: {e}")

        # Give exchange a brief moment
        time.sleep(0.15)

        # KuCoin close (verbose logs)
        all_kc_positions = []
        try:
            all_kc_positions = kucoin.fetch_positions(symbols=KUCOIN_TRADE_SYMBOLS)
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
                    raw_qty_signed = float(pos.get('info').get('currentQty') or 0)
                if abs(raw_qty_signed) == 0:
                    size = float(pos.get('size') or pos.get('contracts') or pos.get('positionAmt') or 0)
                    if size > 0:
                        side_norm = pos.get('side')
                        raw_qty_signed = size * (-1 if side_norm == 'short' else 1)
                if abs(raw_qty_signed) == 0:
                    continue
                if ccxt_sym not in KUCOIN_TRADE_SYMBOLS:
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

    # Wait for positions to close, with periodic, verbose checks
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

# --- NEW helper: robust executed price/time extractor ---
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

# safe_create_order extended to capture executed price/time, slippage & latency and log them
def safe_create_order(exchange, side, notional, price, symbol, trigger_time=None, trigger_price=None):
    amt, _, _, prec = compute_amount_for_notional(exchange, symbol, notional, price)
    amt = round_down(amt, prec) if prec is not None else amt
    if amt <= 0:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} computed amt <=0, skipping order for {exchange.id} {symbol}")
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

# -------------------- NEW: match exposure by BASE TOKEN --------------------
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

def get_prices():
    bin_prices = {}
    kc_prices = {}
    try:
        data = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
        for s in SYMBOLS:
            for item in data:
                if item['symbol'] == s:
                    bin_prices[s] = (float(item['bidPrice']), float(item['askPrice']))
                    break
        for raw_id in KUCOIN_SYMBOLS:
            resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={raw_id}", timeout=5).json()
            d = resp.get('data', {})
            kc_prices[raw_id] = (float(d.get('bestBidPrice', '0') or 0), float(d.get('bestAskPrice', '0') or 0))
    except Exception:
        pass
    return bin_prices, kc_prices

# Starting balance
start_total_balance, start_bin_balance, start_kc_balance = get_total_futures_balance()
print(f"Starting total balance approx: ${start_total_balance:.2f} (Binance: ${start_bin_balance:.2f} | KuCoin: ${start_kc_balance:.2f})\n")
print(f"{datetime.now()} BOT STARTED – monitoring {SYMBOLS} / {KUCOIN_SYMBOLS}\n")

while True:
    try:
        bin_prices, kc_prices = get_prices()
        for i, sym in enumerate(SYMBOLS):
            bin_bid, bin_ask = bin_prices.get(sym, (None, None))
            kc_bid, kc_ask = kc_prices.get(KUCOIN_SYMBOLS[i], (None, None))
            kc_trade_sym = KUCOIN_TRADE_SYMBOLS[i]
            if None in (bin_bid, bin_ask, kc_bid, kc_ask) or not kc_trade_sym:
                continue

            # Reset confirmation counters if spread condition breaks
            if bin_ask < kc_bid:
                trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                if trigger_spread < ENTRY_SPREAD:
                    entry_confirm_count[sym] = 0
            elif bin_bid > kc_ask:
                trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                if trigger_spread < ENTRY_SPREAD:
                    entry_confirm_count[sym] = 0

            # CASE A: Binance ask < KuCoin bid (Buy Binance, Sell KuCoin)
            if bin_ask < kc_bid:
                trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                print(f"{datetime.now().strftime('%H:%M:%S')} CASE A | Trigger Spread: {trigger_spread:.3f}% | Confirm: {entry_confirm_count[sym] + 1}/3")
                if positions[sym] is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions():
                    entry_confirm_count[sym] += 1
                    if entry_confirm_count[sym] >= 3:
                        total_bal, bin_bal, kc_bal = get_total_futures_balance()
                        print(f"*** PRE-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
                        trigger_time = datetime.utcnow()
                        entry_actual[sym]['trigger_time'] = trigger_time
                        entry_actual[sym]['trigger_price'] = {'binance': bin_ask, 'kucoin': kc_bid}
                        print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE A CONFIRMED 3/3 -> EXECUTING PARALLEL ORDERS")

                        notional_bin, notional_kc, bin_base_amt, kc_contracts = match_base_exposure_per_exchange(
                            binance, kucoin, sym, kc_trade_sym, NOTIONAL, bin_ask, kc_bid
                        )
                        results = {}
                        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_trade_sym, trigger_time=trigger_time, trigger_price=kc_bid)
                        def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, sym, trigger_time=trigger_time, trigger_price=bin_ask)
                        t1 = threading.Thread(target=exec_kc)
                        t2 = threading.Thread(target=exec_bin)
                        t1.start(); t2.start(); t1.join(); t2.join()
                        ok_kc, exec_price_kc, exec_time_kc = results.get('kc', (False, None, None))
                        ok_bin, exec_price_bin, exec_time_bin = results.get('bin', (False, None, None))
                        if ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None:
                            real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                            final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Spread: Real({real_entry_spread:.3f}%) {'≥' if real_entry_spread >= trigger_spread else '<'} Trigger({trigger_spread:.3f}%). Using {'Trigger' if real_entry_spread >= trigger_spread else 'Real'} Spread as profit basis.")
                            entry_prices[sym]['kucoin'] = exec_price_kc
                            entry_prices[sym]['binance'] = exec_price_bin
                            entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}
                            entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}
                            entry_spreads[sym] = final_entry_spread
                            positions[sym] = 'caseA'
                            trade_start_balances[sym] = start_total_balance
                            entry_confirm_count[sym] = 0
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
                                implied_bin = compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)[1]
                                implied_kc = compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)[1]
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} MATCHED BASE TARGET | bin_base_amount={bin_base_amt} | kc_contracts={kc_contracts}")
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} MATCHED NOTIONALS | bin_notional=${notional_bin:.6f} implied=${implied_bin:.8f} | kc_notional=${notional_kc:.6f} implied=${implied_kc:.8f}")
                            except Exception:
                                pass
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ENTRY SUMMARY | trigger_time={trigger_time.strftime('%H:%M:%S.%f')[:-3]} | trigger_prices bin:{bin_ask} kc:{kc_bid}")
                            print(f" KuCoin executed: price={exec_price_kc} exec_time={exec_time_kc} slippage={sl_kc:.8f} latency_ms={lat_kc}")
                            print(f" Binance executed: price={exec_price_bin} exec_time={exec_time_bin} slippage={sl_bin:.8f} latency_ms={lat_bin}")
                            print(f" REAL Entry Spread: {real_entry_spread:.3f}% | PROFIT BASIS Spread: {final_entry_spread:.3f}%")
                        else:
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} WARNING: Partial or failed execution in Case A. Closing positions if any.")
                            close_all_and_wait()
                            entry_confirm_count[sym] = 0

            # CASE B: Binance bid > KuCoin ask (Sell Binance, Buy KuCoin)
            elif bin_bid > kc_ask:
                trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                print(f"{datetime.now().strftime('%H:%M:%S')} CASE B | Trigger Spread: {trigger_spread:.3f}% | Confirm: {entry_confirm_count[sym] + 1}/3")
                if positions[sym] is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions():
                    entry_confirm_count[sym] += 1
                    if entry_confirm_count[sym] >= 3:
                        total_bal, bin_bal, kc_bal = get_total_futures_balance()
                        print(f"*** PRE-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
                        trigger_time = datetime.utcnow()
                        entry_actual[sym]['trigger_time'] = trigger_time
                        entry_actual[sym]['trigger_price'] = {'binance': bin_bid, 'kucoin': kc_ask}
                        print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE B CONFIRMED 3/3 -> EXECUTING PARALLEL ORDERS")
                        notional_bin, notional_kc, bin_base_amt, kc_contracts = match_base_exposure_per_exchange(
                            binance, kucoin, sym, kc_trade_sym, NOTIONAL, bin_bid, kc_ask
                        )
                        results = {}
                        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_trade_sym, trigger_time=trigger_time, trigger_price=kc_ask)
                        def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, sym, trigger_time=trigger_time, trigger_price=bin_bid)
                        t1 = threading.Thread(target=exec_kc)
                        t2 = threading.Thread(target=exec_bin)
                        t1.start(); t2.start(); t1.join(); t2.join()
                        ok_kc, exec_price_kc, exec_time_kc = results.get('kc', (False, None, None))
                        ok_bin, exec_price_bin, exec_time_bin = results.get('bin', (False, None, None))
                        if ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None:
                            real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                            final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Spread: Real({real_entry_spread:.3f}%) {'≥' if real_entry_spread >= trigger_spread else '<'} Trigger({trigger_spread:.3f}%). Using {'Trigger' if real_entry_spread >= trigger_spread else 'Real'} Spread as profit basis.")
                            entry_prices[sym]['kucoin'] = exec_price_kc
                            entry_prices[sym]['binance'] = exec_price_bin
                            entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc}
                            entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin}
                            entry_spreads[sym] = final_entry_spread
                            positions[sym] = 'caseB'
                            trade_start_balances[sym] = start_total_balance
                            entry_confirm_count[sym] = 0
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
                                implied_bin = compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)[1]
                                implied_kc = compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)[1]
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} MATCHED BASE TARGET | bin_base_amount={bin_base_amt} | kc_contracts={kc_contracts}")
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} MATCHED NOTIONALS | bin_notional=${notional_bin:.6f} implied=${implied_bin:.8f} | kc_notional=${notional_kc:.6f} implied=${implied_kc:.8f}")
                            except Exception:
                                pass
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ENTRY SUMMARY | trigger_time={trigger_time.strftime('%H:%M:%S.%f')[:-3]} | trigger_prices bin:{bin_bid} kc:{kc_ask}")
                            print(f" KuCoin executed: price={exec_price_kc} exec_time={exec_time_kc} slippage={sl_kc:.8f} latency_ms={lat_kc}")
                            print(f" Binance executed: price={exec_price_bin} exec_time={exec_time_bin} slippage={sl_bin:.8f} latency_ms={lat_bin}")
                            print(f" REAL Entry Spread: {real_entry_spread:.3f}% | PROFIT BASIS Spread: {final_entry_spread:.3f}%")
                        else:
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} WARNING: Partial or failed execution in Case B. Closing positions if any.")
                            close_all_and_wait()
                            entry_confirm_count[sym] = 0

            # EXIT logic with 3x confirmation
            if positions[sym] is not None:
                if positions[sym] == 'caseA':
                    current_exit_spread = 100 * (kc_ask - bin_bid) / entry_prices[sym]['binance']
                    captured = entry_spreads[sym] - current_exit_spread
                    current_entry_spread = 100 * (kc_bid - bin_ask) / bin_ask
                elif positions[sym] == 'caseB':
                    current_exit_spread = 100 * (bin_bid - kc_ask) / entry_prices[sym]['kucoin']
                    captured = entry_spreads[sym] - current_exit_spread
                    current_entry_spread = 100 * (bin_bid - kc_ask) / kc_ask

                print(f"{datetime.now().strftime('%H:%M:%S')} POSITION OPEN | Entry Spread (Trigger): {current_entry_spread:.3f}% | Entry Basis: {entry_spreads[sym]:.3f}% | Exit Spread: {current_exit_spread:.3f}% | Captured: {captured:.3f}% | Exit Confirm: {exit_confirm_count[sym] + (1 if (captured >= PROFIT_TARGET or abs(current_exit_spread) < 0.02) else 0)}/3")
                exit_condition = captured >= PROFIT_TARGET or abs(current_exit_spread) < 0.02
                if exit_condition:
                    exit_confirm_count[sym] += 1
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
                        exit_confirm_count[sym] = 0
                    else:
                        print(f"{datetime.now().strftime('%H:%M:%S')} → Exit condition met, confirming {exit_confirm_count[sym]}/3...")
                else:
                    exit_confirm_count[sym] = 0

        time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopping bot...")
        try:
            close_all_and_wait()
        except Exception as e:
            print("Error during graceful shutdown close:", e)
        break

    except Exception as e:
        print("ERROR:", e)
        time.sleep(0.5)
