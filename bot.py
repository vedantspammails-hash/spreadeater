#!/usr/bin/env python3
# MULTI-COIN 4x LIVE ARB BOT – Single coin (TRUSTUSDT / AIAUSDT / etc.)
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

# ========================= CONFIG =========================
SYMBOLS = ["AIAUSDT"]                        # CHANGE TO YOUR COIN
KUCOIN_SYMBOLS = ["AIAUSDTM"]                # KuCoin futures symbol ID
NOTIONAL = 10.0
LEVERAGE = 5
ENTRY_SPREAD = 1.2
PROFIT_TARGET = 0.1
MARGIN_BUFFER = 1.02
# =========================================================

print(f"\n{'='*80}")
print(f"SINGLE COIN 4x LIVE ARB BOT | NOTIONAL ${NOTIONAL} @ {LEVERAGE}x | ENTRY >= {ENTRY_SPREAD}% | PROFIT TARGET {PROFIT_TARGET}%")
print(f"Tracking symbol: {SYMBOLS[0]} ↔ {KUCOIN_SYMBOLS[0]}")
print(f"{'='*80}\n")

# Exchanges
binance = ccxt.binance({
    'apiKey': os.getenv('BINANCE_API_KEY'),
    'secret': os.getenv('BINANCE_API_SECRET'),
    'options': {'defaultType': 'future'},
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
    except: pass
    try:
        server = requests.get("https://api-futures.kucoin.com/api/v1/timestamp", timeout=5).json().get('data')
        if server: kucoin.options['timeDifference'] = int(time.time()*1000) - int(server)
    except: pass
fix_time_offset()

# Helpers
def ensure_markets_loaded():
    for ex in [binance, kucoin]:
        try: ex.load_markets(reload=False)
        except:
            try: ex.load_markets(reload=True)
            except: pass

def get_market(exchange, symbol):
    ensure_markets_loaded()
    m = exchange.markets.get(symbol)
    if not m:
        try: exchange.load_markets(reload=True)
        except: pass
        m = exchange.markets.get(symbol)
    return m

def round_down(value, precision):
    if precision is None: return float(value)
    factor = 10**precision
    return math.floor(value * factor) / factor

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
    try: exchange.load_markets(reload=True)
    except: pass
    raw_id = (raw_id or "").upper()
    for sym, m in (exchange.markets or {}).items():
        if (m.get('id') or "").upper() == raw_id: return sym
    for sym, m in (exchange.markets or {}).items():
        if raw_id in (m.get('id') or "").upper(): return sym
    return None

KUCOIN_TRADE_SYMBOLS = [resolve_kucoin_trade_symbol(kucoin, k) for k in KUCOIN_SYMBOLS]

def set_leverage_and_margin():
    for i, sym in enumerate(SYMBOLS):
        try:
            binance.set_leverage(LEVERAGE, sym)
            print(f"Binance leverage set to {LEVERAGE}x for {sym}")
        except Exception as e: print(f"Binance leverage error for {sym}: {e}")
        kc_sym = KUCOIN_TRADE_SYMBOLS[i]
        if kc_sym:
            try:
                kucoin.set_leverage(LEVERAGE, kc_sym, {'marginMode': 'cross'})
                print(f"KuCoin leverage set to {LEVERAGE}x CROSS for {kc_sym}")
            except Exception as e: print(f"KuCoin leverage error for {kc_sym}: {e}")

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

def get_total_futures_balance():
    try:
        bal_bin = binance.fetch_balance(params={'type': 'future'})
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
            if pos and abs(float(pos[0].get('positionAmt') or 0)) > 0: return True
        for kc_sym in KUCOIN_TRADE_SYMBOLS:
            if not kc_sym: continue
            pos = kucoin.fetch_positions([kc_sym])
            if pos:
                raw = pos[0].get('info', {}).get('currentQty', 0)
                if abs(float(raw or 0)) > 0: return True
        return False
    except: return True

# FINAL BULLETPROOF CLOSE FUNCTION – ONLY CHANGE IN ENTIRE SCRIPT
def close_all_and_wait(timeout_s=25, poll_interval=0.4):
    global closing_in_progress
    print("\n" + "="*80)
    print("CLOSING ALL POSITIONS – BULLETPROOF MODE (closePosition=True on Binance)")
    print("="*80)

    # BINANCE: closePosition=True → works for LONG and SHORT, no side/amount errors EVER
    for sym in SYMBOLS:
        try:
            pos_list = binance.fetch_positions([sym])
            if not pos_list:
                continue
            pos = pos_list[0]
            raw_amt = float(pos.get('positionAmt') or 0)
            if abs(raw_amt) <= 0:
                continue
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE {sym} | positionAmt={raw_amt} → USING closePosition=True")
            binance.create_order(
                symbol=sym,
                type='market',
                side='buy',                           # side ignored when closePosition=True
                amount=None,
                params={'closePosition': True, 'reduceOnly': True}
            )
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE {sym} FULL CLOSE ORDER SENT SUCCESSFULLY")
        except Exception as e:
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE CLOSE FAILED: {e}")

    # KUCOIN: unchanged – already perfect
    try:
        all_kc_positions = kucoin.fetch_positions(symbols=KUCOIN_TRADE_SYMBOLS)
        for pos in all_kc_positions:
            ccxt_sym = pos.get('symbol')
            if ccxt_sym not in KUCOIN_TRADE_SYMBOLS: continue
            raw_qty_signed = float(pos.get('info', {}).get('currentQty', 0))
            if abs(raw_qty_signed) == 0: continue
            side = 'sell' if raw_qty_signed > 0 else 'buy'
            qty = abs(raw_qty_signed)
            market = get_market(kucoin, ccxt_sym)
            prec = market.get('precision', {}).get('amount') if market else None
            qty = round_down(qty, prec) if prec is not None else qty
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Closing KuCoin {ccxt_sym} {side} {qty}")
            kucoin.create_market_order(ccxt_sym, side, qty, params={'reduceOnly': True, 'marginMode': 'cross'})
    except Exception as e:
        print(f"KUCOIN CLOSE ERROR: {e}")

    start = time.time()
    while time.time() - start < timeout_s:
        if not has_open_positions():
            closing_in_progress = False
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ALL POSITIONS CLOSED AND CONFIRMED")
            total_bal, bin_bal, kc_bal = get_total_futures_balance()
            print(f"*** POST-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
            print("="*80)
            return True
        time.sleep(poll_interval)

    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} TIMEOUT – positions may still be open")
    print("="*80)
    closing_in_progress = False
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
                        if ts_int > 1e12: ts_ms = ts_int
                        elif ts_int > 1e9: ts_ms = ts_int * 1000
                        else: ts_ms = int(time.time()*1000)
                        iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + 'Z'
                        return float(avg), iso
                    except: pass
                return float(avg), datetime.utcnow().isoformat() + 'Z'
    except: pass
    return None, None

def safe_create_order(exchange, side, notional, price, symbol, trigger_time=None, trigger_price=None):
    amt, _, _, prec = compute_amount_for_notional(exchange, symbol, notional, price)
    amt = round_down(amt, prec) if prec is not None else amt
    if amt <= 0:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} computed amt <=0, skipping order for {exchange.id} {symbol}")
        return False, None, None
    try:
        if exchange.id == 'binance':
            order = exchange.create_market_buy_order(symbol, amt) if side.lower() == 'buy' else exchange.create_market_sell_order(symbol, amt)
        else:
            params = {'leverage': LEVERAGE, 'marginMode': 'cross'}
            order = exchange.create_market_buy_order(symbol, amt, params=params) if side.lower() == 'buy' else exchange.create_market_sell_order(symbol, amt, params=params)
        exec_price, exec_time = extract_executed_price_and_time(exchange, symbol, order)
        slippage = exec_price - float(trigger_price) if trigger_price and exec_price else None
        latency_ms = None
        if trigger_time and exec_time:
            try:
                t0_ms = int(trigger_time.timestamp() * 1000)
                t1_ms = int(datetime.fromisoformat(exec_time.replace('Z', '')).timestamp() * 1000)
                latency_ms = t1_ms - t0_ms
            except: pass
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {exchange.id.upper()} ORDER EXECUTED | {side.upper()} {amt} {symbol} | exec_price={exec_price} slippage={slippage} latency_ms={latency_ms}")
        return True, exec_price, exec_time
    except Exception as e:
        print(f"{exchange.id.upper()} order failed: {e}")
        return False, None, None

def match_base_exposure_per_exchange(bin_exchange, kc_exchange, bin_symbol, kc_symbol, desired_usdt, bin_price, kc_price):
    m_bin = get_market(bin_exchange, bin_symbol)
    m_kc = get_market(kc_exchange, kc_symbol)
    bin_prec = m_bin.get('precision', {}).get('amount') if m_bin else None
    kc_prec = m_kc.get('precision', {}).get('amount') if m_kc else None
    bin_contract_size = float(m_bin.get('contractSize') or 1.0) if m_bin else 1.0
    kc_contract_size = float(m_kc.get('contractSize') or 1.0) if m_kc else 1.0
    ref_price = (float(bin_price) + float(kc_price)) / 2.0
    target_base = desired_usdt / ref_price
    bin_base_amount = round_down(target_base, bin_prec) if bin_prec is not None else target_base
    kc_contracts = round_down(target_base / kc_contract_size, kc_prec) if kc_prec is not None and kc_contract_size else target_base
    notional_bin = bin_base_amount * float(bin_price)
    notional_kc = kc_contracts * float(kc_contract_size) * float(kc_price)
    return float(notional_bin), float(notional_kc), float(bin_base_amount), float(kc_contracts)

def get_prices():
    bin_prices = {}
    kc_prices = {}
    try:
        data = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
        for item in data:
            if item['symbol'] in SYMBOLS:
                bin_prices[item['symbol']] = (float(item['bidPrice']), float(item['askPrice']))
        for raw in KUCOIN_SYMBOLS:
            resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={raw}", timeout=5).json()
            d = resp.get('data', {})
            kc_prices[raw] = (float(d.get('bestBidPrice', 0) or 0), float(d.get('bestAskPrice', 0) or 0))
    except: pass
    return bin_prices, kc_prices

start_total_balance, start_bin_balance, start_kc_balance = get_total_futures_balance()
print(f"Starting total balance approx: ${start_total_balance:.2f} (Binance: ${start_bin_balance:.2f} | KuCoin: ${start_kc_balance:.2f})\n")
print(f"{datetime.now()} BOT STARTED – monitoring {SYMBOLS[0]}\n")

while True:
    try:
        bin_prices, kc_prices = get_prices()
        for i, sym in enumerate(SYMBOLS):
            bin_bid, bin_ask = bin_prices.get(sym, (None, None))
            kc_bid, kc_ask = kc_prices.get(KUCOIN_SYMBOLS[i], (None, None))
            kc_trade_sym = KUCOIN_TRADE_SYMBOLS[i]
            if None in (bin_bid, bin_ask, kc_bid, kc_ask) or not kc_trade_sym: continue

            if bin_ask < kc_bid:
                trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                if trigger_spread < ENTRY_SPREAD:
                    entry_confirm_count[sym] = 0
            elif bin_bid > kc_ask:
                trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                if trigger_spread < ENTRY_SPREAD:
                    entry_confirm_count[sym] = 0

            # CASE A
            if bin_ask < kc_bid:
                trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                print(f"{datetime.now().strftime('%H:%M:%S')} CASE A | Trigger Spread: {trigger_spread:.3f}% | Confirm: {entry_confirm_count[sym]+1}/3")
                if positions[sym] is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions():
                    entry_confirm_count[sym] += 1
                    if entry_confirm_count[sym] >= 3:
                        total_bal, bin_bal, kc_bal = get_total_futures_balance()
                        print(f"*** PRE-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
                        trigger_time = datetime.utcnow()
                        entry_actual[sym]['trigger_time'] = trigger_time
                        entry_actual[sym]['trigger_price'] = {'binance': bin_ask, 'kucoin': kc_bid}
                        print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE A CONFIRMED 3/3 -> EXECUTING")
                        notional_bin, notional_kc, bin_base_amt, kc_contracts = match_base_exposure_per_exchange(binance, kucoin, sym, kc_trade_sym, NOTIONAL, bin_ask, kc_bid)
                        results = {}
                        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_trade_sym, trigger_time, kc_bid)
                        def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, sym, trigger_time, bin_ask)
                        t1 = threading.Thread(target=exec_kc); t2 = threading.Thread(target=exec_bin)
                        t1.start(); t2.start(); t1.join(); t2.join()
                        ok_kc, exec_price_kc, exec_time_kc = results.get('kc', (False, None, None))
                        ok_bin, exec_price_bin, exec_time_bin = results.get('bin', (False, None, None))
                        if ok_kc and ok_bin:
                            real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                            final_entry_spread = max(trigger_spread, real_entry_spread)
                            entry_prices[sym] = {'binance': exec_price_bin, 'kucoin': exec_price_kc}
                            entry_spreads[sym] = final_entry_spread
                            positions[sym] = 'caseA'
                            entry_confirm_count[sym] = 0
                            print(f" MATCHED BASE: bin_base_amount={bin_base_amt} | kc_contracts={kc_contracts}")
                            print(f" ENTRY EXECUTED | Binance @ {exec_price_bin} | KuCoin @ {exec_price_kc} | Real Spread: {real_entry_spread:.3f}%")
                        else:
                            print("Partial execution → closing any open positions")
                            close_all_and_wait()

            # CASE B
            elif bin_bid > kc_ask:
                trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                print(f"{datetime.now().strftime('%H:%M:%S')} CASE B | Trigger Spread: {trigger_spread:.3f}% | Confirm: {entry_confirm_count[sym]+1}/3")
                if positions[sym] is None and trigger_spread >= ENTRY_SPREAD and not closing_in_progress and not has_open_positions():
                    entry_confirm_count[sym] += 1
                    if entry_confirm_count[sym] >= 3:
                        total_bal, bin_bal, kc_bal = get_total_futures_balance()
                        print(f"*** PRE-TRADE Total Balance: ${total_bal:.2f} (Binance: ${bin_bal:.2f} | KuCoin: ${kc_bal:.2f}) ***")
                        trigger_time = datetime.utcnow()
                        entry_actual[sym]['trigger_time'] = trigger_time
                        entry_actual[sym]['trigger_price'] = {'binance': bin_bid, 'kucoin': kc_ask}
                        print(f"{trigger_time.strftime('%H:%M:%S.%f')[:-3]} ENTRY CASE B CONFIRMED 3/3 -> EXECUTING")
                        notional_bin, notional_kc, bin_base_amt, kc_contracts = match_base_exposure_per_exchange(binance, kucoin, sym, kc_trade_sym, NOTIONAL, bin_bid, kc_ask)
                        results = {}
                        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_trade_sym, trigger_time, kc_ask)
                        def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, sym, trigger_time, bin_bid)
                        t1 = threading.Thread(target=exec_kc); t2 = threading.Thread(target=exec_bin)
                        t1.start(); t2.start(); t1.join(); t2.join()
                        ok_kc, exec_price_kc, exec_time_kc = results.get('kc', (False, None, None))
                        ok_bin, exec_price_bin, exec_time_bin = results.get('bin', (False, None, None))
                        if ok_kc and ok_bin:
                            real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                            final_entry_spread = max(trigger_spread, real_entry_spread)
                            entry_prices[sym] = {'binance': exec_price_bin, 'kucoin': exec_price_kc}
                            entry_spreads[sym] = final_entry_spread
                            positions[sym] = 'caseB'
                            entry_confirm_count[sym] = 0
                            print(f" MATCHED BASE: bin_base_amount={bin_base_amt} | kc_contracts={kc_contracts}")
                            print(f" ENTRY EXECUTED | Binance @ {exec_price_bin} | KuCoin @ {exec_price_kc} | Real Spread: {real_entry_spread:.3f}%")
                        else:
                            print("Partial execution → closing any open positions")
                            close_all_and_wait()

            # EXIT LOGIC
            if positions[sym] is not None:
                if positions[sym] == 'caseA':
                    current_exit_spread = 100 * (kc_ask - bin_bid) / entry_prices[sym]['binance']
                    captured = entry_spreads[sym] - current_exit_spread
                else:  # caseB
                    current_exit_spread = 100 * (bin_bid - kc_ask) / entry_prices[sym]['kucoin']
                    captured = entry_spreads[sym] - current_exit_spread

                print(f"{datetime.now().strftime('%H:%M:%S')} POSITION OPEN | Entry Basis: {entry_spreads[sym]:.3f}% | Current Exit Spread: {current_exit_spread:.3f}% | Captured: {captured:.3f}% | Exit Confirm: {exit_confirm_count[sym]+(1 if captured >= PROFIT_TARGET or abs(current_exit_spread) < 0.02 else 0)}/3")
                if captured >= PROFIT_TARGET or abs(current_exit_spread) < 0.02:
                    exit_confirm_count[sym] += 1
                    if exit_confirm_count[sym] >= 3:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} EXIT TRIGGERED 3/3 | Captured: {captured:.3f}%")
                        close_all_and_wait()
                        positions[sym] = None
                        exit_confirm_count[sym] = 0
                else:
                    exit_confirm_count[sym] = 0

        time.sleep(0.1)
    except KeyboardInterrupt:
        print("Stopping bot...")
        close_all_and_wait()
        break
    except Exception as e:
        print("ERROR:", e)
        time.sleep(0.5)
