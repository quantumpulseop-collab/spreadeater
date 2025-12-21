#!/usr/bin/env python3
# FULL BOT - single-file copy
# Goal: ensure correct executed price for KuCoin by polling REST endpoints (no websockets).
# Includes:
#  - robust extraction of executed price/qty/time
#  - poll_kucoin_until_filled() that aggregates fetch_order / fetch_my_trades until authoritative fill info is found
#  - safe_create_order uses poller for KuCoin to ensure correct exec price/qty
#  - rebalance behavior: KuCoin left unchanged after entry; only Binance adjusted
#  - stop placement (reduceOnly stop-market) attempted after entries
#  - liquidation watcher with immediate single-confirm action when counterpart drops to zero
#
# NOTE: This file is intended to be pasted as a full replacement for your bot.py.
# Test on dry-run or testnet before running live.

import os
import sys
import time
import math
import requests
import threading
from datetime import datetime
from dotenv import load_dotenv
import ccxt

load_dotenv()

REQUIRED = [
    'BINANCE_API_KEY',
    'BINANCE_API_SECRET',
    'KUCOIN_API_KEY',
    'KUCOIN_API_SECRET',
    'KUCOIN_API_PASSPHRASE',
]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print(f"ERROR: Missing .env keys: {', '.join(missing)}")
    sys.exit(1)

# CONFIG
SYMBOLS = ["LIGHTUSDT"]
KUCOIN_SYMBOLS = ["LIGHTUSDTM"]
NOTIONAL = float(os.getenv('NOTIONAL', "45.0"))
LEVERAGE = int(os.getenv('LEVERAGE', "5"))
ENTRY_SPREAD = float(os.getenv('ENTRY_SPREAD', "1.5"))
PROFIT_TARGET = float(os.getenv('PROFIT_TARGET', "0.8"))
MARGIN_BUFFER = float(os.getenv('MARGIN_BUFFER', "1.02"))

# Liquidation watcher config
WATCHER_POLL_INTERVAL = float(os.getenv('WATCHER_POLL_INTERVAL', "0.5"))
WATCHER_DETECT_CONFIRM = int(os.getenv('WATCHER_DETECT_CONFIRM', "2"))

# Notional / rebalance config
MAX_NOTIONAL_MISMATCH_PCT = float(os.getenv('MAX_NOTIONAL_MISMATCH_PCT', "0.5"))
REBALANCE_MIN_DOLLARS = float(os.getenv('REBALANCE_MIN_DOLLARS', "0.5"))

# Stop order buffer (2% from liquidation by default)
STOP_BUFFER_PCT = float(os.getenv('STOP_BUFFER_PCT', "0.02"))

# KuCoin transient error handling
KC_TRANSIENT_ERROR_THRESHOLD = int(os.getenv('KC_TRANSIENT_ERROR_THRESHOLD', "10"))
KC_TRANSIENT_BACKOFF_SECONDS = float(os.getenv('KC_TRANSIENT_BACKOFF_SECONDS', "2.0"))

print(f"\n{'='*72}")
print(f"SINGLE COIN 4x LIVE ARB BOT | NOTIONAL ${NOTIONAL} @ {LEVERAGE}x | ENTRY >= {ENTRY_SPREAD}% | PROFIT TARGET {PROFIT_TARGET}%")
print(f"Tracking symbol: {SYMBOLS[0]}")
print(f"NOTIONAL mismatch tolerance: {MAX_NOTIONAL_MISMATCH_PCT}% | REBALANCE_MIN_DOLLARS: ${REBALANCE_MIN_DOLLARS}")
print(f"STOP_BUFFER_PCT: {STOP_BUFFER_PCT*100:.1f}%")
print(f"{'='*72}\n")

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

# Time sync (best-effort)
def fix_time_offset():
    try:
        server = requests.get("https://fapi.binance.com/fapi/v1/time", timeout=5).json().get('serverTime')
        if server:
            binance.options['timeDifference'] = int(time.time()*1000) - int(server)
    except Exception:
        pass
    try:
        server = requests.get("https://api-futures.kucoin.com/api/v1/timestamp", timeout=5).json().get('data')
        if server:
            kucoin.options['timeDifference'] = int(time.time()*1000) - int(server)
    except Exception:
        pass

fix_time_offset()

# Helpers
def ensure_markets_loaded():
    for ex in (binance, kucoin):
        try:
            ex.load_markets(False)
        except Exception:
            try:
                ex.load_markets(True)
            except Exception:
                pass

def get_market(exchange, symbol):
    ensure_markets_loaded()
    m = exchange.markets.get(symbol)
    if not m:
        try:
            exchange.load_markets(True)
        except Exception:
            pass
        m = exchange.markets.get(symbol)
    return m

def round_down(value, precision):
    if precision is None:
        return float(value)
    factor = 10 ** precision
    return math.floor(value * factor) / factor

def compute_amount_for_notional(exchange, symbol, desired_usdt, price):
    market = get_market(exchange, symbol)
    amount_precision = None
    contract_size = 1.0
    if market:
        prec = market.get('precision')
        if isinstance(prec, dict):
            amount_precision = prec.get('amount')
        contract_size = float(market.get('contractSize') or market.get('info', {}).get('contractSize') or 1.0)
    if price <= 0:
        return 0.0, 0.0, contract_size, amount_precision
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
        exchange.load_markets(True)
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

# Set leverage & margin
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
                kucoin.set_leverage(LEVERAGE, kc_sym, {'marginMode': 'cross'})
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
terminate_bot = False
_liquidation_watchers = {}
_last_known_positions = {s: {'bin': 0.0, 'kc': 0.0, 'kc_err_count': 0} for s in SYMBOLS}

# --- Balance / position readers ---
def get_total_futures_balance():
    try:
        bal_bin = binance.fetch_balance(params={'type': 'future'})
        bin_usdt = float(bal_bin.get('USDT', {}).get('total', 0.0))
        bal_kc = kucoin.fetch_balance()
        kc_usdt = float(bal_kc.get('USDT', {}).get('total', 0.0))
        return bin_usdt + kc_usdt, bin_usdt, kc_usdt
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
            if not kc_sym:
                continue
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

# --- Robust position signed qty extractors ---
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

# --- Close helpers ---
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
    for sym in SYMBOLS:
        try:
            positions_bin = binance.fetch_positions([sym])
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance fetched positions for {sym}: {positions_bin}")
        except Exception as e:
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance fetch_positions error for {sym}: {e}")
            positions_bin = None
        if positions_bin:
            pos = positions_bin[0]
            raw_signed = _get_signed_position_amount(pos)
            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance raw positionAmt (signed) for {sym}: {raw_signed}")
            if abs(raw_signed) > 0:
                side = 'sell' if raw_signed > 0 else 'buy'
                market = get_market(binance, sym)
                prec = market.get('precision', {}).get('amount') if market else None
                qty = round_down(abs(raw_signed), prec) if prec is not None else abs(raw_signed)
                if qty > 0:
                    try:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Attempting Binance qty-based reduceOnly close for {sym} -> {side} {qty}")
                        try:
                            binance.create_market_order(sym, side, qty, params={'reduceOnly': True})
                        except TypeError:
                            binance.create_order(symbol=sym, type='market', side=side, amount=qty, params={'reduceOnly': True})
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} Binance qty-based reduceOnly close submitted for {sym}")
                    except Exception as e:
                        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} BINANCE qty close failed for {sym}: {e}")
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

# --- executed price/time/qty extractor (robust) ---
def extract_executed_price_time_qty(exchange, symbol, order_obj):
    try:
        if isinstance(order_obj, dict):
            info = order_obj.get('info') or {}
            avg = order_obj.get('average') or order_obj.get('averagePrice') or order_obj.get('price') or info.get('avgPrice') or info.get('avg_price') or info.get('avg') or info.get('dealPrice') or info.get('deal_price')
            exec_price = None
            if avg:
                try:
                    exec_price = float(avg)
                except Exception:
                    try:
                        exec_price = float(str(avg))
                    except Exception:
                        exec_price = None
            ts = order_obj.get('timestamp') or info.get('transactTime') or info.get('time') or info.get('tradeTime') or info.get('createdAt') or info.get('dealTime')
            exec_time_iso = None
            if ts:
                try:
                    ts_int = int(ts)
                    if ts_int > 1e12:
                        ts_ms = ts_int
                    elif ts_int > 1e9:
                        ts_ms = ts_int * 1000
                    else:
                        ts_ms = int(time.time() * 1000)
                    exec_time_iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + 'Z'
                except Exception:
                    exec_time_iso = None
            executed_qty = None
            for k in ('filled', 'filledQty', 'filledSize', 'executedQty', 'filled_amount', 'amount', 'filledAmount', 'dealSize', 'dealAmount', 'dealQty', 'filled_size', 'size'):
                v = order_obj.get(k) or info.get(k) or order_obj.get('amount') or order_obj.get('filled')
                if v not in (None, ''):
                    try:
                        executed_qty = float(v)
                        break
                    except Exception:
                        try:
                            executed_qty = float(str(v).replace(',', ''))
                            break
                        except Exception:
                            executed_qty = None
            deals = info.get('deals') or info.get('trades') or info.get('dealList') or info.get('filledTrades')
            if isinstance(deals, (list, tuple)) and (executed_qty is None or exec_price is None):
                total_qty = 0.0
                weighted = 0.0
                last_ts = None
                for d in deals:
                    try:
                        px = d.get('price') or d.get('dealPrice') or d.get('avgPrice') or d.get('priceStr')
                        q = d.get('size') or d.get('qty') or d.get('quantity') or d.get('amount') or d.get('dealSize')
                        if px is None or q is None:
                            continue
                        px_f = float(px)
                        q_f = float(q)
                        weighted += px_f * q_f
                        total_qty += q_f
                        last_ts = last_ts or d.get('time') or d.get('tradeTime') or d.get('createTime') or d.get('dealTime')
                    except Exception:
                        continue
                if total_qty > 0:
                    executed_qty = executed_qty or total_qty
                    exec_price = exec_price or (weighted / total_qty)
                    if last_ts and not exec_time_iso:
                        try:
                            ts_int = int(last_ts)
                            ts_ms = ts_int if ts_int > 1e12 else (ts_int * 1000 if ts_int > 1e9 else int(time.time()*1000))
                            exec_time_iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + 'Z'
                        except Exception:
                            pass
            if exec_price is not None or executed_qty is not None or exec_time_iso is not None:
                return exec_price, exec_time_iso, executed_qty
    except Exception:
        pass
    # fetch_my_trades fallback
    try:
        now_ms = int(time.time() * 1000)
        trades = exchange.fetch_my_trades(symbol, since=now_ms-120000, limit=200)
        if trades:
            t = sorted(trades, key=lambda x: x.get('timestamp') or 0)[-1]
            px = t.get('price') or (t.get('info') or {}).get('price') or (t.get('info') or {}).get('dealPrice')
            ts = t.get('timestamp') or (t.get('info') or {}).get('time') or (t.get('info') or {}).get('tradeTime')
            qty = t.get('amount') or t.get('cost') or t.get('quantity') or (t.get('info') or {}).get('size') or (t.get('info') or {}).get('dealSize')
            exec_price = None
            exec_time_iso = None
            executed_qty = None
            if px:
                try:
                    exec_price = float(px)
                except Exception:
                    pass
            if ts:
                try:
                    ts_int = int(ts)
                    ts_ms = ts_int if ts_int > 1e12 else (ts_int * 1000 if ts_int > 1e9 else int(time.time()*1000))
                    exec_time_iso = datetime.utcfromtimestamp(ts_ms/1000.0).isoformat() + 'Z'
                except Exception:
                    pass
            if qty:
                try:
                    executed_qty = float(qty)
                except Exception:
                    try:
                        executed_qty = float(str(qty).replace(',', ''))
                    except Exception:
                        executed_qty = None
            if exec_price is not None or executed_qty is not None:
                return exec_price, exec_time_iso, executed_qty
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
            return float(mid), datetime.utcnow().isoformat() + 'Z', None
    except Exception:
        pass
    return None, None, None

# --- KuCoin polling fallback (new) ---
def _resolve_order_id(order_resp):
    if not order_resp:
        return None
    if isinstance(order_resp, dict):
        if order_resp.get('id'):
            return str(order_resp.get('id'))
        info = order_resp.get('info') or {}
        for k in ('orderId', 'order_id', 'id', 'clientOid', 'orderOid'):
            if info.get(k):
                return str(info.get(k))
        for k in ('orderId', 'order_id', 'clientOid'):
            if order_resp.get(k):
                return str(order_resp.get(k))
    try:
        return str(order_resp)
    except Exception:
        return None

def _aggregate_trades_for_order_from_trades(trades, order_id):
    if not trades:
        return None, None, None
    total_qty = 0.0
    weighted = 0.0
    latest_ts = None
    for t in trades:
        info = t.get('info') or {}
        tid_order = None
        for fld in ('orderId', 'order_id', 'clientOrderId', 'orderOid'):
            if info.get(fld):
                tid_order = str(info.get(fld))
                break
        if not tid_order:
            tid_order = str(t.get('order') or t.get('orderId') or (t.get('info') or {}).get('orderId') or '')
        if tid_order and order_id and (str(order_id) == tid_order):
            q = t.get('amount') or t.get('filled') or t.get('size') or info.get('size') or info.get('qty') or info.get('dealSize')
            p = t.get('price') or info.get('price') or info.get('dealPrice') or info.get('avgPrice')
            ts = t.get('timestamp') or info.get('tradeTime') or info.get('ts')
            try:
                qf = float(q)
            except Exception:
                continue
            try:
                pf = float(p)
            except Exception:
                continue
            total_qty += qf
            weighted += pf * qf
            if ts:
                try:
                    t_int = int(ts)
                    t_ms = t_int if t_int > 1e12 else (t_int * 1000 if t_int > 1e9 else int(time.time()*1000))
                    latest_ts = max(latest_ts or 0, t_ms)
                except Exception:
                    pass
    if total_qty > 0:
        exec_price = weighted / total_qty if weighted else None
        exec_time_iso = (datetime.utcfromtimestamp(latest_ts/1000.0).isoformat() + 'Z') if latest_ts else None
        return exec_price, exec_time_iso, total_qty
    return None, None, None

def poll_kucoin_until_filled(kucoin_exchange, create_order_resp, symbol, timeout_s=8.0, poll_interval=0.6):
    order_id = _resolve_order_id(create_order_resp)
    deadline = time.time() + timeout_s
    try:
        px, ts_iso, qty = extract_executed_price_time_qty(kucoin_exchange, symbol, create_order_resp)
        if qty and px:
            return px, ts_iso, qty
    except Exception:
        pass
    while time.time() < deadline:
        try:
            fo = None
            if order_id:
                try:
                    fo = kucoin_exchange.fetch_order(order_id, symbol)
                except Exception:
                    fo = None
                if fo:
                    px, ts_iso, qty = extract_executed_price_time_qty(kucoin_exchange, symbol, fo)
                    if qty and px:
                        return px, ts_iso, qty
            since_ms = int((time.time() - 60) * 1000)
            try:
                trades = kucoin_exchange.fetch_my_trades(symbol, since=since_ms, limit=200)
            except Exception:
                trades = None
            if trades:
                px, ts_iso, qty = _aggregate_trades_for_order_from_trades(trades, order_id)
                if qty and px:
                    return px, ts_iso, qty
        except Exception:
            pass
        time.sleep(poll_interval)
    try:
        px, ts_iso, qty = extract_executed_price_time_qty(kucoin_exchange, symbol, create_order_resp)
        if qty and px:
            return px, ts_iso, qty
    except Exception:
        pass
    try:
        t = kucoin_exchange.fetch_ticker(symbol)
        if t:
            bid = t.get('bid') or t.get('bidPrice')
            ask = t.get('ask') or t.get('askPrice')
            mid = None
            if bid and ask:
                mid = (float(bid) + float(ask)) / 2.0
            elif t.get('last'):
                mid = float(t.get('last'))
            if mid:
                return float(mid), datetime.utcnow().isoformat() + 'Z', None
    except Exception:
        pass
    return None, None, None

# --- safe_create_order (uses poller for KuCoin) ---
def safe_create_order(exchange, side, notional, price, symbol, trigger_time=None, trigger_price=None):
    amt, _, _, prec = compute_amount_for_notional(exchange, symbol, notional, price)
    amt = round_down(amt, prec) if prec is not None else amt
    if amt <= 0:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} computed amt <=0, skipping order for {exchange.id} {symbol} (notional=${notional} price={price})")
        return False, None, None, None
    try:
        order = None
        if exchange.id == 'binance':
            if side.lower() == 'buy':
                order = exchange.create_market_buy_order(symbol, amt)
            else:
                order = exchange.create_market_sell_order(symbol, amt)
        else:
            params = {'leverage': LEVERAGE, 'marginMode': 'cross'}
            if side.lower() == 'buy':
                try:
                    order = exchange.create_market_buy_order(symbol, amt, params=params)
                except TypeError:
                    order = exchange.create_market_buy_order(symbol, amt)
            else:
                try:
                    order = exchange.create_market_sell_order(symbol, amt, params=params)
                except TypeError:
                    order = exchange.create_market_sell_order(symbol, amt)

        exec_price, exec_time, executed_qty = extract_executed_price_time_qty(exchange, symbol, order)

        # For KuCoin: poll REST until authoritative fill info is available (no websockets)
        if exchange.id != 'binance':
            try:
                px2, ts2, qty2 = poll_kucoin_until_filled(exchange, order, symbol, timeout_s=8.0, poll_interval=0.6)
                if px2 is not None:
                    exec_price = px2
                if ts2 is not None:
                    exec_time = ts2
                if qty2 is not None:
                    executed_qty = qty2
            except Exception as e:
                print(f"KuCoin polling fallback error: {e}")

        if executed_qty is None:
            try:
                if isinstance(order, dict):
                    info = order.get('info') or {}
                    possible = order.get('filled') or order.get('amount') or info.get('filledQty') or info.get('filledSize') or info.get('filled_amount') or info.get('filled')
                    if possible not in (None, ''):
                        executed_qty = float(possible)
            except Exception:
                executed_qty = None

        if executed_qty is None and exec_price is not None:
            try:
                market = get_market(exchange, symbol)
                contract_size = float(market.get('contractSize') or market.get('info', {}).get('contractSize') or 1.0) if market else 1.0
                if exchange.id == 'binance':
                    executed_qty = round_down(notional / exec_price, prec) if prec is not None else (notional / exec_price)
                else:
                    executed_qty = round_down((notional / exec_price) / contract_size, prec) if prec is not None else ((notional / exec_price) / contract_size)
            except Exception:
                executed_qty = None

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
        implied_exec_notional = None
        try:
            market = get_market(exchange, symbol)
            contract_size = float(market.get('contractSize') or market.get('info', {}).get('contractSize') or 1.0) if market else 1.0
            if executed_qty is not None and exec_price is not None:
                implied_exec_notional = float(executed_qty) * float(contract_size) * float(exec_price)
        except Exception:
            implied_exec_notional = None

        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {exchange.id.upper()} ORDER EXECUTED | {side.upper()} requested_amt={amt} {symbol} at market | exec_price={exec_price} exec_time={exec_time} executed_qty={executed_qty} implied_notional={implied_exec_notional} slippage={slippage} latency_ms={latency_ms}")
        return True, exec_price, exec_time, executed_qty
    except Exception as e:
        print(f"{exchange.id.upper()} order failed: {e}")
        return False, None, None, None

# --- match exposure function (unchanged) ---
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
            if isinstance(prec, dict):
                bin_prec = prec.get('amount')
            bin_contract_size = float(m_bin.get('contractSize') or m_bin.get('info', {}).get('contractSize') or 1.0)
    except Exception:
        pass
    try:
        if m_kc:
            prec = m_kc.get('precision')
            if isinstance(prec, dict):
                kc_prec = prec.get('amount')
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

# --- Prices via REST (bookTicker / kucoin ticker) ---
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

# --- Position info fetcher (used for stops) ---
def fetch_position_info(exchange, symbol):
    try:
        pos_list = exchange.fetch_positions([symbol])
    except Exception:
        try:
            pos_list = exchange.fetch_positions()
        except Exception:
            pos_list = []
    if not pos_list:
        return {'signed_qty': 0.0, 'liquidationPrice': None, 'side': None}
    pos = pos_list[0]
    signed = 0.0
    try:
        if exchange.id == 'binance':
            signed = _get_signed_from_binance_pos(pos)
        else:
            signed = _get_signed_from_kucoin_pos(pos)
    except Exception:
        signed = _get_signed_position_amount(pos)
    liq = None
    info = pos.get('info') or {}
    for fld in ('liquidationPrice', 'liquidation_price', 'liquidationPriceUsd', 'liquidation'):
        if fld in pos and pos.get(fld):
            try:
                liq = float(pos.get(fld))
                break
            except Exception:
                pass
        if fld in info and info.get(fld):
            try:
                liq = float(info.get(fld))
                break
            except Exception:
                pass
    if liq is None:
        try:
            if isinstance(info.get('position'), dict) and info.get('position').get('liquidationPrice'):
                liq = float(info.get('position').get('liquidationPrice'))
        except Exception:
            pass
    side = 'long' if signed > 0 else ('short' if signed < 0 else None)
    return {'signed_qty': float(signed or 0.0), 'liquidationPrice': liq, 'side': side}

# --- place_reduce_only_stop (best-effort) ---
def place_reduce_only_stop(exchange, symbol, position_side, position_qty, liquidation_price, buffer_pct=STOP_BUFFER_PCT, exec_price_fallback=None):
    if position_qty is None or position_qty <= 0:
        print(f"{datetime.now().isoformat()} Cannot place stop for {exchange.id} {symbol}: position_qty invalid ({position_qty})")
        return False
    stop_side = 'sell' if position_side == 'long' else 'buy'
    if liquidation_price is None:
        if exec_price_fallback is None:
            print(f"{datetime.now().isoformat()} Cannot place stop for {exchange.id} {symbol}: liquidation_price unknown and no fallback exec price provided")
            return False
        if position_side == 'long':
            stop_price = float(exec_price_fallback) * (1.0 - buffer_pct)
        else:
            stop_price = float(exec_price_fallback) * (1.0 + buffer_pct)
        print(f"{datetime.now().isoformat()} Using exec-price fallback for stop for {exchange.id} {symbol}: exec_price={exec_price_fallback} => stop_price={stop_price:.8f}")
    else:
        try:
            if position_side == 'long':
                stop_price = float(liquidation_price) * (1.0 + buffer_pct)
            else:
                stop_price = float(liquidation_price) * (1.0 - buffer_pct)
        except Exception:
            print(f"{datetime.now().isoformat()} Error computing stop_price for {exchange.id} {symbol} with liq={liquidation_price}")
            return False

    market = get_market(exchange, symbol)
    prec = None
    min_amount = None
    if market:
        prec = market.get('precision', {}).get('amount')
        limits = market.get('limits') or {}
        amt_lim = limits.get('amount') or {}
        try:
            if isinstance(amt_lim, dict):
                min_amount = amt_lim.get('min')
        except Exception:
            min_amount = None

    qty = round_down(position_qty, prec) if prec is not None else position_qty
    if qty <= 0:
        print(f"{datetime.now().isoformat()} Stop placement for {exchange.id} {symbol} skipped because qty rounded to 0 (raw={position_qty} prec={prec})")
        return False
    if min_amount is not None:
        try:
            if float(qty) < float(min_amount):
                print(f"{datetime.now().isoformat()} Stop placement for {exchange.id} {symbol} skipped: qty {qty} < min_amount {min_amount}")
                return False
        except Exception:
            pass

    err_msgs = []
    try:
        params = {'reduceOnly': True, 'marginMode': 'cross', 'stopPrice': stop_price}
        try:
            exchange.create_order(symbol, 'STOP_MARKET', stop_side, qty, None, params)
            print(f"{datetime.now().isoformat()} Placed STOP_MARKET reduceOnly on {exchange.id} {symbol} -> {stop_side} {qty} stopPrice={stop_price}")
            return True
        except Exception as e:
            err_msgs.append(f"STOP_MARKET failed: {e}")
    except Exception as e:
        err_msgs.append(f"STOP_MARKET exception: {e}")
    try:
        params = {'reduceOnly': True, 'marginMode': 'cross', 'stopPrice': stop_price}
        try:
            exchange.create_order(symbol, 'market', stop_side, qty, None, params)
            print(f"{datetime.now().isoformat()} Placed market stop (params.stopPrice) reduceOnly on {exchange.id} {symbol} -> {stop_side} {qty} stopPrice={stop_price}")
            return True
        except Exception as e:
            err_msgs.append(f"market+stopPrice failed: {e}")
    except Exception as e:
        err_msgs.append(f"market+stopPrice exception: {e}")
    try:
        params = {'reduceOnly': True, 'marginMode': 'cross', 'stopPrice': stop_price, 'closePosition': True}
        try:
            exchange.create_order(symbol, 'market', stop_side, None, None, params)
            print(f"{datetime.now().isoformat()} Placed closePosition stop reduceOnly on {exchange.id} {symbol} -> {stop_side} closePosition stopPrice={stop_price}")
            return True
        except Exception as e:
            err_msgs.append(f"closePosition stop failed: {e}")
    except Exception as e:
        err_msgs.append(f"closePosition stop exception: {e}")

    print(f"{datetime.now().isoformat()} Failed to place reduceOnly stop on {exchange.id} {symbol}. Attempts: {err_msgs}")
    return False

def try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc):
    try:
        pos_kc = None
        pos_bin = None
        try:
            pos_kc = fetch_position_info(kucoin, kc_trade_sym)
            _last_known_positions[sym]['kc'] = pos_kc.get('signed_qty', 0.0)
            _last_known_positions[sym]['kc_err_count'] = 0
        except Exception:
            _last_known_positions[sym]['kc_err_count'] = _last_known_positions[sym].get('kc_err_count', 0) + 1
            pos_kc = {'signed_qty': _last_known_positions[sym]['kc'], 'liquidationPrice': None, 'side': None}
            print(f"{datetime.now().isoformat()} Using last-known KuCoin qty due to fetch error: {_last_known_positions[sym]['kc']} (error_count={_last_known_positions[sym]['kc_err_count']})")
        try:
            pos_bin = fetch_position_info(binance, sym)
            _last_known_positions[sym]['bin'] = pos_bin.get('signed_qty', 0.0)
        except Exception:
            pos_bin = {'signed_qty': _last_known_positions[sym].get('bin', 0.0), 'liquidationPrice': None, 'side': None}
            print(f"{datetime.now().isoformat()} Binance fetch error for pos; using last-known bin qty: {_last_known_positions[sym].get('bin', 0.0)}")

        print(f"{datetime.now().isoformat()} POST-ENTRY POS INFO | kc={pos_kc} | bin={pos_bin}")

        qty_kc = abs(exec_qty_kc) if exec_qty_kc is not None else abs(pos_kc.get('signed_qty') or 0.0)
        qty_bin = abs(exec_qty_bin) if exec_qty_bin is not None else abs(pos_bin.get('signed_qty') or 0.0)
        liq_kc = pos_kc.get('liquidationPrice')
        liq_bin = pos_bin.get('liquidationPrice')
        side_kc = pos_kc.get('side')
        side_bin = pos_bin.get('side')

        print(f"{datetime.now().isoformat()} ATTEMPTING STOPS | kc_qty={qty_kc} kc_liq={liq_kc} kc_side={side_kc} | bin_qty={qty_bin} bin_liq={liq_bin} bin_side={side_bin}")

        placed_kc = place_reduce_only_stop(kucoin, kc_trade_sym, side_kc, qty_kc, liq_kc, buffer_pct=STOP_BUFFER_PCT, exec_price_fallback=exec_price_kc)
        placed_bin = place_reduce_only_stop(binance, sym, side_bin, qty_bin, liq_bin, buffer_pct=STOP_BUFFER_PCT, exec_price_fallback=exec_price_bin)

        print(f"{datetime.now().isoformat()} STOP_PLACED_RESULT | kc={placed_kc} | bin={placed_bin}")
        return placed_kc, placed_bin
    except Exception as e:
        print(f"{datetime.now().isoformat()} Error placing stops after entry: {e}")
        return False, False

# --- Liquidation watcher (immediate single-confirm on explicit zero, resilient to KuCoin 500s) ---
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
        raise

def _start_liquidation_watcher_for_symbol(sym, bin_sym, kc_sym):
    if _liquidation_watchers.get(sym):
        return
    stop_flag = threading.Event()
    _liquidation_watchers[sym] = stop_flag

    def monitor():
        print(f"{datetime.now().isoformat()} Liquidation watcher STARTED for {sym} (bin:{bin_sym} kc:{kc_sym})")
        prev_bin = _last_known_positions[sym].get('bin', 0.0)
        prev_kc = _last_known_positions[sym].get('kc', 0.0)
        zero_cnt_bin = 0
        zero_cnt_kc = 0
        seen_nonzero_bin = abs(prev_bin) > 1e-12
        seen_nonzero_kc = abs(prev_kc) > 1e-12
        ZERO_ABS_THRESHOLD = 1e-12

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
                if cur_bin is None:
                    cur_bin = _last_known_positions[sym].get('bin', 0.0)
                    print(f"{datetime.now().isoformat()} WATCHER BINANCE fetch error -> using last-known bin qty {cur_bin}")
                else:
                    _last_known_positions[sym]['bin'] = cur_bin

                cur_kc = None
                try:
                    cur_kc = _fetch_signed_kucoin(kc_sym)
                    _last_known_positions[sym]['kc'] = cur_kc
                    _last_known_positions[sym]['kc_err_count'] = 0
                except Exception as e:
                    _last_known_positions[sym]['kc_err_count'] = _last_known_positions[sym].get('kc_err_count', 0) + 1
                    print(f"{datetime.now().isoformat()} KUCOIN fetch error for {kc_sym}: {e} (consecutive_errors={_last_known_positions[sym]['kc_err_count']})")
                    cur_kc = _last_known_positions[sym].get('kc', None)
                    if _last_known_positions[sym]['kc_err_count'] >= KC_TRANSIENT_ERROR_THRESHOLD:
                        print(f"{datetime.now().isoformat()} KUCOIN persistent errors >= {KC_TRANSIENT_ERROR_THRESHOLD}; backing off")
                        time.sleep(KC_TRANSIENT_BACKOFF_SECONDS * _last_known_positions[sym]['kc_err_count'])

                try:
                    cur_bin_f = 0.0 if cur_bin is None else float(cur_bin)
                except Exception:
                    cur_bin_f = 0.0
                try:
                    cur_kc_f = None if cur_kc is None else float(cur_kc)
                except Exception:
                    cur_kc_f = None

                if cur_kc_f is None:
                    print(f"{datetime.now().isoformat()} WATCHER SKIP (no KuCoin reading available) prev_bin={prev_bin} prev_kc={prev_kc} cur_bin={cur_bin_f} cur_kc=None")
                    prev_bin = cur_bin_f
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue

                cur_bin_abs = abs(cur_bin_f)
                cur_kc_abs = abs(cur_kc_f)
                prev_bin_abs = abs(prev_bin)
                prev_kc_abs = abs(prev_kc)

                # Immediate single-confirm detection: prev non-zero -> cur zero
                if prev_bin_abs > ZERO_ABS_THRESHOLD and cur_bin_abs <= ZERO_ABS_THRESHOLD:
                    print(f"{datetime.now().isoformat()} Detected immediate ZERO on Binance (prev non-zero -> now zero). Triggering immediate targeted close of KuCoin.")
                    try:
                        ok = close_single_exchange_position(kucoin, kc_sym)
                        if not ok:
                            print(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} Error when closing after Binance immediate zero: {e}")
                    global terminate_bot
                    terminate_bot = True
                    break

                if prev_kc_abs > ZERO_ABS_THRESHOLD and cur_kc_abs <= ZERO_ABS_THRESHOLD:
                    print(f"{datetime.now().isoformat()} Detected immediate ZERO on KuCoin (prev non-zero -> now zero). Triggering immediate targeted close of Binance.")
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            print(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} Error when closing after KuCoin immediate zero: {e}")
                    terminate_bot = True
                    break

                # Sustained-zero detection
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

                print(f"{datetime.now().isoformat()} WATCHER {sym} prev_bin={prev_bin_abs:.6f} cur_bin={cur_bin_abs:.6f} prev_kc={prev_kc_abs:.6f} cur_kc={cur_kc_abs:.6f} zero_cnt_bin={zero_cnt_bin}/{WATCHER_DETECT_CONFIRM} zero_cnt_kc={zero_cnt_kc}/{WATCHER_DETECT_CONFIRM}")

                if zero_cnt_bin >= WATCHER_DETECT_CONFIRM:
                    print(f"{datetime.now().isoformat()} Detected sustained ZERO on Binance -> targeted close KuCoin & cleanup.")
                    try:
                        ok = close_single_exchange_position(kucoin, kc_sym)
                        if not ok:
                            print(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} Error when closing after Binance sustained zero: {e}")
                    terminate_bot = True
                    break

                if zero_cnt_kc >= WATCHER_DETECT_CONFIRM:
                    print(f"{datetime.now().isoformat()} Detected sustained ZERO on KuCoin -> targeted close Binance & cleanup.")
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            print(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        print(f"{datetime.now().isoformat()} Error when closing after KuCoin sustained zero: {e}")
                    terminate_bot = True
                    break

                prev_bin = cur_bin_f
                prev_kc = cur_kc_f
                time.sleep(WATCHER_POLL_INTERVAL)
            except Exception as e:
                print(f"{datetime.now().isoformat()} Liquidation watcher exception for {sym}: {e}")
                time.sleep(0.5)

        _liquidation_watchers.pop(sym, None)
        print(f"{datetime.now().isoformat()} Liquidation watcher EXIT for {sym}")

    t = threading.Thread(target=monitor, daemon=True)
    t.start()

# Ctrl+E listener (simple)
def start_ctrl_e_listener():
    def _listener():
        global terminate_bot
        try:
            if os.name == 'nt':
                import msvcrt
                while True:
                    try:
                        if msvcrt.kbhit():
                            ch = msvcrt.getch()
                            if ch == b'\x05':
                                print(f"{datetime.now().isoformat()} Ctrl+E detected -> closing all positions now.")
                                try:
                                    close_all_and_wait()
                                except Exception as e:
                                    print(f"Error during Ctrl+E close_all_and_wait: {e}")
                                terminate_bot = True
                                break
                    except Exception as e:
                        print(f"Ctrl+E listener error (windows): {e}")
                    time.sleep(0.1)
            else:
                import termios, tty, select
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                try:
                    tty.setcbreak(fd)
                    while True:
                        try:
                            r, _, _ = select.select([sys.stdin], [], [], 0.1)
                            if r:
                                ch = sys.stdin.read(1)
                                if ch == '\x05':
                                    print(f"{datetime.now().isoformat()} Ctrl+E detected -> closing all positions now.")
                                    try:
                                        close_all_and_wait()
                                    except Exception as e:
                                        print(f"Error during Ctrl+E close_all_and_wait: {e}")
                                    terminate_bot = True
                                    break
                        except Exception as e:
                            print(f"Ctrl+E listener error (posix loop): {e}")
                            time.sleep(0.1)
                finally:
                    try:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    except Exception:
                        pass
        except Exception as e:
            print(f"Ctrl+E listener fatal error: {e}")
    t = threading.Thread(target=_listener, daemon=True)
    t.start()

start_ctrl_e_listener()

# Starting balances
start_total_balance, start_bin_balance, start_kc_balance = get_total_futures_balance()
print(f"Starting total balance approx: ${start_total_balance:.2f} (Binance: ${start_bin_balance:.2f} | KuCoin: ${start_kc_balance:.2f})\n")
print(f"{datetime.now()} BOT STARTED – monitoring {SYMBOLS} / {KUCOIN_SYMBOLS}\n")

# ----------------- MAIN LOOP -----------------
while True:
    try:
        if terminate_bot:
            print("Termination requested (liquidation protection triggered) — stopping bot.")
            try:
                close_all_and_wait()
            except Exception:
                pass
            break

        bin_prices, kc_prices = get_prices()
        for i, sym in enumerate(SYMBOLS):
            bin_bid, bin_ask = bin_prices.get(sym, (None, None))
            kc_bid, kc_ask = kc_prices.get(KUCOIN_SYMBOLS[i], (None, None))
            kc_trade_sym = KUCOIN_TRADE_SYMBOLS[i]
            if None in (bin_bid, bin_ask, kc_bid, kc_ask) or not kc_trade_sym:
                continue

            # Reset confirmations
            if bin_ask < kc_bid:
                trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                if trigger_spread < ENTRY_SPREAD:
                    entry_confirm_count[sym] = 0
            elif bin_bid > kc_ask:
                trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                if trigger_spread < ENTRY_SPREAD:
                    entry_confirm_count[sym] = 0

            # CASE A: Buy Binance, Sell KuCoin
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

                        notional_bin, notional_kc, bin_base_amt, kc_contracts = match_base_exposure_per_exchange(binance, kucoin, sym, kc_trade_sym, NOTIONAL, bin_ask, kc_bid)
                        print(f"Placing orders with adjusted notionals to match base exposure -> Binance notional: ${notional_bin:.6f} | KuCoin notional: ${notional_kc:.6f}")

                        results = {}
                        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_trade_sym, trigger_time=trigger_time, trigger_price=kc_bid)
                        def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, sym, trigger_time=trigger_time, trigger_price=bin_ask)
                        t1 = threading.Thread(target=exec_kc)
                        t2 = threading.Thread(target=exec_bin)
                        t1.start(); t2.start(); t1.join(); t2.join()
                        ok_kc, exec_price_kc, exec_time_kc, exec_qty_kc = results.get('kc', (False, None, None, None))
                        ok_bin, exec_price_bin, exec_time_bin, exec_qty_bin = results.get('bin', (False, None, None, None))

                        if ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None:
                            try:
                                market_bin = get_market(binance, sym)
                                bin_contract_size = float(market_bin.get('contractSize') or market_bin.get('info', {}).get('contractSize') or 1.0) if market_bin else 1.0
                                implied_bin = float(exec_qty_bin) * bin_contract_size * float(exec_price_bin) if exec_qty_bin is not None else compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)[1]
                                market_kc = get_market(kucoin, kc_trade_sym)
                                kc_contract_size = float(market_kc.get('contractSize') or market_kc.get('info', {}).get('contractSize') or 1.0) if market_kc else 1.0
                                implied_kc = float(exec_qty_kc) * kc_contract_size * float(exec_price_kc) if exec_qty_kc is not None else compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)[1]
                            except Exception:
                                implied_bin = compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)[1] if exec_price_bin else 0.0
                                implied_kc = compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)[1] if exec_price_kc else 0.0

                            mismatch_pct = abs(implied_bin - implied_kc) / max(implied_bin, implied_kc) * 100 if max(implied_bin, implied_kc) > 0 else 100
                            print(f"IMPLIED NOTIONALS | Binance: ${implied_bin:.6f} | KuCoin: ${implied_kc:.6f} | mismatch={mismatch_pct:.3f}%")

                            # Accept or rebalance (Binance only) then attempt stops always
                            if mismatch_pct <= MAX_NOTIONAL_MISMATCH_PCT:
                                real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                                final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                                entry_prices[sym]['kucoin'] = exec_price_kc
                                entry_prices[sym]['binance'] = exec_price_bin
                                entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc}
                                entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin}
                                entry_spreads[sym] = final_entry_spread
                                positions[sym] = 'caseA'
                                trade_start_balances[sym] = start_total_balance
                                entry_confirm_count[sym] = 0
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ENTRY ACCEPTED (no rebalance required).")
                                try:
                                    _start_liquidation_watcher_for_symbol(sym, sym, kc_trade_sym)
                                except Exception as e:
                                    print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
                                try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)

                            else:
                                diff_dollars = abs(implied_bin - implied_kc)
                                print(f"NOTIONAL MISMATCH {mismatch_pct:.3f}% -> diff ${diff_dollars:.6f}")
                                if diff_dollars >= REBALANCE_MIN_DOLLARS:
                                    print("Attempting rebalance ON BINANCE ONLY to match KuCoin (do not touch KuCoin).")
                                    reb_ok = False
                                    reb_exec_price = None
                                    reb_exec_qty = None
                                    try:
                                        if implied_bin < implied_kc:
                                            reb_ok, reb_exec_price, _, reb_exec_qty = safe_create_order(binance, 'buy', diff_dollars, exec_price_bin, sym)
                                        else:
                                            reb_ok, reb_exec_price, _, reb_exec_qty = safe_create_order(binance, 'sell', diff_dollars, exec_price_bin, sym)
                                    except Exception as e:
                                        print(f"Rebalance attempt on Binance failed with exception: {e}")
                                        reb_ok = False
                                    # Accept final and place stops
                                    real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                                    final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                                    entry_prices[sym]['kucoin'] = exec_price_kc
                                    entry_prices[sym]['binance'] = exec_price_bin
                                    entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc}
                                    entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin}
                                    entry_spreads[sym] = final_entry_spread
                                    positions[sym] = 'caseA'
                                    trade_start_balances[sym] = start_total_balance
                                    entry_confirm_count[sym] = 0
                                    print("Finalized entry after attempted Binance rebalance. Attempting stops now.")
                                    try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)
                                else:
                                    real_entry_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                                    final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                                    entry_prices[sym]['kucoin'] = exec_price_kc
                                    entry_prices[sym]['binance'] = exec_price_bin
                                    entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc}
                                    entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin}
                                    entry_spreads[sym] = final_entry_spread
                                    positions[sym] = 'caseA'
                                    trade_start_balances[sym] = start_total_balance
                                    entry_confirm_count[sym] = 0
                                    try:
                                        _start_liquidation_watcher_for_symbol(sym, sym, kc_trade_sym)
                                    except Exception as e:
                                        print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
                                    try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)
                        else:
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} WARNING: Partial or failed execution in Case A. Closing positions if any.")
                            close_all_and_wait()
                            entry_confirm_count[sym] = 0

            # CASE B: Sell Binance, Buy KuCoin
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

                        notional_bin, notional_kc, bin_base_amt, kc_contracts = match_base_exposure_per_exchange(binance, kucoin, sym, kc_trade_sym, NOTIONAL, bin_bid, kc_ask)
                        print(f"Placing orders with adjusted notionals to match base exposure -> Binance notional: ${notional_bin:.6f} | KuCoin notional: ${notional_kc:.6f}")

                        results = {}
                        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_trade_sym, trigger_time=trigger_time, trigger_price=kc_ask)
                        def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, sym, trigger_time=trigger_time, trigger_price=bin_bid)
                        t1 = threading.Thread(target=exec_kc)
                        t2 = threading.Thread(target=exec_bin)
                        t1.start(); t2.start(); t1.join(); t2.join()
                        ok_kc, exec_price_kc, exec_time_kc, exec_qty_kc = results.get('kc', (False, None, None, None))
                        ok_bin, exec_price_bin, exec_time_bin, exec_qty_bin = results.get('bin', (False, None, None, None))

                        if ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None:
                            try:
                                market_bin = get_market(binance, sym)
                                bin_contract_size = float(market_bin.get('contractSize') or market_bin.get('info', {}).get('contractSize') or 1.0) if market_bin else 1.0
                                implied_bin = float(exec_qty_bin) * bin_contract_size * float(exec_price_bin) if exec_qty_bin is not None else compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)[1]
                                market_kc = get_market(kucoin, kc_trade_sym)
                                kc_contract_size = float(market_kc.get('contractSize') or market_kc.get('info', {}).get('contractSize') or 1.0) if market_kc else 1.0
                                implied_kc = float(exec_qty_kc) * kc_contract_size * float(exec_price_kc) if exec_qty_kc is not None else compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)[1]
                            except Exception:
                                implied_bin = compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)[1] if exec_price_bin else 0.0
                                implied_kc = compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)[1] if exec_price_kc else 0.0

                            mismatch_pct = abs(implied_bin - implied_kc) / max(implied_bin, implied_kc) * 100 if max(implied_bin, implied_kc) > 0 else 100
                            print(f"IMPLIED NOTIONALS | Binance: ${implied_bin:.6f} | KuCoin: ${implied_kc:.6f} | mismatch={mismatch_pct:.3f}%")

                            if mismatch_pct <= MAX_NOTIONAL_MISMATCH_PCT:
                                real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                                final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                                entry_prices[sym]['kucoin'] = exec_price_kc
                                entry_prices[sym]['binance'] = exec_price_bin
                                entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc}
                                entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin}
                                entry_spreads[sym] = final_entry_spread
                                positions[sym] = 'caseB'
                                trade_start_balances[sym] = start_total_balance
                                entry_confirm_count[sym] = 0
                                print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ENTRY ACCEPTED (no rebalance required).")
                                try:
                                    _start_liquidation_watcher_for_symbol(sym, sym, kc_trade_sym)
                                except Exception as e:
                                    print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
                                try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)

                            else:
                                diff_dollars = abs(implied_bin - implied_kc)
                                print(f"NOTIONAL MISMATCH {mismatch_pct:.3f}% -> diff ${diff_dollars:.6f}")
                                if diff_dollars >= REBALANCE_MIN_DOLLARS:
                                    print("Attempting rebalance ON BINANCE ONLY to match KuCoin (do not touch KuCoin).")
                                    reb_ok = False
                                    try:
                                        if implied_bin < implied_kc:
                                            reb_ok, _, _, _ = safe_create_order(binance, 'sell', diff_dollars, exec_price_bin, sym)
                                        else:
                                            reb_ok, _, _, _ = safe_create_order(binance, 'buy', diff_dollars, exec_price_bin, sym)
                                    except Exception as e:
                                        print(f"Rebalance attempt on Binance failed with exception: {e}")
                                        reb_ok = False
                                    real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                                    final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                                    entry_prices[sym]['kucoin'] = exec_price_kc
                                    entry_prices[sym]['binance'] = exec_price_bin
                                    entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc}
                                    entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin}
                                    entry_spreads[sym] = final_entry_spread
                                    positions[sym] = 'caseB'
                                    trade_start_balances[sym] = start_total_balance
                                    entry_confirm_count[sym] = 0
                                    print("Finalized entry after attempted Binance rebalance. Attempting stops now.")
                                    try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)
                                else:
                                    real_entry_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                                    final_entry_spread = trigger_spread if real_entry_spread >= trigger_spread else real_entry_spread
                                    entry_prices[sym]['kucoin'] = exec_price_kc
                                    entry_prices[sym]['binance'] = exec_price_bin
                                    entry_actual[sym]['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': exec_time_kc, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc}
                                    entry_actual[sym]['binance'] = {'exec_price': exec_price_bin, 'exec_time': exec_time_bin, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin}
                                    entry_spreads[sym] = final_entry_spread
                                    positions[sym] = 'caseB'
                                    trade_start_balances[sym] = start_total_balance
                                    entry_confirm_count[sym] = 0
                                    print("Mismatch below REBALANCE_MIN_DOLLARS — accepting small residual exposure and proceeding. Attempting stops now.")
                                    try:
                                        _start_liquidation_watcher_for_symbol(sym, sym, kc_trade_sym)
                                    except Exception as e:
                                        print(f"{datetime.now().isoformat()} Failed to start liquidation watcher: {e}")
                                    try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)
                        else:
                            print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} WARNING: Partial or failed execution in Case B. Closing positions if any.")
                            close_all_and_wait()
                            entry_confirm_count[sym] = 0

            # EXIT logic
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
