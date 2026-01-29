#!/usr/bin/env python3
# Integrated Arb Bot - final with 3-confirm and pre/post balance snapshots
# WARNING: Live trading bot. Test carefully.

import os
import sys
import time
import math
import requests
import threading
from datetime import datetime
from dotenv import load_dotenv
import ccxt
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Required env keys
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

# Config (defaults)
NOTIONAL = float(os.getenv('NOTIONAL', "10.0"))
LEVERAGE = int(os.getenv('LEVERAGE', "5"))
WATCHER_POLL_INTERVAL = float(os.getenv('WATCHER_POLL_INTERVAL', "0.5"))
WATCHER_DETECT_CONFIRM = int(os.getenv('WATCHER_DETECT_CONFIRM', "2"))
MAX_NOTIONAL_MISMATCH_PCT = float(os.getenv('MAX_NOTIONAL_MISMATCH_PCT', "0.5"))
REBALANCE_MIN_DOLLARS = float(os.getenv('REBALANCE_MIN_DOLLARS', "0.5"))
STOP_BUFFER_PCT = float(os.getenv('STOP_BUFFER_PCT', "0.02"))
KC_TRANSIENT_ERROR_THRESHOLD = int(os.getenv('KC_TRANSIENT_ERROR_THRESHOLD', "10"))
KC_TRANSIENT_BACKOFF_SECONDS = float(os.getenv('KC_TRANSIENT_BACKOFF_SECONDS', "2.0"))

# Strategy-specific params (as agreed)
FR_ADVANTAGE_THRESHOLD = 0.3
CASE1_MIN_SPREAD = 1.5
CASE2_MULT = 13.0
CASE3_MULT = 15.0
BIG_SPREAD_THRESHOLD = 7.5
BIG_SPREAD_ENTRY_MULTIPLIER = 2
MAX_AVERAGES = 2
AVERAGE_TRIGGER_MULTIPLIER = 2.0
TAKE_PROFIT_FACTOR = 0.5
MIN_FR_DIFF_THRESHOLD = 0.12  # NEW: Minimum funding rate difference to consider (ignore below this)

SCAN_THRESHOLD = 0.25
ALERT_THRESHOLD = 5.0
ALERT_COOLDOWN = 60
CONFIRM_RETRY_DELAY = 0.5
CONFIRM_RETRIES = 2
MONITOR_DURATION = 60
MONITOR_POLL = 2
MAX_WORKERS = 12
SCANNER_FULL_INTERVAL = 120

# Telegram (kept)
TELEGRAM_TOKEN = "8589870096:AAHahTpg6LNXbUwUMdt3q2EqVa2McIo14h8"
TELEGRAM_CHAT_IDS = ["5054484162", "497819952"]

# Live mode (per your request)
DRY_RUN = False

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

# Logging - FORCED STDOUT FOR RAILWAY
import sys
sys.stdout = sys.stderr  # Force everything to stderr (Railway captures this)
sys.stdout.reconfigure(line_buffering=True)  # Unbuffered
sys.stderr.reconfigure(line_buffering=True)

logger = logging.getLogger("arb_integrated")
logger.setLevel(logging.DEBUG)
logger.propagate = True

# Console handler - FORCE to stdout/stderr
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(ch)

# File handler
fh = RotatingFileHandler("arb_integrated.log", maxBytes=8_000_000, backupCount=5)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
logger.addHandler(fh)

# CRITICAL: Force flush after every log
class FlushHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

ch2 = FlushHandler(sys.stderr)
ch2.setLevel(logging.INFO)
ch2.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(ch2)

# Print to verify logging works
print("=" * 80, flush=True)
print("LOGGING INITIALIZED - YOU SHOULD SEE THIS IN RAILWAY", flush=True)
print("=" * 80, flush=True)

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def send_telegram(message, chat_ids=None):
    if chat_ids is None:
        chat_ids = TELEGRAM_CHAT_IDS
    for chat_id in chat_ids:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            resp = requests.get(url, params={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
            if resp.status_code != 200:
                logger.warning("Telegram non-200 response: %s %s", resp.status_code, resp.text[:200])
        except Exception:
            logger.exception("Failed to send Telegram message")

# Spreadwatcher endpoints & helpers (kept)
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
KUCOIN_ACTIVE_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
KUCOIN_TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"
BINANCE_FUND_URL = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
KUCOIN_FUND_URL = "https://api-futures.kucoin.com/api/v1/funding-rate?symbol={symbol}"

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
            syms = [s["symbol"] for s in data.get("symbols", []) if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
            logger.debug("[BINANCE] fetched %d symbols", len(syms))
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
            logger.debug("[KUCOIN] fetched %d symbols", len(syms))
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
    logger.info("Common symbols: %d", len(common))
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
                return None, None
            d = r.json()
            bid = float(d.get("bidPrice") or 0)
            ask = float(d.get("askPrice") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
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
                return None, None
            data = r.json()
            d = data.get("data", {}) if isinstance(data, dict) else {}
            bid = float(d.get("bestBidPrice") or d.get("bid") or 0)
            ask = float(d.get("bestAskPrice") or d.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
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

# Funding rate helpers
def fetch_binance_funding(symbol):
    try:
        r = requests.get(BINANCE_FUND_URL.format(symbol=symbol), timeout=6)
        if r.status_code != 200:
            return None
        d = r.json()
        fr_raw = d.get('fundingRate') or d.get('lastFundingRate')
        nft = d.get('nextFundingTime') or d.get('nextFundingRateTime')
        fr = None
        if fr_raw is not None:
            fr = float(fr_raw) * 100.0 if abs(float(fr_raw)) < 1.0 else float(fr_raw)
        next_ft = int(nft) if nft else None
        return {'fundingRatePct': fr, 'nextFundingTimeMs': next_ft}
    except Exception:
        logger.exception("fetch_binance_funding error for %s", symbol)
        return None

def fetch_kucoin_funding(symbol):
    try:
        try:
            fr_info = kucoin.fetchFundingRate(symbol)
            if fr_info:
                fr = fr_info.get('fundingRate') or fr_info.get('info', {}).get('fundingRate')
                next_ft = fr_info.get('nextFundingTime') or fr_info.get('info', {}).get('nextFundingTime')
                fr_pct = None
                if fr is not None:
                    fr_pct = float(fr) * 100.0 if abs(float(fr)) < 1.0 else float(fr)
                return {'fundingRatePct': fr_pct, 'nextFundingTimeMs': int(next_ft) if next_ft else None}
        except Exception:
            pass
        r = requests.get(KUCOIN_FUND_URL.format(symbol=symbol), timeout=6)
        if r.status_code != 200:
            return None
        d = r.json()
        data = d.get('data') or {}
        fr_raw = data.get('fundingRate') or data.get('fundingRate24h')
        nft = data.get('nextFundingTime')
        fr = None
        if fr_raw is not None:
            fr = float(fr_raw) * 100.0 if abs(float(fr_raw)) < 1.0 else float(fr_raw)
        next_ft = int(nft) if nft else None
        return {'fundingRatePct': fr, 'nextFundingTimeMs': next_ft}
    except Exception:
        logger.exception("fetch_kucoin_funding error for %s", symbol)
        return None

def compute_net_funding_for_plan(bin_fr_pct, kc_fr_pct, plan_long='binance', plan_short='kucoin'):
    try:
        fb = bin_fr_pct or 0.0
        fk = kc_fr_pct or 0.0
        if plan_long == 'binance' and plan_short == 'kucoin':
            return fk - fb
        elif plan_long == 'kucoin' and plan_short == 'binance':
            return fb - fk
        return 0.0
    except Exception:
        return 0.0

# Spreadeater helpers (preserved)
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

ensure_markets_loaded()

def set_leverage_for_symbol(exchange, symbol):
    try:
        if exchange.id == 'binance':
            exchange.set_leverage(LEVERAGE, symbol)
            logger.info("Set leverage %dx for %s on Binance", LEVERAGE, symbol)
        else:
            try:
                exchange.set_leverage(LEVERAGE, symbol, {'marginMode': 'cross'})
                logger.info("Set leverage %dx for %s on KuCoin", LEVERAGE, symbol)
            except Exception:
                exchange.set_leverage(LEVERAGE, symbol)
                logger.info("Set leverage %dx for %s on KuCoin (fallback)", LEVERAGE, symbol)
    except Exception:
        logger.exception("Leverage set error for %s on %s", symbol, exchange.id)

# Position & close helpers (preserved)
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

def fetch_position_info(exchange, symbol):
    """
    FIXED: Enhanced liquidation price extraction for Binance
    """
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
    
    # FIXED: Enhanced liquidation price extraction
    liq = None
    info = pos.get('info') or {}
    
    # For Binance, try multiple possible fields in the correct order
    if exchange.id == 'binance':
        # First try the direct liquidationPrice field from info
        for fld in ('liquidationPrice', 'liquidation_price', 'liquidationPriceUsd', 'liquidation'):
            if fld in info and info.get(fld):
                try:
                    val = info.get(fld)
                    # Convert string "0" or 0 to None (means no liquidation price set yet)
                    if val and str(val) not in ('0', '0.0', '0.00'):
                        liq = float(val)
                        logger.info("BINANCE LIQUIDATION EXTRACTED from info.%s: %s", fld, liq)
                        break
                except Exception as e:
                    logger.debug("Failed to parse liquidation from info.%s: %s", fld, e)
                    pass
        
        # If still None, try from the top-level pos object
        if liq is None:
            for fld in ('liquidationPrice', 'liquidation_price', 'liquidationPriceUsd', 'liquidation'):
                if fld in pos and pos.get(fld):
                    try:
                        val = pos.get(fld)
                        if val and str(val) not in ('0', '0.0', '0.00'):
                            liq = float(val)
                            logger.info("BINANCE LIQUIDATION EXTRACTED from pos.%s: %s", fld, liq)
                            break
                    except Exception as e:
                        logger.debug("Failed to parse liquidation from pos.%s: %s", fld, e)
                        pass
    else:
        # For KuCoin and other exchanges
        for fld in ('liquidationPrice', 'liquidation_price', 'liquidationPriceUsd', 'liquidation'):
            if fld in pos and pos.get(fld):
                try:
                    val = pos.get(fld)
                    if val and str(val) not in ('0', '0.0', '0.00'):
                        liq = float(val)
                        break
                except Exception:
                    pass
            if fld in info and info.get(fld):
                try:
                    val = info.get(fld)
                    if val and str(val) not in ('0', '0.0', '0.00'):
                        liq = float(val)
                        break
                except Exception:
                    pass
    
    # Final fallback: check if position dict exists
    if liq is None:
        try:
            if isinstance(info.get('position'), dict) and info.get('position').get('liquidationPrice'):
                val = info.get('position').get('liquidationPrice')
                if val and str(val) not in ('0', '0.0', '0.00'):
                    liq = float(val)
        except Exception:
            pass
    
    side = 'long' if signed > 0 else ('short' if signed < 0 else None)
    
    logger.info("POSITION INFO for %s %s: signed_qty=%s, liquidationPrice=%s, side=%s", 
                exchange.id, symbol, signed, liq, side)
    
    return {'signed_qty': float(signed or 0.0), 'liquidationPrice': liq, 'side': side}

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
            logger.info(f"{datetime.now().isoformat()} No positions returned for {exchange.id} {symbol} (treated as already closed).")
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
            logger.info(f"{datetime.now().isoformat()} No open qty to close for {exchange.id} {symbol}.")
            return True
        side = 'sell' if raw_signed > 0 else 'buy'
        qty = abs(raw_signed)
        market = get_market(exchange, symbol)
        prec = market.get('precision', {}).get('amount') if market else None
        qty_rounded = round_down(qty, prec) if prec is not None else qty
        if qty_rounded > 0:
            try:
                logger.info(f"{datetime.now().isoformat()} Submitting targeted reduceOnly market close on {exchange.id} {symbol} -> {side} {qty_rounded}")
                try:
                    exchange.create_market_order(symbol, side, qty_rounded, params={'reduceOnly': True, 'marginMode': 'cross'})
                except TypeError:
                    exchange.create_order(symbol=symbol, type='market', side=side, amount=qty_rounded, params={'reduceOnly': True})
                logger.info(f"{datetime.now().isoformat()} Targeted reduceOnly close submitted on {exchange.id} {symbol}")
                return True
            except Exception as e:
                logger.info(f"{datetime.now().isoformat()} Targeted reduceOnly close failed on {exchange.id} {symbol}: {e}")
                try:
                    logger.info(f"{datetime.now().isoformat()} Trying closePosition fallback on {exchange.id} {symbol}")
                    try:
                        exchange.create_order(symbol=symbol, type='market', side=side, amount=None, params={'closePosition': True})
                    except TypeError:
                        exchange.create_order(symbol, 'market', side, params={'closePosition': True})
                    logger.info(f"{datetime.now().isoformat()} closePosition fallback submitted on {exchange.id} {symbol}")
                    return True
                except Exception as e2:
                    logger.info(f"{datetime.now().isoformat()} closePosition fallback failed on {exchange.id} {symbol}: {e2}")
                    return False
        else:
            try:
                logger.info(f"{datetime.now().isoformat()} qty rounded to 0, using closePosition fallback on {exchange.id} {symbol}")
                try:
                    exchange.create_order(symbol=symbol, type='market', side=side, amount=None, params={'closePosition': True})
                except TypeError:
                    exchange.create_order(symbol, 'market', side, params={'closePosition': True})
                logger.info(f"{datetime.now().isoformat()} closePosition fallback submitted on {exchange.id} {symbol}")
                return True
            except Exception as e:
                logger.info(f"{datetime.now().isoformat()} closePosition fallback failed on {exchange.id} {symbol}: {e}")
                return False
    except Exception as e:
        logger.exception("Error in close_single_exchange_position(%s,%s): %s", exchange.id, symbol, e)
        return False

def close_all_and_wait(timeout_s=20, poll_interval=0.5):
    global closing_in_progress
    closing_in_progress = True
    logger.info("Closing all positions...")
    # Close on Binance and KuCoin
    try:
        all_bin_pos = binance.fetch_positions()
    except Exception:
        all_bin_pos = []
    for pos in all_bin_pos:
        try:
            sym = pos.get('symbol') or (pos.get('info') or {}).get('symbol')
            if not sym:
                continue
            signed = _get_signed_from_binance_pos(pos)
            if abs(float(signed or 0)) == 0:
                continue
            side = 'sell' if signed > 0 else 'buy'
            market = get_market(binance, sym)
            prec = market.get('precision', {}).get('amount') if market else None
            qty = round_down(abs(signed), prec) if prec is not None else abs(signed)
            if qty > 0:
                try:
                    logger.info(f"Closing Binance {sym} {side} {qty}")
                    try:
                        binance.create_market_order(sym, side, qty, params={'reduceOnly': True})
                    except TypeError:
                        binance.create_order(symbol=sym, type='market', side=side, amount=qty, params={'reduceOnly': True})
                except Exception:
                    logger.exception("Error closing binance position %s", sym)
        except Exception:
            continue
    try:
        all_kc_pos = kucoin.fetch_positions()
    except Exception:
        all_kc_pos = []
    for pos in all_kc_pos:
        try:
            sym = pos.get('symbol') or (pos.get('info') or {}).get('symbol')
            if not sym:
                continue
            signed = _get_signed_from_kucoin_pos(pos)
            if abs(float(signed or 0)) == 0:
                continue
            side = 'sell' if signed > 0 else 'buy'
            market = get_market(kucoin, sym)
            prec = market.get('precision', {}).get('amount') if market else None
            qty = round_down(abs(signed), prec) if prec is not None else abs(signed)
            if qty > 0:
                try:
                    logger.info(f"Closing KuCoin {sym} {side} {qty}")
                    kucoin.create_market_order(sym, side, qty, params={'reduceOnly': True, 'marginMode': 'cross'})
                except Exception:
                    logger.exception("Error closing kucoin position %s", sym)
        except Exception:
            continue
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            bin_check = binance.fetch_positions()
            kc_check = kucoin.fetch_positions()
            any_open = False
            for p in (bin_check or []):
                if abs(float(_get_signed_from_binance_pos(p) or 0)) > 0:
                    any_open = True
                    break
            if not any_open:
                for p in (kc_check or []):
                    if abs(float(_get_signed_from_kucoin_pos(p) or 0)) > 0:
                        any_open = True
                        break
            logger.info("Checking open positions... any_open=%s", any_open)
            if not any_open:
                closing_in_progress = False
                total_bal, bin_bal, kc_bal = get_total_futures_balance()
                logger.info("*** POST-TRADE Total Balance approx: ${:.2f} (Binance: ${:.2f} | KuCoin: ${:.2f}) ***".format(total_bal, bin_bal, kc_bal))
                return True
        except Exception:
            logger.exception("Error checking open positions in close_all_and_wait")
        time.sleep(poll_interval)
    closing_in_progress = False
    logger.info("Timeout waiting for positions to close.")
    return False

# Exec price/time/qty extractor and KuCoin poller (preserved)
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

def safe_create_order(exchange, side, notional, price, symbol, trigger_time=None, trigger_price=None):
    amt, _, _, prec = compute_amount_for_notional(exchange, symbol, notional, price)
    amt = round_down(amt, prec) if prec is not None else amt
    if amt <= 0:
        logger.info(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} computed amt <=0, skipping order for {exchange.id} {symbol} (notional=${notional} price={price})")
        return False, None, None, None
    if DRY_RUN:
        logger.info(f"DRY RUN: would place {side} {amt} {symbol} on {exchange.id} at notional ${notional}")
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
            exec_price = mid
            exec_time = datetime.utcnow().isoformat() + 'Z'
            executed_qty = amt
            msg = f"*DRY RUN TRADE (simulated)* on `{exchange.id}` — {side.upper()} {symbol}\nprice: `{exec_price}`\nqty: `{executed_qty}`\nnotional: `{notional}`\n{timestamp()}"
            send_telegram(msg)
            return True, exec_price, exec_time, executed_qty
        except Exception:
            return True, price, datetime.utcnow().isoformat() + 'Z', amt
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

        if exchange.id != 'binance':
            try:
                px2, ts2, qty2 = poll_kucoin_until_filled(exchange, order, symbol, timeout_s=8.0, poll_interval=0.6)
                if px2 is not None:
                    exec_price = px2
                if ts2 is not None:
                    exec_time = ts2
                if qty2 is not None:
                    executed_qty = qty2
            except Exception:
                pass

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

        implied_exec_notional = None
        try:
            market = get_market(exchange, symbol)
            contract_size = float(market.get('contractSize') or market.get('info', {}).get('contractSize') or 1.0) if market else 1.0
            if executed_qty is not None and exec_price is not None:
                implied_exec_notional = float(executed_qty) * float(contract_size) * float(exec_price)
        except Exception:
            implied_exec_notional = None

        logger.info(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {exchange.id.upper()} ORDER EXECUTED | {side.upper()} {symbol} | exec_price={exec_price} executed_qty={executed_qty} implied_notional={implied_exec_notional}")
        msg = f"*TRADE EXECUTED* on `{exchange.id}` — {side.upper()} {symbol}\nprice: `{exec_price}`\nqty: `{executed_qty}`\nnotional: `{implied_exec_notional}`\n{timestamp()}"
        send_telegram(msg)
        return True, exec_price, exec_time, executed_qty
    except Exception:
        logger.exception("%s order failed", exchange.id.upper())
        return False, None, None, None

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

def get_prices_for_symbol(sym, ku_api_sym):
    bin_bid = bin_ask = kc_bid = kc_ask = None
    try:
        data = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
        for item in data:
            if item['symbol'] == sym:
                bin_bid = float(item['bidPrice']); bin_ask = float(item['askPrice'])
                break
        resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={ku_api_sym}", timeout=5).json()
        d = resp.get('data', {})
        kc_bid = float(d.get('bestBidPrice', '0') or 0); kc_ask = float(d.get('bestAskPrice', '0') or 0)
    except Exception:
        pass
    return bin_bid, bin_ask, kc_bid, kc_ask

# Liquidation watcher adapted
_liquidation_watchers = {}

def _fetch_signed_binance(sym):
    try:
        p = binance.fetch_positions([sym])
        if not p:
            return 0.0
        pos = p[0]
        return _get_signed_from_binance_pos(pos)
    except Exception as e:
        logger.info(f"{datetime.utcnow().isoformat()} BINANCE fetch error for {sym}: {e}")
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

def _start_liquidation_watcher_for_symbols(sym, bin_sym, kc_ccxt_sym):
    key = f"{sym}:{bin_sym}:{kc_ccxt_sym}"
    if _liquidation_watchers.get(key):
        return
    stop_flag = threading.Event()
    _liquidation_watchers[key] = stop_flag

    def monitor():
        logger.info(f"{datetime.now().isoformat()} Liquidation watcher STARTED for {sym} (bin:{bin_sym} kc:{kc_ccxt_sym})")
        prev_bin = _last_known_positions.get(sym, {}).get('bin', 0.0)
        prev_kc = _last_known_positions.get(sym, {}).get('kc', 0.0)
        zero_cnt_bin = 0
        zero_cnt_kc = 0
        ZERO_ABS_THRESHOLD = 1e-12
        entry_bin_initial = entry_initial_qtys.get(sym, {}).get('bin', 0.0)
        entry_kc_initial = entry_initial_qtys.get(sym, {}).get('kc', 0.0)
        seen_nonzero_bin = abs(prev_bin) > ZERO_ABS_THRESHOLD or abs(entry_bin_initial) > ZERO_ABS_THRESHOLD
        seen_nonzero_kc = abs(prev_kc) > ZERO_ABS_THRESHOLD or abs(entry_kc_initial) > ZERO_ABS_THRESHOLD
        logger.info(f"{datetime.now().isoformat()} Watcher initial prev_bin={prev_bin} prev_kc={prev_kc} entry_bin_initial={entry_bin_initial} entry_kc_initial={entry_kc_initial}")
        while not stop_flag.is_set():
            try:
                if closing_in_progress or positions.get(sym) is None:
                    zero_cnt_bin = zero_cnt_kc = 0
                    if positions.get(sym) is None:
                        logger.info(f"{datetime.now().isoformat()} Liquidation watcher stopping for {sym} because positions[sym] is None")
                        break
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue
                cur_bin = _fetch_signed_binance(bin_sym)
                if cur_bin is None:
                    cur_bin = _last_known_positions.get(sym, {}).get('bin', 0.0)
                    logger.info(f"{datetime.now().isoformat()} WATCHER BINANCE fetch error -> using last-known bin qty {cur_bin}")
                else:
                    _last_known_positions.setdefault(sym, {})['bin'] = cur_bin
                cur_kc = None
                try:
                    cur_kc = _fetch_signed_kucoin(kc_ccxt_sym)
                    _last_known_positions.setdefault(sym, {})['kc'] = cur_kc
                    _last_known_positions.setdefault(sym, {})['kc_err_count'] = 0
                except Exception as e:
                    _last_known_positions.setdefault(sym, {})['kc_err_count'] = _last_known_positions.setdefault(sym, {}).get('kc_err_count', 0) + 1
                    logger.info(f"{datetime.now().isoformat()} KUCOIN fetch error for {kc_ccxt_sym}: {e} (consecutive_errors={_last_known_positions[sym]['kc_err_count']})")
                    cur_kc = _last_known_positions.get(sym, {}).get('kc', None)
                    if _last_known_positions[sym]['kc_err_count'] >= KC_TRANSIENT_ERROR_THRESHOLD:
                        logger.info(f"{datetime.now().isoformat()} KUCOIN persistent errors >= {KC_TRANSIENT_ERROR_THRESHOLD}; backing off")
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
                    logger.info(f"{datetime.now().isoformat()} WATCHER SKIP (no KuCoin reading available) prev_bin={prev_bin} prev_kc={prev_kc} cur_bin={cur_bin_f} cur_kc=None")
                    prev_bin = cur_bin_f
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue
                cur_bin_abs = abs(cur_bin_f)
                cur_kc_abs = abs(cur_kc_f)
                prev_bin_abs = abs(prev_bin)
                prev_kc_abs = abs(prev_kc)
                bin_initial_nonzero = abs(entry_initial_qtys.get(sym, {}).get('bin', 0.0)) > ZERO_ABS_THRESHOLD
                kc_initial_nonzero = abs(entry_initial_qtys.get(sym, {}).get('kc', 0.0)) > ZERO_ABS_THRESHOLD
                if (prev_bin_abs > ZERO_ABS_THRESHOLD or bin_initial_nonzero) and cur_bin_abs <= ZERO_ABS_THRESHOLD:
                    logger.info(f"{datetime.now().isoformat()} Detected immediate ZERO on Binance (prev non-zero -> now zero). Triggering immediate targeted close of KuCoin.")
                    try:
                        ok = close_single_exchange_position(kucoin, kc_ccxt_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after Binance immediate zero: %s", e)
                    global terminate_bot
                    terminate_bot = True
                    break
                if (prev_kc_abs > ZERO_ABS_THRESHOLD or kc_initial_nonzero) and cur_kc_abs <= ZERO_ABS_THRESHOLD:
                    logger.info(f"{datetime.now().isoformat()} Detected immediate ZERO on KuCoin (prev non-zero -> now zero). Triggering immediate targeted close of Binance.")
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after KuCoin immediate zero: %s", e)
                    terminate_bot = True
                    break
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
                logger.debug(f"{datetime.now().isoformat()} WATCHER {sym} prev_bin={prev_bin_abs:.6f} cur_bin={cur_bin_abs:.6f} prev_kc={prev_kc_abs:.6f} cur_kc={cur_kc_abs:.6f} zero_cnt_bin={zero_cnt_bin}/{WATCHER_DETECT_CONFIRM} zero_cnt_kc={zero_cnt_kc}/{WATCHER_DETECT_CONFIRM}")
                if zero_cnt_bin >= WATCHER_DETECT_CONFIRM:
                    logger.info(f"{datetime.now().isoformat()} Detected sustained ZERO on Binance -> targeted close KuCoin & cleanup.")
                    try:
                        ok = close_single_exchange_position(kucoin, kc_ccxt_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after Binance sustained zero: %s", e)
                    terminate_bot = True
                    break
                if zero_cnt_kc >= WATCHER_DETECT_CONFIRM:
                    logger.info(f"{datetime.now().isoformat()} Detected sustained ZERO on KuCoin -> targeted close Binance & cleanup.")
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after KuCoin sustained zero: %s", e)
                    terminate_bot = True
                    break
                prev_bin = cur_bin_f
                prev_kc = cur_kc_f
                time.sleep(WATCHER_POLL_INTERVAL)
            except Exception as e:
                logger.exception("Liquidation watcher exception for %s: %s", sym, e)
                time.sleep(0.5)
        _liquidation_watchers.pop(key, None)
        logger.info(f"{datetime.now().isoformat()} Liquidation watcher EXIT for {sym} (key={key})")

    t = threading.Thread(target=monitor, daemon=True)
    t.start()

# State
closing_in_progress = False
positions = {}
entry_spreads = {}
entry_prices = {}
entry_actual = {}
_last_known_positions = {}
entry_initial_qtys = {}

# Active trade structure
active_trade = {
    'symbol': None,
    'ku_api': None,
    'ku_ccxt': None,
    'case': None,
    'entry_spread': None,
    'avg_entry_spread': None,
    'entry_prices': None,
    'notional': None,
    'averages_done': 0,
    'final_implied_notional': None,
    'funding_accumulated_pct': 0.0,
    'funding_rounds_seen': set(),
    'suppress_full_scan': False
}

# Confirm counters per symbol (consecutive confirms)
confirm_counts = {}
CONFIRM_COUNT = 3

# Scanner shared candidates
candidates_shared_lock = threading.Lock()
candidates_shared = {}

# Scanner thread (keeps original behaviour)
def spread_scanner_loop():
    http_session = requests.Session()
    last_alert = {}
    while True:
        try:
            print(f"\n{'='*60}", flush=True)
            print(f"🔍 SCANNER ITERATION - {timestamp()}", flush=True)
            print(f"{'='*60}", flush=True)
            
            common_symbols, ku_map = get_common_symbols()
            if not common_symbols:
                print("⚠️  No common symbols found, sleeping...", flush=True)
                time.sleep(5)
                continue
            
            print(f"✓ Common symbols: {len(common_symbols)}", flush=True)
            logger.info(f"Scanner: {len(common_symbols)} common symbols")
            
            bin_book = get_binance_book()
            print(f"✓ Binance book: {len(bin_book)} entries", flush=True)
            
            ku_symbols = [ku_map.get(sym, sym + "M") for sym in common_symbols]
            ku_prices = threaded_kucoin_prices(ku_symbols)
            print(f"✓ KuCoin prices: {len(ku_prices)}/{len(ku_symbols)}", flush=True)
            
            new_candidates = {}
            for sym in common_symbols:
                bin_tick = bin_book.get(sym)
                ku_sym = ku_map.get(sym, sym + "M")
                ku_tick = ku_prices.get(ku_sym)
                if not bin_tick or not ku_tick:
                    continue
                spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], ku_tick["bid"], ku_tick["ask"])
                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    new_candidates[sym] = {
                        "ku_sym": ku_sym,
                        "start_spread": spread,
                        "max_spread": spread,
                        "min_spread": spread,
                        "alerted": False,
                        "bin_bid": bin_tick["bid"],
                        "bin_ask": bin_tick["ask"],
                        "ku_bid": ku_tick["bid"],
                        "ku_ask": ku_tick["ask"]
                    }
            with candidates_shared_lock:
                candidates_shared.clear()
                candidates_shared.update(new_candidates)
            
            print(f"✅ CANDIDATES: {len(new_candidates)}", flush=True)
            logger.info("[%s] Scanner: shortlisted %d candidate(s)", timestamp(), len(new_candidates))
            
            # Show top 5 candidates
            if new_candidates:
                sorted_cands = sorted(new_candidates.items(), key=lambda x: abs(x[1]['start_spread']), reverse=True)[:5]
                for sym, info in sorted_cands:
                    print(f"   → {sym}: {info['start_spread']:+.4f}%", flush=True)
                    logger.info(f"   Candidate: {sym} {info['start_spread']:+.4f}%")
            
            if not new_candidates:
                print(f"No candidates, sleeping {MONITOR_DURATION}s...", flush=True)
                time.sleep(max(1, MONITOR_DURATION))
                continue
            window_start = time.time()
            window_end = window_start + MONITOR_DURATION
            while time.time() < window_end and new_candidates:
                round_start = time.time()
                workers = min(MAX_WORKERS, max(4, len(new_candidates)))
                latest = {s: {"bin": None, "ku": None} for s in list(new_candidates.keys())}
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    fut_map = {}
                    for sym, info in list(new_candidates.items()):
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
                for sym in list(new_candidates.keys()):
                    info = new_candidates.get(sym)
                    if not info:
                        continue
                    b = latest[sym].get("bin")
                    k = latest[sym].get("ku")
                    if not b or not k:
                        continue
                    spread = calculate_spread(b["bid"], b["ask"], k["bid"], k["ask"])
                    if spread is None:
                        continue
                    if spread > info["max_spread"]:
                        new_candidates[sym]["max_spread"] = spread
                    if spread < info["min_spread"]:
                        new_candidates[sym]["min_spread"] = spread
                    if abs(spread) >= ALERT_THRESHOLD:
                        now = time.time()
                        cooldown_ok = (sym not in last_alert) or (now - last_alert[sym] > ALERT_COOLDOWN)
                        if not cooldown_ok:
                            new_candidates[sym]["alerted"] = True
                            continue
                        confirmed = False
                        for attempt in range(CONFIRM_RETRIES):
                            time.sleep(CONFIRM_RETRY_DELAY)
                            b2_bid, b2_ask = get_binance_price(sym, http_session, retries=1)
                            k2_bid, k2_ask = get_kucoin_price_once(info["ku_sym"], http_session, retries=1)
                            if b2_bid and b2_ask and k2_bid and k2_ask:
                                spread2 = calculate_spread(b2_bid, b2_ask, k2_bid, k2_ask)
                                if spread2 is not None and abs(spread2) >= ALERT_THRESHOLD:
                                    confirmed = True
                                    b_confirm, k_confirm = {"bid": b2_bid, "ask": b2_ask}, {"bid": k2_bid, "ask": k2_ask}
                                    break
                        if not confirmed:
                            logger.info("False positive avoided for %s (initial %.4f%%)", sym, spread)
                            new_candidates[sym]["alerted"] = False
                            continue
                        direction = "Long Binance / Short KuCoin" if spread2 > 0 else "Long KuCoin / Short Binance"
                        msg = (
                            f"*BIG SPREAD ALERT*\n"
                            f"`{sym}` → *{spread2:+.4f}%*\n"
                            f"Direction → {direction}\n"
                            f"Binance: `{b_confirm['bid']:.6f}` ↔ `{b_confirm['ask']:.6f}`\n"
                            f"KuCoin : `{k_confirm['bid']:.6f}` ↔ `{k_confirm['ask']:.6f}`\n"
                            f"{timestamp()}"
                        )
                        send_telegram(msg)
                        logger.info("ALERT → %s %+.4f%% (confirmed)", sym, spread2)
                        last_alert[sym] = time.time()
                        new_candidates.pop(sym, None)
                        with candidates_shared_lock:
                            if sym in candidates_shared:
                                candidates_shared[sym]['alerted'] = True
                elapsed = time.time() - round_start
                sleep_for = MONITOR_POLL - elapsed
                if sleep_for > 0:
                    if time.time() + sleep_for > window_end:
                        sleep_for = max(0, window_end - time.time())
                    if sleep_for > 0:
                        time.sleep(sleep_for)
            time.sleep(0.5)
        except Exception:
            logger.exception("Scanner fatal error, sleeping briefly")
            time.sleep(5)

# Trade orchestration helpers
def get_best_positive_and_negative():
    with candidates_shared_lock:
        items = list(candidates_shared.items())
    if not items:
        return None, None
    best_pos = None; best_pos_spread = -1e9
    best_neg = None; best_neg_spread = 1e9
    for sym, info in items:
        sp = info.get('max_spread') or info.get('start_spread') or 0.0
        if sp is not None and sp > best_pos_spread and sp > 0:
            best_pos_spread = sp
            best_pos = (sym, info)
        mn = info.get('min_spread') or info.get('start_spread') or 0.0
        if mn is not None and mn < best_neg_spread and mn < 0:
            best_neg_spread = mn
            best_neg = (sym, info)
    return best_pos, best_neg

def evaluate_entry_conditions(sym, info):
    """
    FIXED: Now properly handles funding rate threshold and doesn't trigger
    on small funding differences below MIN_FR_DIFF_THRESHOLD
    """
    # returns dict with keys if candidate meets any case, else None
    bin_bid = info.get('bin_bid'); bin_ask = info.get('bin_ask'); kc_bid = info.get('ku_bid'); kc_ask = info.get('ku_ask'); ku_api_sym = info.get('ku_sym')
    if not all([bin_bid, bin_ask, kc_bid, kc_ask, ku_api_sym]):
        return None
    if kc_bid > bin_ask:
        trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
        plan = ('binance', 'kucoin')
    elif bin_bid > kc_ask:
        trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
        plan = ('kucoin', 'binance')
    else:
        return None
    # funding fetch
    bin_fr_info = fetch_binance_funding(sym)
    kc_fr_info = fetch_kucoin_funding(ku_api_sym)
    bin_fr_pct = bin_fr_info.get('fundingRatePct') if bin_fr_info else None
    kc_fr_pct = kc_fr_info.get('fundingRatePct') if kc_fr_info else None
    bin_next = bin_fr_info.get('nextFundingTimeMs') if bin_fr_info else None
    kc_next = kc_fr_info.get('nextFundingTimeMs') if kc_fr_info else None
    if plan == ('binance','kucoin'):
        net_fr = compute_net_funding_for_plan(bin_fr_pct, kc_fr_pct, 'binance','kucoin')
    else:
        net_fr = compute_net_funding_for_plan(bin_fr_pct, kc_fr_pct, 'kucoin','binance')
    
    # FIXED: Don't consider funding rate if difference is below threshold
    if net_fr is not None and abs(net_fr) < MIN_FR_DIFF_THRESHOLD:
        net_fr = 0.0  # Treat as neutral for small differences
    
    now_ms = int(time.time()*1000)
    next_ft_ms = None
    if bin_next and kc_next:
        next_ft_ms = min(bin_next, kc_next)
    elif bin_next:
        next_ft_ms = bin_next
    elif kc_next:
        next_ft_ms = kc_next
    time_left_min = (next_ft_ms - now_ms)/60000.0 if next_ft_ms else None
    # Evaluate cases in order
    # Case1
    if abs(trigger_spread) >= CASE1_MIN_SPREAD and net_fr is not None and net_fr >= FR_ADVANTAGE_THRESHOLD:
        return {'case':'case1','plan':plan,'trigger_spread':trigger_spread,'net_fr':net_fr,'time_left_min':time_left_min,'bin_bid':bin_bid,'bin_ask':bin_ask,'kc_bid':kc_bid,'kc_ask':kc_ask,'ku_api_sym':ku_api_sym}
    # Case2
    if net_fr is not None and net_fr < 0 and time_left_min is not None and time_left_min < 30:
        net_dis = abs(net_fr)
        if abs(trigger_spread) >= CASE2_MULT * net_dis:
            return {'case':'case2','plan':plan,'trigger_spread':trigger_spread,'net_fr':net_fr,'time_left_min':time_left_min,'bin_bid':bin_bid,'bin_ask':bin_ask,'kc_bid':kc_bid,'kc_ask':kc_ask,'ku_api_sym':ku_api_sym}
    # Case3
    if net_fr is not None and net_fr < 0 and time_left_min is not None and time_left_min >= 30:
        net_dis = abs(net_fr)
        if abs(trigger_spread) >= CASE3_MULT * net_dis:
            return {'case':'case3','plan':plan,'trigger_spread':trigger_spread,'net_fr':net_fr,'time_left_min':time_left_min,'bin_bid':bin_bid,'bin_ask':bin_ask,'kc_bid':kc_bid,'kc_ask':kc_ask,'ku_api_sym':ku_api_sym}
    return None

def get_total_futures_balance():
    try:
        bal_bin = binance.fetch_balance(params={'type': 'future'})
        bin_usdt = float(bal_bin.get('USDT', {}).get('total', 0.0))
        bal_kc = kucoin.fetch_balance()
        kc_usdt = float(bal_kc.get('USDT', {}).get('total', 0.0))
        return bin_usdt + kc_usdt, bin_usdt, kc_usdt
    except Exception:
        logger.exception("Error fetching balances")
        return 0.0, 0.0, 0.0

# Execute pair trade with pre/post balance snapshots and retained finalize behavior
def execute_pair_trade_with_snapshots(sym, eval_info, initial_multiplier=1):
    ku_api = eval_info.get('ku_api_sym')
    plan = eval_info.get('plan')
    entry_case = eval_info.get('case')
    bin_bid = eval_info.get('bin_bid'); bin_ask = eval_info.get('bin_ask'); kc_bid = eval_info.get('kc_bid'); kc_ask = eval_info.get('kc_ask')
    kc_ccxt = resolve_kucoin_trade_symbol(kucoin, ku_api)
    if not kc_ccxt:
        logger.warning("Failed to resolve KuCoin CCXT symbol for %s api=%s", sym, ku_api)
        return False
    # Pre-trade balance snapshot
    total_pre, bin_pre, kc_pre = get_total_futures_balance()
    msg_pre = f"*PRE-TRADE BALANCE* — `{sym}`\nTotal: `${total_pre:.2f}` (Binance: `${bin_pre:.2f}` | KuCoin: `${kc_pre:.2f}`)\n{timestamp()}"
    logger.info("PRE-TRADE: Total=%s Binance=%s KuCoin=%s", total_pre, bin_pre, kc_pre)
    send_telegram(msg_pre)
    # set leverage
    try:
        set_leverage_for_symbol(binance, sym)
        set_leverage_for_symbol(kucoin, kc_ccxt)
    except Exception:
        pass
    # prepare notionals
    # choose price depending on plan for match_base exposure
    if plan == ('binance','kucoin'):
        notional_bin, notional_kc, _, _ = match_base_exposure_per_exchange(binance, kucoin, sym, kc_ccxt, NOTIONAL * initial_multiplier, bin_ask, kc_bid)
        results = {}
        trigger_time = datetime.utcnow()
        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_ccxt, trigger_time=trigger_time, trigger_price=kc_bid)
        def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, sym, trigger_time=trigger_time, trigger_price=bin_ask)
    else:
        notional_bin, notional_kc, _, _ = match_base_exposure_per_exchange(binance, kucoin, sym, kc_ccxt, NOTIONAL * initial_multiplier, bin_bid, kc_ask)
        results = {}
        trigger_time = datetime.utcnow()
        def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_ccxt, trigger_time=trigger_time, trigger_price=kc_ask)
        def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, sym, trigger_time=trigger_time, trigger_price=bin_bid)
    # Execute orders in parallel
    t1 = threading.Thread(target=exec_kc)
    t2 = threading.Thread(target=exec_bin)
    t1.start(); t2.start(); t1.join(); t2.join()
    ok_kc, exec_price_kc, exec_time_kc, exec_qty_kc = results.get('kc', (False, None, None, None))
    ok_bin, exec_price_bin, exec_time_bin, exec_qty_bin = results.get('bin', (False, None, None, None))
    if not (ok_kc and ok_bin and exec_price_kc is not None and exec_price_bin is not None):
        logger.info("Partial/failed execution for %s - closing partials", sym)
        close_all_and_wait()
        confirm_counts[sym] = 0
        return False
    # Finalize entry (reuse finalize_entry logic but minimal side-effects already included)
    success = finalize_entry_postexec(sym, ku_api, kc_ccxt, entry_case, eval_info.get('trigger_spread'), exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc, notional_bin, notional_kc, eval_info.get('net_fr'), eval_info)
    if success:
        # Post-entry balance snapshot
        total_post, bin_post, kc_post = get_total_futures_balance()
        msg_post = f"*POST-ENTRY BALANCE* — `{sym}`\nTotal: `${total_post:.2f}` (Binance: `${bin_post:.2f}` | KuCoin: `${kc_post:.2f}`)\n{timestamp()}"
        logger.info("POST-ENTRY: Total=%s Binance=%s KuCoin=%s", total_post, bin_post, kc_post)
        send_telegram(msg_post)
        return True
    else:
        return False

# We'll reuse finalize logic but as a separate function to avoid duplicate side-effects
def finalize_entry_postexec(sym, ku_api_sym, kc_ccxt_sym, case, trigger_spread, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc, implied_bin, implied_kc, net_fr, eval_info):
    """
    FIXED: Now properly stores the entry spread with correct sign (preserving negative for negative spreads)
    so that exit logic can correctly compare spreads
    """
    try:
        market_bin = get_market(binance, sym)
        bin_contract_size = float(market_bin.get('contractSize') or market_bin.get('info', {}).get('contractSize') or 1.0) if market_bin else 1.0
        market_kc = get_market(kucoin, kc_ccxt_sym)
        kc_contract_size = float(market_kc.get('contractSize') or market_kc.get('info', {}).get('contractSize') or 1.0) if market_kc else 1.0
        implied_bin_amt = float(exec_qty_bin) * bin_contract_size * float(exec_price_bin) if exec_qty_bin is not None else implied_bin
        implied_kc_amt = float(exec_qty_kc) * kc_contract_size * float(exec_price_kc) if exec_qty_kc is not None else implied_kc
    except Exception:
        implied_bin_amt = implied_bin
        implied_kc_amt = implied_kc
    mismatch_pct = abs(implied_bin_amt - implied_kc_amt) / max(implied_bin_amt, implied_kc_amt) * 100 if max(implied_bin_amt, implied_kc_amt) > 0 else 100
    logger.info("IMPLIED NOTIONALS | Binance: $%.6f | KuCoin: $%.6f | mismatch=%.3f%%", implied_bin_amt, implied_kc_amt, mismatch_pct)
    
    # FIXED: Calculate executed spread - formula depends on which direction we're trading
    # For positive trigger: BIN bid > KC ask (short BIN, long KC) → use (bin - kc) / kc
    # For negative trigger: KC bid > BIN ask (long BIN, short KC) → use (kc - bin) / bin
    if trigger_spread > 0:
        # Positive trigger spread
        exec_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc if exec_price_kc > 0 else trigger_spread
    else:
        # Negative trigger spread
        exec_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin if exec_price_bin > 0 else trigger_spread
    
    # Use minimum of absolute values but preserve the original sign
    if abs(exec_spread) < abs(trigger_spread):
        practical_entry_spread = exec_spread
    else:
        practical_entry_spread = trigger_spread
    
    logger.info("ENTRY SPREAD CALCULATION | trigger=%.4f%% executed=%.4f%% practical=%.4f%%", 
                trigger_spread, exec_spread, practical_entry_spread)
    
    active_trade['symbol'] = sym
    active_trade['ku_api'] = ku_api_sym
    active_trade['ku_ccxt'] = kc_ccxt_sym
    active_trade['case'] = case
    active_trade['entry_spread'] = trigger_spread
    active_trade['avg_entry_spread'] = practical_entry_spread  # FIXED: Now preserves sign
    active_trade['entry_prices'] = {'binance': {'price': exec_price_bin, 'qty': exec_qty_bin, 'implied': implied_bin_amt}, 'kucoin': {'price': exec_price_kc, 'qty': exec_qty_kc, 'implied': implied_kc_amt}}
    active_trade['notional'] = NOTIONAL
    active_trade['averages_done'] = 0
    active_trade['final_implied_notional'] = {'bin': implied_bin_amt, 'kc': implied_kc_amt}
    active_trade['funding_accumulated_pct'] = 0.0
    active_trade['funding_rounds_seen'] = set()
    active_trade['suppress_full_scan'] = abs(trigger_spread) >= BIG_SPREAD_THRESHOLD
    entry_spreads[sym] = practical_entry_spread  # FIXED: Store with sign preserved
    entry_prices.setdefault(sym, {})['kucoin'] = exec_price_kc
    entry_prices.setdefault(sym, {})['binance'] = exec_price_bin
    entry_actual.setdefault(sym, {})['kucoin'] = {'exec_price': exec_price_kc, 'exec_time': None, 'exec_qty': exec_qty_kc, 'implied_notional': implied_kc_amt}
    entry_actual.setdefault(sym, {})['binance'] = {'exec_price': exec_price_bin, 'exec_time': None, 'exec_qty': exec_qty_bin, 'implied_notional': implied_bin_amt}
    positions[sym] = case
    _set_initial_entry_qtys(sym, exec_qty_bin, exec_qty_kc, exec_price_bin, exec_price_kc, implied_bin_amt, implied_kc_amt, kc_ccxt_sym)
    try:
        try_place_stops_after_entry(sym, kc_ccxt_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc)
    except Exception:
        logger.exception("Error placing stops immediately after entry")
    try:
        _start_liquidation_watcher_for_symbols(sym, sym, kc_ccxt_sym)
    except Exception:
        logger.exception("Failed to start liquidation watcher")
    
    # NEW: Get entry condition details for messaging
    entry_condition_msg = get_entry_condition_message(eval_info)
    
    msg = f"*ENTRY OPENED* `{sym}`\n{entry_condition_msg}\nCase: {case}\nTrigger Spread: `{trigger_spread:.4f}%`\nExecuted Spread: `{exec_spread:.4f}%`\nPractical Entry Spread: `{practical_entry_spread:.4f}%`\nBinance exec: `{exec_price_bin}`\nKuCoin exec: `{exec_price_kc}`\nNotionals ~ Bin: `${implied_bin_amt:.2f}` | Kc: `${implied_kc_amt:.2f}`\nAverages allowed: {MAX_AVERAGES}\n{timestamp()}"
    logger.info("ENTRY_OPENED | %s | %s | trigger_spread=%.4f%% exec_spread=%.4f%% practical=%.4f%%", 
                sym, entry_condition_msg.replace('\n', ' | '), trigger_spread, exec_spread, practical_entry_spread)
    send_telegram(msg)
    return True

def get_entry_condition_message(eval_info):
    """Generate a descriptive message about which entry condition was triggered"""
    case = eval_info.get('case')
    net_fr = eval_info.get('net_fr')
    time_left_min = eval_info.get('time_left_min')
    trigger_spread = eval_info.get('trigger_spread')
    
    if case == 'case1':
        return f"✅ CASE 1 TRIGGERED\nSpread ≥ {CASE1_MIN_SPREAD}% AND FR Advantage ≥ {FR_ADVANTAGE_THRESHOLD}%\nNet FR: {net_fr:.4f}%"
    elif case == 'case2':
        net_dis = abs(net_fr) if net_fr else 0
        return f"✅ CASE 2 TRIGGERED\nTime < 30min AND Spread ≥ {CASE2_MULT}x FR Disadvantage\nNet FR: {net_fr:.4f}% | Time left: {time_left_min:.1f}min"
    elif case == 'case3':
        net_dis = abs(net_fr) if net_fr else 0
        return f"✅ CASE 3 TRIGGERED\nTime ≥ 30min AND Spread ≥ {CASE3_MULT}x FR Disadvantage\nNet FR: {net_fr:.4f}% | Time left: {time_left_min:.1f}min"
    else:
        return f"Entry triggered | Net FR: {net_fr:.4f}% | Time left: {time_left_min:.1f}min" if net_fr else "Entry triggered"

# NEW: place_reduce_only_stop function
def place_reduce_only_stop(exchange, symbol, side, qty, liq_price, buffer_pct=0.02, exec_price_fallback=None):
    """
    Place a reduce-only stop market order at liquidation price + buffer
    side: 'long' or 'short' (position side)
    qty: position quantity (absolute value)
    liq_price: liquidation price
    buffer_pct: percentage buffer from liquidation
    exec_price_fallback: use this if liq_price is None
    """
    try:
        if qty is None or qty == 0:
            logger.info("No qty to place stop for %s %s", exchange.id, symbol)
            return False
        
        # Determine stop price
        stop_price = None
        if liq_price is not None and liq_price > 0:
            if side == 'long':
                # For long, stop below liq
                stop_price = liq_price * (1 - buffer_pct)
            elif side == 'short':
                # For short, stop above liq
                stop_price = liq_price * (1 + buffer_pct)
            logger.info("✅ STOP PRICE CALCULATED FROM LIQUIDATION PRICE: %s side=%s liq_price=%s stop_price=%s", 
                       exchange.id, side, liq_price, stop_price)
        elif exec_price_fallback is not None and exec_price_fallback > 0:
            # Use exec price with larger buffer if no liq
            if side == 'long':
                stop_price = exec_price_fallback * (1 - buffer_pct * 3)
            elif side == 'short':
                stop_price = exec_price_fallback * (1 + buffer_pct * 3)
            logger.warning("⚠️  FALLBACK: Using exec price for stop (no liquidation price available): %s side=%s exec_price=%s stop_price=%s", 
                         exchange.id, side, exec_price_fallback, stop_price)
        
        if stop_price is None or stop_price <= 0:
            logger.warning("Cannot determine valid stop price for %s %s (liq=%s, exec=%s, side=%s)", 
                         exchange.id, symbol, liq_price, exec_price_fallback, side)
            return False
        
        # Determine order side (opposite of position side)
        order_side = 'sell' if side == 'long' else 'buy'
        
        # Round quantity
        market = get_market(exchange, symbol)
        prec = market.get('precision', {}).get('amount') if market else None
        qty_rounded = round_down(abs(qty), prec) if prec is not None else abs(qty)
        
        if qty_rounded == 0:
            logger.warning("Rounded qty is 0 for %s %s", exchange.id, symbol)
            return False
        
        logger.info("Placing stop order: %s %s %s qty=%s stopPrice=%s (liq=%s)", 
                   exchange.id, symbol, order_side, qty_rounded, stop_price, liq_price)
        
        if DRY_RUN:
            logger.info("[DRY_RUN] Would place stop order")
            return True
        
        # Place stop market order
        try:
            if exchange.id == 'binance':
                order = exchange.create_order(
                    symbol=symbol,
                    type='STOP_MARKET',
                    side=order_side,
                    amount=qty_rounded,
                    params={
                        'stopPrice': stop_price,
                        'reduceOnly': True
                    }
                )
            else:  # kucoin
                order = exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side=order_side,
                    amount=qty_rounded,
                    params={
                        'stop': 'down' if order_side == 'sell' else 'up',
                        'stopPrice': stop_price,
                        'reduceOnly': True,
                        'marginMode': 'cross'
                    }
                )
            logger.info("Stop order placed successfully: %s %s order_id=%s", 
                       exchange.id, symbol, order.get('id'))
            return True
        except Exception as e:
            logger.exception("Failed to place stop order: %s %s: %s", exchange.id, symbol, e)
            return False
            
    except Exception as e:
        logger.exception("Error in place_reduce_only_stop: %s", e)
        return False

# Minimal re-used helpers (stop placing & initial qty)
def _set_initial_entry_qtys(sym, exec_qty_bin, exec_qty_kc, exec_price_bin, exec_price_kc, notional_bin, notional_kc, kc_trade_sym):
    try:
        bin_qty = None
        kc_qty = None
        if exec_qty_bin is not None:
            try:
                bin_qty = float(exec_qty_bin)
            except Exception:
                bin_qty = None
        if exec_qty_kc is not None:
            try:
                kc_qty = float(exec_qty_kc)
            except Exception:
                kc_qty = None
        if (bin_qty is None or bin_qty == 0) and exec_price_bin:
            try:
                amt, implied, _, prec = compute_amount_for_notional(binance, sym, notional_bin, exec_price_bin)
                bin_qty = amt
            except Exception:
                bin_qty = bin_qty or 0.0
        if (kc_qty is None or kc_qty == 0) and exec_price_kc:
            try:
                amt, implied, _, prec = compute_amount_for_notional(kucoin, kc_trade_sym, notional_kc, exec_price_kc)
                kc_qty = amt
            except Exception:
                kc_qty = kc_qty or 0.0
        bin_qty = abs(bin_qty or 0.0)
        kc_qty = abs(kc_qty or 0.0)
        entry_initial_qtys.setdefault(sym, {})['bin'] = float(bin_qty)
        entry_initial_qtys.setdefault(sym, {})['kc'] = float(kc_qty)
        _last_known_positions.setdefault(sym, {})['bin'] = float(bin_qty)
        _last_known_positions.setdefault(sym, {})['kc'] = float(kc_qty)
        _last_known_positions[sym]['kc_err_count'] = 0
        logger.info("Cached initial entry qtys for watcher: sym=%s bin=%s kc=%s", sym, bin_qty, kc_qty)
    except Exception:
        logger.exception("Error caching initial entry qtys")

def try_place_stops_after_entry(sym, kc_trade_sym, exec_price_bin, exec_price_kc, exec_qty_bin, exec_qty_kc):
    try:
        pos_kc = None
        pos_bin = None
        try:
            pos_kc = fetch_position_info(kucoin, kc_trade_sym)
            _last_known_positions.setdefault(sym, {})['kc'] = pos_kc.get('signed_qty', 0.0)
            _last_known_positions.setdefault(sym, {})['kc_err_count'] = 0
        except Exception:
            _last_known_positions.setdefault(sym, {})['kc_err_count'] = _last_known_positions.setdefault(sym, {}).get('kc_err_count', 0) + 1
            pos_kc = {'signed_qty': _last_known_positions.get(sym, {}).get('kc', 0.0), 'liquidationPrice': None, 'side': None}
            logger.info("Using last-known KuCoin qty due to fetch error")
        try:
            pos_bin = fetch_position_info(binance, sym)
            _last_known_positions.setdefault(sym, {})['bin'] = pos_bin.get('signed_qty', 0.0)
        except Exception:
            pos_bin = {'signed_qty': _last_known_positions.get(sym, {}).get('bin', 0.0), 'liquidationPrice': None, 'side': None}
            logger.info("Binance fetch error for pos; using last-known bin qty")
        qty_kc = abs(exec_qty_kc) if exec_qty_kc is not None else abs(pos_kc.get('signed_qty') or 0.0)
        qty_bin = abs(exec_qty_bin) if exec_qty_bin is not None else abs(pos_bin.get('signed_qty') or 0.0)
        liq_kc = pos_kc.get('liquidationPrice')
        liq_bin = pos_bin.get('liquidationPrice')
        side_kc = pos_kc.get('side')
        side_bin = pos_bin.get('side')
        logger.info("ATTEMPTING STOPS | kc_qty=%s kc_liq=%s kc_side=%s | bin_qty=%s bin_liq=%s bin_side=%s", qty_kc, liq_kc, side_kc, qty_bin, liq_bin, side_bin)
        placed_kc = place_reduce_only_stop(kucoin, kc_trade_sym, side_kc, qty_kc, liq_kc, buffer_pct=STOP_BUFFER_PCT, exec_price_fallback=exec_price_kc)
        placed_bin = place_reduce_only_stop(binance, sym, side_bin, qty_bin, liq_bin, buffer_pct=STOP_BUFFER_PCT, exec_price_fallback=exec_price_bin)
        logger.info("STOP_PLACED_RESULT | kc=%s | bin=%s", placed_kc, placed_bin)
        return placed_kc, placed_bin
    except Exception:
        logger.exception("Error placing stops after entry")
        return False, False

# Averaging, funding accounting, maintenance loops (preserved; will run background)
def attempt_averaging_if_needed():
    sym = active_trade.get('symbol')
    if not sym:
        return
    if active_trade.get('averages_done', 0) >= MAX_AVERAGES:
        return
    case = active_trade.get('case')
    avg_entry_spread = active_trade.get('avg_entry_spread') or 0.0
    try:
        bin_book = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
        bin_price = None
        for item in bin_book:
            if item['symbol'] == sym:
                bin_price = (float(item['bidPrice']), float(item['askPrice']))
                break
        kc_api = active_trade.get('ku_api') or (sym + "M")
        kc_resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={kc_api}", timeout=5).json()
        d = kc_resp.get('data', {}) if isinstance(kc_resp, dict) else {}
        kc_price = (float(d.get('bestBidPrice') or 0), float(d.get('bestAskPrice') or 0))
        if not bin_price or not kc_price:
            return
        bin_bid, bin_ask = bin_price
        kc_bid, kc_ask = kc_price
    except Exception:
        return
    
    # FIXED: Calculate current spread using entry spread direction
    # For positive entry: use (bin_bid - kc_ask) / kc_ask
    # For negative entry: use (kc_bid - bin_ask) / bin_ask
    if avg_entry_spread > 0:
        # Entry was positive spread
        if kc_ask <= 0:
            return
        current_spread = 100 * (bin_bid - kc_ask) / kc_ask
    else:
        # Entry was negative spread
        if bin_ask <= 0:
            return
        current_spread = 100 * (kc_bid - bin_ask) / bin_ask
    
    logger.info("Averaging check for %s | current_spread=%.4f%% avg_entry=%.4f%%", sym, current_spread, avg_entry_spread)
    
    # FIXED: Averaging trigger must consider spread direction
    # For POSITIVE spreads: average when current > 2x entry (spread widening = good)
    # For NEGATIVE spreads: average when current < 2x entry (more negative = good)
    should_average = False
    if avg_entry_spread > 0:
        # Positive spread: more positive is better
        should_average = current_spread >= AVERAGE_TRIGGER_MULTIPLIER * avg_entry_spread
    else:
        # Negative spread: more negative is better
        should_average = current_spread <= AVERAGE_TRIGGER_MULTIPLIER * avg_entry_spread
    
    if should_average:
        logger.info("Averaging triggered for %s", sym)
        kc_ccxt = active_trade.get('ku_ccxt')
        if not kc_ccxt:
            kc_ccxt = resolve_kucoin_trade_symbol(kucoin, active_trade.get('ku_api') or sym + "M")
            active_trade['ku_ccxt'] = kc_ccxt
        
        # Use entry spread direction for order placement
        if avg_entry_spread > 0:
            # Positive entry: short binance, long kucoin
            notional_bin, notional_kc, _, _ = match_base_exposure_per_exchange(binance, kucoin, sym, kc_ccxt, NOTIONAL, bin_bid, kc_ask)
            results = {}
            def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_ccxt)
            def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, sym)
        else:
            # Negative entry: long binance, short kucoin
            notional_bin, notional_kc, _, _ = match_base_exposure_per_exchange(binance, kucoin, sym, kc_ccxt, NOTIONAL, bin_ask, kc_bid)
            results = {}
            def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_ccxt)
            def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, sym)
        t1 = threading.Thread(target=exec_kc)
        t2 = threading.Thread(target=exec_bin)
        t1.start(); t2.start(); t1.join(); t2.join()
        ok_kc, exec_price_kc, exec_time_kc, exec_qty_kc = results.get('kc', (False, None, None, None))
        ok_bin, exec_price_bin, exec_time_bin, exec_qty_bin = results.get('bin', (False, None, None, None))
        if ok_kc and ok_bin:
            try:
                market_bin = get_market(binance, sym)
                bin_contract_size = float(market_bin.get('contractSize') or market_bin.get('info', {}).get('contractSize') or 1.0) if market_bin else 1.0
                market_kc = get_market(kucoin, kc_ccxt)
                kc_contract_size = float(market_kc.get('contractSize') or market_kc.get('info', {}).get('contractSize') or 1.0) if market_kc else 1.0
                new_implied_bin = float(exec_qty_bin) * bin_contract_size * float(exec_price_bin) if exec_qty_bin is not None else 0.0
                new_implied_kc = float(exec_qty_kc) * kc_contract_size * float(exec_price_kc) if exec_qty_kc is not None else 0.0
                prev_bin = active_trade['final_implied_notional']['bin']
                prev_kc = active_trade['final_implied_notional']['kc']
                prev_avg = active_trade['avg_entry_spread'] or active_trade['entry_spread']
                
                # Calculate this averaging spread using entry direction
                # For positive entry: use (bin - kc) / kc
                # For negative entry: use (kc - bin) / bin
                if prev_avg > 0:
                    # Positive entry
                    this_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                else:
                    # Negative entry
                    this_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                prev_total = prev_bin + prev_kc
                new_total = new_implied_bin + new_implied_kc
                if prev_total + new_total > 0:
                    weighted_prev = prev_avg * prev_total
                    weighted_new = this_spread * new_total
                    new_avg = (weighted_prev + weighted_new) / (prev_total + new_total)
                else:
                    new_avg = this_spread
                active_trade['averages_done'] += 1
                active_trade['avg_entry_spread'] = new_avg
                active_trade['final_implied_notional']['bin'] = prev_bin + new_implied_bin
                active_trade['final_implied_notional']['kc'] = prev_kc + new_implied_kc
                try_place_stops_after_entry(sym, kc_ccxt, exec_price_bin or None, exec_price_kc or None, None, None)
                send_telegram(f"*AVERAGE ADDED* `{sym}` -> new avg spread `{new_avg:.4f}%` (averages_done={active_trade['averages_done']})\n{timestamp()}")
            except Exception:
                logger.exception("Error aggregating averaging results")
        else:
            logger.info("Averaging partial/failed execution; will not count as average")

def check_take_profit_or_close_conditions():
    """
    FIXED: Now properly handles spread sign comparison and exits when:
    1. 50% profit target is hit (spread converges by 50%)
    2. Spread reverses direction (sign changes) - meaning at least 50% profit captured
    """
    sym = active_trade.get('symbol')
    if not sym:
        return
    case = active_trade.get('case')
    avg_entry_spread = active_trade.get('avg_entry_spread') or active_trade.get('entry_spread') or 0.0
    
    try:
        bin_book = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
        bin_price = None
        for item in bin_book:
            if item['symbol'] == sym:
                bin_price = (float(item['bidPrice']), float(item['askPrice']))
                break
        kc_api = active_trade.get('ku_api') or sym + "M"
        kc_resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={kc_api}", timeout=5).json()
        d = kc_resp.get('data', {}) if isinstance(kc_resp, dict) else {}
        kc_price = (float(d.get('bestBidPrice') or 0), float(d.get('bestAskPrice') or 0))
        if not bin_price or not kc_price:
            return
        bin_bid, bin_ask = bin_price
        kc_bid, kc_ask = kc_price
        # Validate prices to prevent division by zero
        if bin_bid <= 0 or bin_ask <= 0 or kc_bid <= 0 or kc_ask <= 0:
            return
    except Exception:
        return
    
    # FIXED: Calculate current spread using the SAME direction as entry spread
    # For positive entry: use (bin_bid - kc_ask) / kc_ask
    # For negative entry: use (kc_bid - bin_ask) / bin_ask
    if avg_entry_spread > 0:
        # Entry was positive spread (bin > kc)
        current_spread = 100 * (bin_bid - kc_ask) / kc_ask if kc_ask > 0 else 0.0
    else:
        # Entry was negative spread (kc > bin)
        current_spread = 100 * (kc_bid - bin_ask) / bin_ask if bin_ask > 0 else 0.0
    
    logger.info("TP CHECK | sym=%s | entry_spread=%.4f%% current_spread=%.4f%%", 
                sym, avg_entry_spread, current_spread)
    
    # FIXED: Check for sign reversal (spread flipped direction = big profit)
    entry_was_positive = avg_entry_spread > 0
    current_is_positive = current_spread > 0
    
    if entry_was_positive != current_is_positive:
        # Sign changed! This means spread reversed = captured at least full spread
        msg = f"*SPREAD SIGN REVERSAL EXIT* `{sym}`\nEntry: `{avg_entry_spread:.4f}%`\nCurrent: `{current_spread:.4f}%`\nSpread reversed direction - profit captured!\n{timestamp()}"
        logger.info("SPREAD_SIGN_REVERSAL | %s | entry=%.4f%% current=%.4f%%", sym, avg_entry_spread, current_spread)
        send_telegram(msg)
        close_all_and_wait()
        reset_active_trade()
        return
    
    # FIXED: Calculate convergence properly with signed values
    # For positive entry spread: profit when spread decreases (current < entry)
    # For negative entry spread: profit when spread increases toward zero (current > entry, both negative)
    if avg_entry_spread > 0:
        # Positive spread case: profit = entry_spread - current_spread
        spread_convergence = avg_entry_spread - current_spread
    else:
        # Negative spread case: profit = current_spread - entry_spread (both negative, so this is positive when converging)
        spread_convergence = current_spread - avg_entry_spread
    
    target_convergence = abs(avg_entry_spread) * TAKE_PROFIT_FACTOR  # 50% of entry spread
    
    logger.info("TP CALC | convergence=%.4f%% target=%.4f%%", spread_convergence, target_convergence)
    
    if spread_convergence >= target_convergence:
        msg = f"*TAKE PROFIT* `{sym}`\nEntry: `{avg_entry_spread:.4f}%`\nCurrent: `{current_spread:.4f}%`\nConvergence: `{spread_convergence:.4f}%` ≥ Target: `{target_convergence:.4f}%`\n{timestamp()}"
        logger.info("TAKE_PROFIT | %s | convergence=%.4f%% >= target=%.4f%%", sym, spread_convergence, target_convergence)
        send_telegram(msg)
        close_all_and_wait()
        reset_active_trade()
        return
    
    # Check funding accumulation
    funding_acc = active_trade.get('funding_accumulated_pct', 0.0)
    if funding_acc > target_convergence:
        # Only close if spread hasn't widened too much
        if abs(current_spread) <= (1.5 * abs(avg_entry_spread)):
            msg = f"*CLOSE ON FUNDING LOSS* `{sym}`\nFunding acc: `{funding_acc:.4f}%` > Target: `{target_convergence:.4f}%`\nEntry: `{avg_entry_spread:.4f}%` Current: `{current_spread:.4f}%`\n{timestamp()}"
            logger.info("FUNDING_LOSS_EXIT | %s | funding=%.4f%% > target=%.4f%%", sym, funding_acc, target_convergence)
            send_telegram(msg)
            close_all_and_wait()
            reset_active_trade()

def reset_active_trade():
    sym = active_trade.get('symbol')
    if sym:
        positions[sym] = None
    active_trade.update({'symbol': None, 'ku_api': None, 'ku_ccxt': None, 'case': None, 'entry_spread': None, 'avg_entry_spread': None, 'entry_prices': None, 'notional': None, 'averages_done': 0, 'final_implied_notional': None, 'funding_accumulated_pct': 0.0, 'funding_rounds_seen': set(), 'suppress_full_scan': False})
    logger.info("Active trade reset")

def funding_round_accounting_loop():
    while True:
        try:
            sym = active_trade.get('symbol')
            if not sym:
                time.sleep(5)
                continue
            bin_fr_info = fetch_binance_funding(sym)
            kc_fr_info = fetch_kucoin_funding(active_trade.get('ku_api') or sym + "M")
            if not bin_fr_info and not kc_fr_info:
                time.sleep(10)
                continue
            now_ms = int(time.time()*1000)
            next_bin = bin_fr_info.get('nextFundingTimeMs') if bin_fr_info else None
            next_kc = kc_fr_info.get('nextFundingTimeMs') if kc_fr_info else None
            for next_ft in (next_bin, next_kc):
                try:
                    if not next_ft:
                        continue
                    if next_ft <= now_ms and next_ft not in active_trade['funding_rounds_seen']:
                        binfo = fetch_binance_funding(sym) or {}
                        kinfo = fetch_kucoin_funding(active_trade.get('ku_api') or sym + "M") or {}
                        b_pct = binfo.get('fundingRatePct') or 0.0
                        k_pct = kinfo.get('fundingRatePct') or 0.0
                        if active_trade['case'] == 'caseA':
                            net = compute_net_funding_for_plan(b_pct, k_pct, 'binance','kucoin')
                        else:
                            net = compute_net_funding_for_plan(b_pct, k_pct, 'kucoin','binance')
                        expense = abs(net) if net < 0 else 0.0
                        active_trade['funding_accumulated_pct'] += expense
                        active_trade['funding_rounds_seen'].add(next_ft)
                        send_telegram(f"*FUNDING ROUND APPLIED* `{sym}`\nBinance: `{b_pct}`% KuCoin: `{k_pct}`%\nNet: `{net}`% expense `{expense}`%\nAccumulated funding expense: `{active_trade['funding_accumulated_pct']:.4f}%`\n{timestamp()}")
                except Exception:
                    logger.exception("Error in funding_round_accounting loop")
            time.sleep(10)
        except Exception:
            logger.exception("Funding accounting loop fatal error")
            time.sleep(5)

def periodic_summary_loop():
    """Send detailed status updates every 60 seconds"""
    while True:
        try:
            print("\n" + "="*80, flush=True)
            print(f"PERIODIC SUMMARY LOOP RUNNING - {timestamp()}", flush=True)
            print("="*80, flush=True)
            time.sleep(60)  # Every 1 minute
            
            # Get max positive and negative spreads
            best_pos, best_neg = get_best_positive_and_negative()
            
            max_pos_spread = 0.0
            max_pos_sym = "None"
            max_pos_fr_bin = 0.0
            max_pos_fr_kc = 0.0
            
            max_neg_spread = 0.0
            max_neg_sym = "None"
            max_neg_fr_bin = 0.0
            max_neg_fr_kc = 0.0
            
            if best_pos:
                max_pos_sym, pos_info = best_pos
                max_pos_spread = pos_info.get('max_spread', pos_info.get('start_spread', 0.0))
                # Fetch funding rates
                try:
                    b_info = fetch_binance_funding(max_pos_sym)
                    k_info = fetch_kucoin_funding(pos_info.get('ku_sym', max_pos_sym + 'M'))
                    max_pos_fr_bin = b_info.get('fundingRatePct', 0.0) if b_info else 0.0
                    max_pos_fr_kc = k_info.get('fundingRatePct', 0.0) if k_info else 0.0
                except Exception as e:
                    logger.warning(f"Error fetching funding for {max_pos_sym}: {e}")
            
            if best_neg:
                max_neg_sym, neg_info = best_neg
                max_neg_spread = neg_info.get('min_spread', neg_info.get('start_spread', 0.0))
                # Fetch funding rates
                try:
                    b_info = fetch_binance_funding(max_neg_sym)
                    k_info = fetch_kucoin_funding(neg_info.get('ku_sym', max_neg_sym + 'M'))
                    max_neg_fr_bin = b_info.get('fundingRatePct', 0.0) if b_info else 0.0
                    max_neg_fr_kc = k_info.get('fundingRatePct', 0.0) if k_info else 0.0
                except Exception as e:
                    logger.warning(f"Error fetching funding for {max_neg_sym}: {e}")
            
            if active_trade.get('symbol'):
                sym = active_trade['symbol']
                case = active_trade.get('case', 'N/A')
                avg_spread = active_trade.get('avg_entry_spread', 0.0)
                avg_count = active_trade.get('avg_count', 0)
                total_not = active_trade.get('total_notional', 0.0)
                funding_acc = active_trade.get('funding_accumulated_pct', 0.0)
                
                # Get current spread
                with candidates_shared_lock:
                    info = candidates_shared.get(sym)
                current_spread = info.get('max_spread', 0.0) if info else 0.0
                
                summary = (
                    f"📊 *BOT STATUS SUMMARY*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🟢 *ACTIVE TRADE*\n"
                    f"Symbol: `{sym}` ({case})\n"
                    f"Entry: `{avg_spread:.4f}%` (×{avg_count})\n"
                    f"Current: `{current_spread:.4f}%`\n"
                    f"Notional: `${total_not:.2f}`\n"
                    f"Funding Cost: `{funding_acc:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 *MAX POSITIVE SPREAD*\n"
                    f"`{max_pos_sym}`: `{max_pos_spread:.4f}%`\n"
                    f"FR → Bin: `{max_pos_fr_bin:.4f}%` | KC: `{max_pos_fr_kc:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📉 *MAX NEGATIVE SPREAD*\n"
                    f"`{max_neg_sym}`: `{max_neg_spread:.4f}%`\n"
                    f"FR → Bin: `{max_neg_fr_bin:.4f}%` | KC: `{max_neg_fr_kc:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ {timestamp()}"
                )
            else:
                # No active trade
                with candidates_shared_lock:
                    num_candidates = len(candidates_shared)
                
                summary = (
                    f"📊 *BOT STATUS SUMMARY*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔍 *SCANNING MODE*\n"
                    f"Active Trade: `None`\n"
                    f"Candidates: `{num_candidates}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 *MAX POSITIVE SPREAD*\n"
                    f"`{max_pos_sym}`: `{max_pos_spread:.4f}%`\n"
                    f"FR → Bin: `{max_pos_fr_bin:.4f}%` | KC: `{max_pos_fr_kc:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📉 *MAX NEGATIVE SPREAD*\n"
                    f"`{max_neg_sym}`: `{max_neg_spread:.4f}%`\n"
                    f"FR → Bin: `{max_neg_fr_bin:.4f}%` | KC: `{max_neg_fr_kc:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ {timestamp()}"
                )
            
            print(f"SENDING SUMMARY TO TELEGRAM: {summary[:100]}...", flush=True)
            logger.info("📊 PERIODIC SUMMARY - Sending to Telegram")
            logger.info(f"Max Pos: {max_pos_sym} {max_pos_spread:.4f}% | Max Neg: {max_neg_sym} {max_neg_spread:.4f}%")
            send_telegram(summary)
            print("SUMMARY SENT SUCCESSFULLY", flush=True)
            
        except Exception as e:
            print(f"ERROR IN SUMMARY LOOP: {e}", flush=True)
            logger.exception("Error in periodic_summary_loop")
            time.sleep(5)

def periodic_trade_maintenance_loop():
    last_full_scan = 0
    while True:
        try:
            if not active_trade.get('symbol'):
                time.sleep(1)
                continue
            attempt_averaging_if_needed()
            check_take_profit_or_close_conditions()
            now = time.time()
            if not active_trade.get('suppress_full_scan') and (now - last_full_scan) >= SCANNER_FULL_INTERVAL:
                last_full_scan = now
                common_symbols, ku_map = get_common_symbols()
                if not common_symbols:
                    continue
                bin_book = get_binance_book()
                ku_symbols = [ku_map.get(sym, sym + "M") for sym in common_symbols]
                ku_prices = threaded_kucoin_prices(ku_symbols)
                best_sym = None; best_spread = 0.0; best_info = None
                for sym in common_symbols:
                    if sym == active_trade.get('symbol'):
                        continue
                    bin_tick = bin_book.get(sym)
                    ku_sym = ku_map.get(sym, sym + "M")
                    ku_tick = ku_prices.get(ku_sym)
                    if not bin_tick or not ku_tick:
                        continue
                    spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], ku_tick["bid"], ku_tick["ask"])
                    if spread is None:
                        continue
                    if abs(spread) > abs(best_spread):
                        best_spread = spread
                        best_sym = sym
                        best_info = {"ku_sym": ku_sym, "bin_bid": bin_tick["bid"], "bin_ask": bin_tick["ask"], "ku_bid": ku_tick["bid"], "ku_ask": ku_tick["ask"]}
                if best_sym and abs(best_spread) >= BIG_SPREAD_THRESHOLD and (active_trade.get('avg_entry_spread') or 0.0) < 4.0:
                    send_telegram(f"*TAKEOVER SIGNAL* `{best_sym}` spread `{best_spread:.4f}%` >= {BIG_SPREAD_THRESHOLD}%\nClosing current `{active_trade['symbol']}` and entering `{best_sym}` with 2x notional\n{timestamp()}")
                    close_all_and_wait()
                    reset_active_trade()
                    eval_info = {'plan': ('binance','kucoin') if best_info['ku_bid'] > best_info['bin_ask'] else ('kucoin','binance'),'ku_api_sym': best_info['ku_sym'],'bin_bid':best_info['bin_bid'],'bin_ask':best_info['bin_ask'],'kc_bid':best_info['ku_bid'],'kc_ask':best_info['ku_ask'],'trigger_spread':best_spread,'net_fr':0.0}
                    execute_pair_trade_with_snapshots(best_sym, eval_info, initial_multiplier=BIG_SPREAD_ENTRY_MULTIPLIER)
            time.sleep(5)
        except Exception:
            logger.exception("trade maintenance loop error")
            time.sleep(2)

# Ctrl+E listener
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
                                logger.info(f"{datetime.now().isoformat()} Ctrl+E detected -> closing all positions now.")
                                try:
                                    close_all_and_wait()
                                except Exception:
                                    pass
                                terminate_bot = True
                                break
                    except Exception:
                        pass
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
                                    logger.info(f"{datetime.now().isoformat()} Ctrl+E detected -> closing all positions now.")
                                    try:
                                        close_all_and_wait()
                                    except Exception:
                                        pass
                                    terminate_bot = True
                                    break
                        except Exception:
                            time.sleep(0.1)
                finally:
                    try:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    except Exception:
                        pass
        except Exception:
            pass
    t = threading.Thread(target=_listener, daemon=True)
    t.start()

start_ctrl_e_listener()

# Start background threads
def start_background_threads():
    print("\n" + "="*80, flush=True)
    print("🚀 STARTING BACKGROUND THREADS", flush=True)
    print("="*80, flush=True)
    
    t_scan = threading.Thread(target=spread_scanner_loop, daemon=True)
    t_scan.start()
    print("✓ Scanner thread started", flush=True)
    
    t_maint = threading.Thread(target=periodic_trade_maintenance_loop, daemon=True)
    t_maint.start()
    print("✓ Maintenance thread started", flush=True)
    
    t_fund = threading.Thread(target=funding_round_accounting_loop, daemon=True)
    t_fund.start()
    print("✓ Funding thread started", flush=True)
    
    t_summary = threading.Thread(target=periodic_summary_loop, daemon=True)
    t_summary.start()
    print("✓ Summary thread started", flush=True)
    
    logger.info("✅ All background threads started (scanner, maintenance, funding, summary)")
    print("="*80, flush=True)
    print("✅ ALL THREADS RUNNING", flush=True)
    print("="*80 + "\n", flush=True)

start_background_threads()

terminate_bot = False

print("\n" + "="*80, flush=True)
print("🎯 BOT FULLY INITIALIZED", flush=True)
print("="*80, flush=True)
logger.info("🚀 Bot initialization complete - sending startup message")
print("Sending Telegram startup message...", flush=True)
send_telegram("🚀 Integrated Arb Bot V2 STARTED\n✅ All systems operational\n📊 Summary every 60s\n⏰ " + timestamp())
print("✅ Telegram startup message sent", flush=True)
print("="*80, flush=True)
print("🔥 MAIN LOOP STARTING - BOT IS LIVE!", flush=True)
print("="*80 + "\n", flush=True)

# Main loop — with 3 confirmations for entry
try:
    loop_count = 0
    while True:
        loop_count += 1
        if loop_count % 30 == 0:  # Every 30 seconds
            print(f"💓 Main loop heartbeat #{loop_count} - {timestamp()}", flush=True)
            logger.debug(f"Main loop iteration {loop_count}")
        
        if terminate_bot:
            print("🛑 Terminate signal received, closing positions...", flush=True)
            try:
                close_all_and_wait()
            except Exception:
                pass
            break
        if not active_trade.get('symbol'):
            # FIXED: Check for 7.5%+ big spread entry even when no trade is active
            best_pos, best_neg = get_best_positive_and_negative()
            
            # Check if any symbol has >= 7.5% spread for immediate entry
            big_spread_candidate = None
            big_spread_info = None
            
            with candidates_shared_lock:
                for sym, info in candidates_shared.items():
                    max_spread = info.get('max_spread', info.get('start_spread', 0.0))
                    if abs(max_spread) >= BIG_SPREAD_THRESHOLD:
                        big_spread_candidate = sym
                        big_spread_info = info
                        logger.info("BIG SPREAD DETECTED (no active trade): %s spread=%.4f%%", sym, max_spread)
                        break
            
            # If big spread found, enter immediately
            if big_spread_candidate and big_spread_info:
                logger.info("Entering on BIG SPREAD (>=%.1f%%) with no active trade: %s", BIG_SPREAD_THRESHOLD, big_spread_candidate)
                send_telegram(f"*BIG SPREAD ENTRY* `{big_spread_candidate}` spread `{big_spread_info.get('max_spread', 0.0):.4f}%` >= {BIG_SPREAD_THRESHOLD}%\nEntering with 2x notional\n{timestamp()}")
                
                # Create eval_info for execution
                sym = big_spread_candidate
                ku_api_sym = big_spread_info.get('ku_sym')
                bin_bid = big_spread_info.get('bin_bid')
                bin_ask = big_spread_info.get('bin_ask')
                kc_bid = big_spread_info.get('ku_bid')
                kc_ask = big_spread_info.get('ku_ask')
                
                if kc_bid > bin_ask:
                    trigger_spread = 100 * (kc_bid - bin_ask) / bin_ask
                    plan = ('binance', 'kucoin')
                else:
                    trigger_spread = 100 * (bin_bid - kc_ask) / kc_ask
                    plan = ('kucoin', 'binance')
                
                eval_info = {
                    'case': 'big_spread',
                    'plan': plan,
                    'trigger_spread': trigger_spread,
                    'net_fr': 0.0,
                    'time_left_min': None,
                    'bin_bid': bin_bid,
                    'bin_ask': bin_ask,
                    'kc_bid': kc_bid,
                    'kc_ask': kc_ask,
                    'ku_api_sym': ku_api_sym
                }
                
                execute_pair_trade_with_snapshots(sym, eval_info, initial_multiplier=BIG_SPREAD_ENTRY_MULTIPLIER)
                time.sleep(1)
                continue
            
            # Otherwise proceed with normal candidate evaluation
            candidate_list = []
            if best_pos:
                candidate_list.append(best_pos)
            if best_neg:
                candidate_list.append(best_neg)
            entry_taken = False
            for cand in candidate_list:
                sym, info = cand
                # Evaluate the entry conditions (cases)
                eval_info = evaluate_entry_conditions(sym, info)
                if not eval_info:
                    confirm_counts[sym] = 0
                    continue
                # Increment confirm counter
                confirm_counts[sym] = confirm_counts.get(sym, 0) + 1
                print(f"⏳ Confirming {sym}: {confirm_counts[sym]}/{CONFIRM_COUNT} (spread={eval_info.get('trigger_spread'):.4f}%)", flush=True)
                logger.info("Confirming %s: %d/%d (spread=%.4f%%)", sym, confirm_counts[sym], CONFIRM_COUNT, eval_info.get('trigger_spread'))
                if confirm_counts[sym] >= CONFIRM_COUNT:
                    print(f"✅ CONFIRMATION COMPLETE for {sym}! Re-evaluating before execution...", flush=True)
                    # re-evaluate immediately before executing
                    eval_info_refresh = evaluate_entry_conditions(sym, info)
                    if not eval_info_refresh:
                        print(f"❌ Re-evaluation failed for {sym}, resetting confirms", flush=True)
                        confirm_counts[sym] = 0
                        continue
                    print(f"🚀 EXECUTING TRADE: {sym}", flush=True)
                    # execute with pre/post snapshots
                    ok = execute_pair_trade_with_snapshots(sym, eval_info_refresh, initial_multiplier=1)
                    confirm_counts[sym] = 0
                    if ok:
                        print(f"✅ Trade executed successfully for {sym}", flush=True)
                        entry_taken = True
                        break
                    else:
                        print(f"❌ Trade execution failed for {sym}", flush=True)
                else:
                    # Wait for more confirms (loop continues)
                    pass
            # reset confirms for symbols not in candidate_list
            with candidates_shared_lock:
                current_syms = set(candidates_shared.keys())
            for s in list(confirm_counts.keys()):
                if s not in current_syms:
                    confirm_counts[s] = 0
        else:
            # Active trade present; maintenance threads handle averaging/TP; main loop sleeps
            pass
        time.sleep(1)
except KeyboardInterrupt:
    try:
        close_all_and_wait()
    except Exception:
        pass
except Exception:
    logger.exception("Unhandled exception at top level")
