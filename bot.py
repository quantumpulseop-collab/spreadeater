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
WATCHER_POLL_INTERVAL = float(os.getenv('WATCHER_POLL_INTERVAL', "2.0"))  # FIXED: Increased from 0.5s to 2s to reduce API calls
WATCHER_DETECT_CONFIRM = int(os.getenv('WATCHER_DETECT_CONFIRM', "2"))
MAX_NOTIONAL_MISMATCH_PCT = float(os.getenv('MAX_NOTIONAL_MISMATCH_PCT', "0.5"))
REBALANCE_MIN_DOLLARS = float(os.getenv('REBALANCE_MIN_DOLLARS', "0.5"))
STOP_BUFFER_PCT = float(os.getenv('STOP_BUFFER_PCT', "0.02"))
KC_TRANSIENT_ERROR_THRESHOLD = int(os.getenv('KC_TRANSIENT_ERROR_THRESHOLD', "10"))
KC_TRANSIENT_BACKOFF_SECONDS = float(os.getenv('KC_TRANSIENT_BACKOFF_SECONDS', "2.0"))

# Strategy-specific params (as agreed)
FR_ADVANTAGE_THRESHOLD = 0.0001  # CHANGED: Was 0.01%, now 0.0001%
CASE1_MIN_SPREAD = 1.8  # CHANGED: Was 1.5%, now 1.8%
CASE2_MULT = 13.0
CASE3_MULT = 15.0
BIG_SPREAD_THRESHOLD = 7.5
BIG_SPREAD_ENTRY_MULTIPLIER = 2
MAX_AVERAGES = 2
AVERAGE_TRIGGER_MULTIPLIER = 2.0
TAKE_PROFIT_FACTOR = 0.5
MIN_FR_DIFF_THRESHOLD = 0.0001  # CHANGED: Minimum funding rate difference (was 0.01%, now 0.0001%)
MIN_SPREAD_THRESHOLD = 1.8     # CHANGED: Minimum spread required for ANY entry (was 1.5%)

# UPDATED: Trading fees configuration (0.2% per exchange, 0.4% total for entry ONLY)
TRADING_FEE_PCT_PER_EXCHANGE = 0.2  # 0.2% fee per exchange (Binance + KuCoin)
ENTRY_TRADING_FEE_PCT = TRADING_FEE_PCT_PER_EXCHANGE * 2  # 0.4% total for opening (entry only, no exit fees)

# NEW: Funding fee tracking configuration
FUNDING_FEE_CHECK_INTERVAL = 30  # Check for new funding fees every 30 seconds
FUNDING_HISTORY_LOOKBACK_HOURS = 1  # Look back 1 hour for funding fee history

# NEW: Spread difference filter (reject if entry/exit spread diff > 30% of profit target)
MAX_SPREAD_DIFF_PCT_OF_TARGET = 30.0  # 30% of profit capture target

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

def get_binance_book(retries=2):
    """
    FIXED: Handle 418 (rate limit) errors gracefully with retry and backoff
    """
    for attempt in range(1, retries+1):
        try:
            r = requests.get(BINANCE_BOOK_URL, timeout=10)
            
            # CRITICAL FIX: Handle 418 (I'm a teapot) rate limit error
            if r.status_code == 418:
                logger.warning(f"⚠️  Binance book ticker returned 418 (rate limit), attempt {attempt}/{retries}")
                if attempt < retries:
                    backoff = 2.0 * attempt  # Increasing backoff: 2s, 4s, etc.
                    logger.info(f"Waiting {backoff}s before retry...")
                    time.sleep(backoff)
                    continue
                else:
                    logger.error("❌ Binance book ticker failed after retries (418 rate limit)")
                    return {}
            
            r.raise_for_status()
            data = r.json()
            out = {}
            for d in data:
                try:
                    out[d["symbol"]] = {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
                except Exception:
                    continue
            logger.debug(f"✓ Fetched {len(out)} Binance book entries")
            return out
        except Exception as e:
            logger.warning(f"[BINANCE_BOOK] fetch error (attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                logger.error("❌ Binance book ticker failed after all retries")
                return {}
            time.sleep(1.0)

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
def fetch_binance_funding(symbol, max_retries=2):
    """
    FIXED: Add retry logic with exponential backoff for 418 (rate limit) errors
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(BINANCE_FUND_URL.format(symbol=symbol), timeout=6)
            if r.status_code == 418:
                # Binance rate limit (I'm a teapot)
                wait_time = (attempt + 1) * 2.0  # Exponential backoff: 2s, 4s, etc.
                logger.debug(f"Binance funding rate 418 (rate limit) for {symbol}, attempt {attempt+1}/{max_retries}, waiting {wait_time}s")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
                    continue
                return None
            if r.status_code != 200:
                logger.debug(f"Binance funding rate non-200: {r.status_code} for {symbol}")
                return None
            d = r.json()
            fr_raw = d.get('fundingRate') or d.get('lastFundingRate')
            nft = d.get('nextFundingTime') or d.get('nextFundingRateTime')
            fr = None
            if fr_raw is not None:
                fr = float(fr_raw) * 100.0 if abs(float(fr_raw)) < 1.0 else float(fr_raw)
                
                # CRITICAL FIX: Check if funding rate is exactly 0.0000 (all zeros)
                # This is likely a data glitch - reject it
                if fr == 0.0 or abs(fr) < 1e-8:
                    logger.debug(f"Binance funding rate is 0.0000 for {symbol} - likely data glitch, rejecting")
                    return None  # Treat as fetch failure
                    
            next_ft = int(nft) if nft else None
            logger.debug(f"✓ Binance FR for {symbol}: {fr:.6f}% (next: {next_ft})")
            return {'fundingRatePct': fr, 'nextFundingTimeMs': next_ft}
        except Exception as e:
            logger.debug(f"fetch_binance_funding error for {symbol} (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1.0)
    
    logger.warning(f"❌ Failed to fetch Binance funding rate for {symbol} after {max_retries} attempts")
    return None

def fetch_kucoin_funding(symbol, max_retries=2):
    """
    FIXED: Add retry logic and validation to reject 0.0000 funding rates as glitches
    """
    for attempt in range(max_retries):
        try:
            # Try CCXT first
            try:
                fr_info = kucoin.fetchFundingRate(symbol)
                if fr_info:
                    fr = fr_info.get('fundingRate') or fr_info.get('info', {}).get('fundingRate')
                    next_ft = fr_info.get('nextFundingTime') or fr_info.get('info', {}).get('nextFundingTime')
                    fr_pct = None
                    if fr is not None:
                        fr_pct = float(fr) * 100.0 if abs(float(fr)) < 1.0 else float(fr)
                        
                        # CRITICAL FIX: Check if funding rate is exactly 0.0000 (all zeros)
                        # This is likely a data glitch - reject it
                        if fr_pct == 0.0 or abs(fr_pct) < 1e-8:
                            logger.warning(f"⚠️  KuCoin (CCXT) funding rate is 0.0000 for {symbol} - likely data glitch, rejecting")
                            # Don't return yet, try REST API fallback
                        else:
                            logger.debug(f"✓ KuCoin (CCXT) FR for {symbol}: {fr_pct:.6f}% (next: {next_ft})")
                            return {'fundingRatePct': fr_pct, 'nextFundingTimeMs': int(next_ft) if next_ft else None}
            except Exception as e:
                logger.debug(f"KuCoin CCXT fetch failed for {symbol}: {e}, trying REST API")
            
            # REST API fallback
            r = requests.get(KUCOIN_FUND_URL.format(symbol=symbol), timeout=6)
            if r.status_code != 200:
                logger.warning(f"KuCoin funding rate non-200: {r.status_code} for {symbol}")
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                return None
            d = r.json()
            data = d.get('data') or {}
            fr_raw = data.get('fundingRate') or data.get('fundingRate24h')
            nft = data.get('nextFundingTime')
            fr = None
            if fr_raw is not None:
                fr = float(fr_raw) * 100.0 if abs(float(fr_raw)) < 1.0 else float(fr_raw)
                
                # CRITICAL FIX: Check if funding rate is exactly 0.0000 (all zeros)
                # This is likely a data glitch - reject it
                if fr == 0.0 or abs(fr) < 1e-8:
                    logger.warning(f"⚠️  KuCoin (REST) funding rate is 0.0000 for {symbol} - likely data glitch, rejecting")
                    if attempt < max_retries - 1:
                        time.sleep(0.5)
                        continue
                    return None  # Treat as fetch failure after retries
                    
            next_ft = int(nft) if nft else None
            logger.debug(f"✓ KuCoin (REST) FR for {symbol}: {fr:.6f}% (next: {next_ft})")
            return {'fundingRatePct': fr, 'nextFundingTimeMs': next_ft}
        except Exception as e:
            logger.warning(f"fetch_kucoin_funding error for {symbol} (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(0.5)
    
    logger.error(f"❌ Failed to fetch KuCoin funding rate for {symbol} after {max_retries} attempts")
    return None

# NEW: Functions to fetch ACTUAL funding fee history (real paid/received amounts)
def fetch_binance_funding_history(symbol, start_time_ms, end_time_ms=None):
    """
    Fetch actual funding fees paid/received from Binance.
    Returns list of funding fee entries with timestamp and amount in USD.
    
    API: GET /fapi/v1/income (incomeType=FUNDING_FEE)
    """
    try:
        if end_time_ms is None:
            end_time_ms = int(time.time() * 1000)
        
        # Binance requires authenticated request
        params = {
            'symbol': symbol,
            'incomeType': 'FUNDING_FEE',
            'startTime': start_time_ms,
            'endTime': end_time_ms,
            'limit': 1000
        }
        
        # Use CCXT to make authenticated request
        response = binance.fapiPrivateGetIncome(params)
        
        funding_fees = []
        for entry in response:
            funding_fees.append({
                'timestamp': entry['time'],
                'symbol': entry['symbol'],
                'income': float(entry['income']),  # USD amount (positive = received, negative = paid)
                'asset': entry['asset']
            })
        
        logger.debug(f"✓ Fetched {len(funding_fees)} Binance funding fee entries for {symbol}")
        return funding_fees
        
    except Exception as e:
        logger.warning(f"Error fetching Binance funding history for {symbol}: {e}")
        return []

def fetch_kucoin_funding_history(symbol, start_time_ms, end_time_ms=None):
    """
    Fetch actual funding fees paid/received from KuCoin.
    Returns list of funding fee entries with timestamp and amount in USD.
    
    API: GET /api/v1/funding-history (for futures)
    """
    try:
        if end_time_ms is None:
            end_time_ms = int(time.time() * 1000)
        
        params = {
            'symbol': symbol,
            'startAt': start_time_ms,
            'endAt': end_time_ms
        }
        
        # Use CCXT to make authenticated request
        # KuCoin uses /api/v1/funding-history endpoint
        response = kucoin.futuresPrivateGetFundingHistory(params)
        
        funding_fees = []
        if response.get('code') == '200000' and response.get('data'):
            data_list = response['data'].get('dataList', [])
            for entry in data_list:
                funding_fees.append({
                    'timestamp': entry['timePoint'],
                    'symbol': entry['symbol'],
                    'income': float(entry['funding']),  # USD amount (positive = received, negative = paid)
                })
        
        logger.debug(f"✓ Fetched {len(funding_fees)} KuCoin funding fee entries for {symbol}")
        return funding_fees
        
    except Exception as e:
        logger.warning(f"Error fetching KuCoin funding history for {symbol}: {e}")
        return []

def get_total_funding_fees_since_entry(symbol, ku_api_symbol, entry_timestamp_ms):
    """
    Get total funding fees paid/received since trade entry.
    
    FIXED: Now tracks individual funding events by timestamp to prevent double counting.
    Only processes NEW funding events that haven't been seen before.
    
    Returns net funding amount in USD (positive = we paid, negative = we received).
    
    Args:
        symbol: Binance symbol (e.g., 'BTCUSDT')
        ku_api_symbol: KuCoin symbol (e.g., 'BTCUSDTM')
        entry_timestamp_ms: Entry time in milliseconds
        
    Returns:
        dict with 'binance_fees_usd', 'kucoin_fees_usd', 'net_fees_usd', 'new_events_count'
    """
    try:
        current_time_ms = int(time.time() * 1000)
        
        # Fetch funding history from both exchanges
        binance_history = fetch_binance_funding_history(symbol, entry_timestamp_ms, current_time_ms)
        kucoin_history = fetch_kucoin_funding_history(ku_api_symbol, entry_timestamp_ms, current_time_ms)
        
        # Get previously seen timestamps from active_trade
        seen_binance_timestamps = active_trade.get('seen_binance_funding_timestamps', set())
        seen_kucoin_timestamps = active_trade.get('seen_kucoin_funding_timestamps', set())
        binance_events = active_trade.get('binance_funding_events', [])
        kucoin_events = active_trade.get('kucoin_funding_events', [])
        
        # Process NEW Binance funding events only
        new_binance_events = 0
        for entry in binance_history:
            timestamp = entry['timestamp']
            income = entry['income']
            
            # Only process if we haven't seen this timestamp before
            if timestamp not in seen_binance_timestamps:
                seen_binance_timestamps.add(timestamp)
                binance_events.append({
                    'timestamp': timestamp,
                    'amount': income,
                    'datetime': datetime.fromtimestamp(timestamp / 1000.0).isoformat()
                })
                new_binance_events += 1
                logger.info(f"  ✓ NEW Binance funding event at {datetime.fromtimestamp(timestamp / 1000.0)}: ${income:+.4f}")
        
        # Process NEW KuCoin funding events only
        new_kucoin_events = 0
        for entry in kucoin_history:
            timestamp = entry['timestamp']
            income = entry['income']
            
            # Only process if we haven't seen this timestamp before
            if timestamp not in seen_kucoin_timestamps:
                seen_kucoin_timestamps.add(timestamp)
                kucoin_events.append({
                    'timestamp': timestamp,
                    'amount': income,
                    'datetime': datetime.fromtimestamp(timestamp / 1000.0).isoformat()
                })
                new_kucoin_events += 1
                logger.info(f"  ✓ NEW KuCoin funding event at {datetime.fromtimestamp(timestamp / 1000.0)}: ${income:+.4f}")
        
        # Calculate total fees from ALL tracked events (not just new ones)
        binance_fees_total = sum(evt['amount'] for evt in binance_events)
        kucoin_fees_total = sum(evt['amount'] for evt in kucoin_events)
        net_fees_usd = binance_fees_total + kucoin_fees_total
        
        # Update active_trade with new tracking data
        active_trade['seen_binance_funding_timestamps'] = seen_binance_timestamps
        active_trade['seen_kucoin_funding_timestamps'] = seen_kucoin_timestamps
        active_trade['binance_funding_events'] = binance_events
        active_trade['kucoin_funding_events'] = kucoin_events
        
        logger.info(f"Funding summary | Binance: {len(binance_events)} events (${binance_fees_total:+.4f}) | "
                   f"KuCoin: {len(kucoin_events)} events (${kucoin_fees_total:+.4f}) | "
                   f"Net: ${net_fees_usd:+.4f} | New events: {new_binance_events + new_kucoin_events}")
        
        return {
            'binance_fees_usd': binance_fees_total,
            'kucoin_fees_usd': kucoin_fees_total,
            'net_fees_usd': net_fees_usd,
            'timestamp_checked': current_time_ms,
            'new_events_count': new_binance_events + new_kucoin_events,
            'binance_events_count': len(binance_events),
            'kucoin_events_count': len(kucoin_events)
        }
        
    except Exception as e:
        logger.exception(f"Error calculating total funding fees: {e}")
        return {
            'binance_fees_usd': 0.0,
            'kucoin_fees_usd': 0.0,
            'net_fees_usd': 0.0,
            'timestamp_checked': int(time.time() * 1000),
            'new_events_count': 0,
            'binance_events_count': 0,
            'kucoin_events_count': 0
        }

def fetch_binance_book_ticker(symbol):
    """
    Fetch Binance book ticker data for a specific symbol.
    Returns dict with 'bid' and 'ask' keys, or None on failure.
    """
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if 'bidPrice' in data and 'askPrice' in data:
            return {
                'bid': float(data['bidPrice']),
                'ask': float(data['askPrice'])
            }
        else:
            logger.warning(f"❌ fetch_binance_book_ticker: Missing price data for {symbol}")
            return None
            
    except Exception as e:
        logger.error(f"❌ fetch_binance_book_ticker error for {symbol}: {e}")
        return None

def fetch_kucoin_book_ticker(symbol):
    """
    Fetch KuCoin book ticker data for a specific symbol.
    Returns dict with 'bid' and 'ask' keys, or None on failure.
    """
    try:
        url = f"https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if data.get('code') == '200000' and 'data' in data:
            ticker_data = data['data']
            best_bid = ticker_data.get('bestBidPrice')
            best_ask = ticker_data.get('bestAskPrice')
            
            if best_bid and best_ask:
                return {
                    'bid': float(best_bid),
                    'ask': float(best_ask)
                }
            else:
                logger.warning(f"❌ fetch_kucoin_book_ticker: Missing price data for {symbol}")
                return None
        else:
            logger.warning(f"❌ fetch_kucoin_book_ticker: Invalid response for {symbol}")
            return None
            
    except Exception as e:
        logger.error(f"❌ fetch_kucoin_book_ticker error for {symbol}: {e}")
        return None

def reconfirm_entry_spread(sym, ku_api_sym, bin_ask, bin_bid, kc_ask, kc_bid, expected_plan):
    """
    Reconfirm entry spread by fetching fresh prices.
    Returns (success, entry_spread, plan, bin_bid, bin_ask, kc_bid, kc_ask) or (False, None, None, None, None, None, None)
    """
    try:
        # Fetch fresh book data
        bin_data = fetch_binance_book_ticker(sym)
        kc_data = fetch_kucoin_book_ticker(ku_api_sym)
        
        if not bin_data or not kc_data:
            logger.warning(f"❌ Reconfirm failed for {sym}: Could not fetch fresh prices")
            return (False, None, None, None, None, None, None)
        
        fresh_bin_bid = bin_data.get('bid')
        fresh_bin_ask = bin_data.get('ask')
        fresh_kc_bid = kc_data.get('bid')
        fresh_kc_ask = kc_data.get('ask')
        
        if not all([fresh_bin_bid, fresh_bin_ask, fresh_kc_bid, fresh_kc_ask]):
            logger.warning(f"❌ Reconfirm failed for {sym}: Missing price data")
            return (False, None, None, None, None, None, None)
        
        # Calculate entry spread based on plan
        if expected_plan == ('binance', 'kucoin'):
            # Long binance, short kucoin: use kc_bid - bin_ask
            entry_spread = 100 * (fresh_kc_bid - fresh_bin_ask) / fresh_bin_ask if fresh_bin_ask > 0 else 0
            plan = ('binance', 'kucoin')
        else:
            # Short binance, long kucoin: use bin_bid - kc_ask
            entry_spread = 100 * (fresh_bin_bid - fresh_kc_ask) / fresh_kc_ask if fresh_kc_ask > 0 else 0
            plan = ('kucoin', 'binance')
        
        logger.info(f"📊 Reconfirmed entry spread for {sym}: {entry_spread:.4f}% (plan={plan})")
        return (True, entry_spread, plan, fresh_bin_bid, fresh_bin_ask, fresh_kc_bid, fresh_kc_ask)
        
    except Exception as e:
        logger.exception(f"Error reconfirming entry spread for {sym}: {e}")
        return (False, None, None, None, None, None, None)

def compute_net_funding_for_plan(bin_fr_pct, kc_fr_pct, plan_long='binance', plan_short='kucoin'):
    """
    FIXED: Don't treat None as 0.0 - if either funding rate is None, return None
    This prevents bad entries when funding rate fetch fails
    """
    try:
        # CRITICAL FIX: If either funding rate is None, return None (don't assume 0)
        if bin_fr_pct is None or kc_fr_pct is None:
            logger.warning(f"⚠️  Cannot compute net funding: bin_fr={bin_fr_pct} kc_fr={kc_fr_pct} - returning None")
            return None
        
        fb = float(bin_fr_pct)
        fk = float(kc_fr_pct)
        
        # CRITICAL FIX: Additional check - if either is exactly 0.0, log warning
        # (this shouldn't happen now due to fetch validation, but double-check)
        if abs(fb) < 1e-8 or abs(fk) < 1e-8:
            logger.warning(f"⚠️  Suspicious funding rate (near zero): bin_fr={fb:.6f}% kc_fr={fk:.6f}% - returning None")
            return None
        
        if plan_long == 'binance' and plan_short == 'kucoin':
            net = fk - fb
        elif plan_long == 'kucoin' and plan_short == 'binance':
            net = fb - fk
        else:
            logger.error(f"Invalid plan: long={plan_long} short={plan_short}")
            return None
        
        logger.debug(f"✓ Net funding: bin={fb:.6f}% kc={fk:.6f}% → net={net:.6f}% (long={plan_long}, short={plan_short})")
        return net
    except Exception as e:
        logger.exception(f"Error computing net funding: {e}")
        return None
        return 0.0
    except Exception:
        return 0.0

# FIXED: Add dynamic funding interval detection
def detect_funding_interval_hours(bin_next_ms, kc_next_ms):
    """
    Dynamically detect funding interval by checking time until next funding.
    Returns estimated interval in hours (1, 2, 4, or 8).
    """
    try:
        now_ms = int(time.time() * 1000)
        intervals = []
        
        if bin_next_ms and bin_next_ms > now_ms:
            time_left_ms = bin_next_ms - now_ms
            time_left_hours = time_left_ms / (1000 * 60 * 60)
            # Round to nearest standard interval
            if time_left_hours <= 1.5:
                intervals.append(1)
            elif time_left_hours <= 3:
                intervals.append(2)
            elif time_left_hours <= 6:
                intervals.append(4)
            else:
                intervals.append(8)
        
        if kc_next_ms and kc_next_ms > now_ms:
            time_left_ms = kc_next_ms - now_ms
            time_left_hours = time_left_ms / (1000 * 60 * 60)
            # Round to nearest standard interval
            if time_left_hours <= 1.5:
                intervals.append(1)
            elif time_left_hours <= 3:
                intervals.append(2)
            elif time_left_hours <= 6:
                intervals.append(4)
            else:
                intervals.append(8)
        
        if intervals:
            # Use minimum interval (most frequent funding)
            return min(intervals)
        return 4  # Default to 4h if can't detect
    except Exception:
        logger.exception("Error detecting funding interval")
        return 4  # Default fallback

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
    
    # ENHANCED LOGGING: Track who called this function and why
    import traceback
    stack = traceback.extract_stack()
    caller_info = f"{stack[-2].filename}:{stack[-2].lineno} in {stack[-2].name}"
    
    logger.info("=" * 80)
    logger.info(f"🔄 CLOSING ALL POSITIONS (reason={closing_reason or 'UNKNOWN'})")
    logger.info(f"📞 Called from: {caller_info}")
    logger.info(f"🔒 Flags: closing_reason={closing_reason}, tp_closing_in_progress={tp_closing_in_progress}, override_execution_in_progress={override_execution_in_progress}")
    logger.info("=" * 80)
    print(f"🔄 Closing all positions (reason={closing_reason or 'UNKNOWN'})...", flush=True)
    
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

def cancel_all_open_orders_aggressive(target_sym):
    """
    FIXED: Aggressively cancel ALL orders and VERIFY they're gone before proceeding.
    This is CRITICAL for Binance because conditional STOP_MARKET orders prevent
    reduce-only market orders from executing.
    """
    logger.info("🔄 Aggressively cancelling ALL orders for %s...", target_sym)
    
    max_attempts = 3
    for attempt in range(max_attempts):
        # Cancel Binance orders
        try:
            binance_orders = binance.fetch_open_orders(target_sym)
            if binance_orders:
                logger.info(f"Attempt {attempt+1}: Found {len(binance_orders)} Binance orders to cancel")
                for order in binance_orders:
                    try:
                        binance.cancel_order(order['id'], target_sym)
                        logger.info(f"✅ Cancelled Binance order {order['id']} (type={order.get('type')})")
                    except Exception as e:
                        logger.warning(f"Failed to cancel Binance order {order.get('id')}: {e}")
                
                # CRITICAL: Verify cancellation
                time.sleep(0.5)  # Give exchange time to process
                verify_orders = binance.fetch_open_orders(target_sym)
                if not verify_orders:
                    logger.info("✅ All Binance orders cancelled successfully")
                    break
                else:
                    logger.warning(f"⚠️  Still {len(verify_orders)} Binance orders remaining, retrying...")
            else:
                logger.info("No Binance orders to cancel")
                break
        except Exception as e:
            logger.warning(f"Error in Binance order cancellation attempt {attempt+1}: {e}")
        
        if attempt < max_attempts - 1:
            time.sleep(0.3)
    
    # Cancel KuCoin orders (for the ku_ccxt symbol)
    try:
        ku_ccxt_sym = active_trade.get('ku_ccxt')
        if ku_ccxt_sym:
            kucoin_orders = kucoin.fetch_open_orders(ku_ccxt_sym)
            if kucoin_orders:
                logger.info(f"Found {len(kucoin_orders)} KuCoin orders to cancel")
                for order in kucoin_orders:
                    try:
                        kucoin.cancel_order(order['id'], ku_ccxt_sym)
                        logger.info(f"✅ Cancelled KuCoin order {order['id']}")
                    except Exception as e:
                        logger.warning(f"Failed to cancel KuCoin order {order.get('id')}: {e}")
    except Exception as e:
        logger.warning(f"Error cancelling KuCoin orders: {e}")
    
    logger.info("✅ Aggressive order cancellation complete")

def close_all_and_wait_with_tracking(target_sym, timeout_s=10, poll_interval=0.3):
    """
    FIXED: Use the SAME reliable closing method as liquidation watcher (close_single_exchange_position)
    This ensures positions actually close instead of silently failing
    Returns dict with bin_exec_price and kc_exec_price
    """
    global closing_in_progress
    closing_in_progress = True
    logger.info("🔄 Closing all positions with tracking for %s...", target_sym)
    
    # CRITICAL FIX: Use AGGRESSIVE order cancellation with verification
    cancel_all_open_orders_aggressive(target_sym)
    
    result = {'bin_exec_price': None, 'kc_exec_price': None}
    
    # CRITICAL FIX: Use the SAME method as liquidation watcher - it WORKS!
    # Get the KuCoin CCXT symbol
    ku_ccxt_sym = active_trade.get('ku_ccxt')
    
    # PARALLEL closing using the liquidation watcher's reliable method
    def close_binance_reliable():
        """Use the liquidation watcher's method that ACTUALLY WORKS"""
        logger.info(f"🔄 Closing Binance {target_sym} using liquidation watcher method...")
        success = close_single_exchange_position(binance, target_sym)
        logger.info(f"{'✅' if success else '❌'} Binance close result: {success}")
        return success
    
    def close_kucoin_reliable():
        """Use the liquidation watcher's method that ACTUALLY WORKS"""
        if ku_ccxt_sym:
            logger.info(f"🔄 Closing KuCoin {ku_ccxt_sym} using liquidation watcher method...")
            success = close_single_exchange_position(kucoin, ku_ccxt_sym)
            logger.info(f"{'✅' if success else '❌'} KuCoin close result: {success}")
            return success
        else:
            logger.warning("⚠️  No KuCoin CCXT symbol, skipping KuCoin close")
            return False
    
    # Execute BOTH closings in PARALLEL using threads
    thread_bin = threading.Thread(target=close_binance_reliable, name="BinanceCloser")
    thread_kc = threading.Thread(target=close_kucoin_reliable, name="KuCoinCloser")
    
    logger.info("⚡ Starting PARALLEL position closing on both exchanges...")
    start_close = time.time()
    thread_bin.start()
    thread_kc.start()
    
    # Wait for both to complete
    thread_bin.join()
    thread_kc.join()
    close_duration = time.time() - start_close
    logger.info("✅ Both exchange closings completed in %.2f seconds", close_duration)
    
    # Wait for positions to close (with faster polling)
    start = time.time()
    check_count = 0
    while time.time() - start < timeout_s:
        check_count += 1
        try:
            bin_check = binance.fetch_positions()
            kc_check = kucoin.fetch_positions()
            any_open = False
            
            # Check if positions are still open with detailed logging
            for p in (bin_check or []):
                qty = abs(float(_get_signed_from_binance_pos(p) or 0))
                if qty > 0:
                    sym = p.get('symbol')
                    logger.info(f"⚠️  Binance {sym} still has {qty} contracts open")
                    any_open = True
                    break
            
            if not any_open:
                for p in (kc_check or []):
                    qty = abs(float(_get_signed_from_kucoin_pos(p) or 0))
                    if qty > 0:
                        sym = p.get('symbol')
                        logger.info(f"⚠️  KuCoin {sym} still has {qty} contracts open")
                        any_open = True
                        break
            
            elapsed = time.time() - start
            logger.info(f"Position check #{check_count}: any_open={any_open}, elapsed={elapsed:.2f}s")
            
            if not any_open:
                closing_in_progress = False
                total_bal, bin_bal, kc_bal = get_total_futures_balance()
                logger.info("✅ All positions closed in %.2f seconds", elapsed)
                logger.info("*** POST-EXIT Total Balance approx: ${:.2f} (Binance: ${:.2f} | KuCoin: ${:.2f}) ***".format(total_bal, bin_bal, kc_bal))
                logger.info("EXIT TRACKING | Positions closed successfully")
                return result
        except Exception as e:
            logger.exception(f"Error checking positions (attempt #{check_count}): {e}")
        
        time.sleep(poll_interval)
    
    # Timeout reached
    closing_in_progress = False
    logger.error("🚨 TIMEOUT: Positions did not close within %ds after %d checks!", timeout_s, check_count)
    
    # Log which positions are still open
    try:
        bin_check = binance.fetch_positions()
        for p in bin_check:
            qty = abs(float(_get_signed_from_binance_pos(p) or 0))
            if qty > 0:
                logger.error(f"🚨 STILL OPEN: Binance {p.get('symbol')} has {qty} contracts")
    except Exception:
        pass
    
    try:
        kc_check = kucoin.fetch_positions()
        for p in kc_check:
            qty = abs(float(_get_signed_from_kucoin_pos(p) or 0))
            if qty > 0:
                logger.error(f"🚨 STILL OPEN: KuCoin {p.get('symbol')} has {qty} contracts")
    except Exception:
        pass
    
    return result

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

# FIXED: Cache Binance positions to avoid excessive API calls
_binance_position_cache = {}
_binance_position_cache_lock = threading.Lock()
_binance_position_cache_time = {}
BINANCE_POSITION_CACHE_TTL = 5.0  # FIXED: Increased from 2s to 5s to reduce API calls

# FIXED: Global rate limit tracking to automatically slow down
_binance_rate_limit_hits = 0
_binance_rate_limit_lock = threading.Lock()
_last_rate_limit_time = 0

def check_and_handle_rate_limit():
    """
    Check if we're hitting rate limits too often and slow down if needed
    Returns: sleep_time in seconds (0 if no slowdown needed)
    """
    global _binance_rate_limit_hits, _last_rate_limit_time
    
    with _binance_rate_limit_lock:
        now = time.time()
        
        # Reset counter every 60 seconds
        if now - _last_rate_limit_time > 60:
            _binance_rate_limit_hits = 0
            _last_rate_limit_time = now
            return 0
        
        # If we've hit rate limits more than 5 times in a minute, slow down dramatically
        if _binance_rate_limit_hits > 5:
            logger.warning(f"⚠️  Hit rate limits {_binance_rate_limit_hits} times in last minute - forcing 10s cooldown")
            return 10.0
        elif _binance_rate_limit_hits > 3:
            logger.info(f"Hit rate limits {_binance_rate_limit_hits} times in last minute - forcing 5s cooldown")
            return 5.0
        elif _binance_rate_limit_hits > 0:
            return 2.0
        
        return 0

def record_rate_limit_hit():
    """Record that we hit a rate limit"""
    global _binance_rate_limit_hits, _last_rate_limit_time
    
    with _binance_rate_limit_lock:
        now = time.time()
        if now - _last_rate_limit_time > 60:
            _binance_rate_limit_hits = 1
            _last_rate_limit_time = now
        else:
            _binance_rate_limit_hits += 1

def _fetch_signed_binance(sym):
    """
    FIXED: Use cached positions to avoid hitting rate limits.
    This function is called very frequently by the liquidation watcher.
    """
    try:
        # FIXED: Check if we need to slow down due to rate limits
        sleep_time = check_and_handle_rate_limit()
        if sleep_time > 0:
            time.sleep(sleep_time)
        
        now = time.time()
        with _binance_position_cache_lock:
            # Check if we have a recent cached value
            if sym in _binance_position_cache and sym in _binance_position_cache_time:
                if now - _binance_position_cache_time[sym] < BINANCE_POSITION_CACHE_TTL:
                    return _binance_position_cache[sym]
        
        # Need to fetch fresh data
        p = binance.fetch_positions([sym])
        if not p:
            with _binance_position_cache_lock:
                _binance_position_cache[sym] = 0.0
                _binance_position_cache_time[sym] = now
            return 0.0
        pos = p[0]
        signed_qty = _get_signed_from_binance_pos(pos)
        
        # Update cache
        with _binance_position_cache_lock:
            _binance_position_cache[sym] = signed_qty
            _binance_position_cache_time[sym] = now
        
        return signed_qty
    except Exception as e:
        # FIXED: Only log rate limit errors at DEBUG level to reduce noise
        error_str = str(e)
        if '418' in error_str or 'rate limit' in error_str.lower() or 'banned' in error_str.lower():
            logger.debug(f"BINANCE rate limit for {sym}: {e}")
            record_rate_limit_hit()  # Track this hit
            # If rate limited, return last known value from cache
            with _binance_position_cache_lock:
                if sym in _binance_position_cache:
                    logger.debug(f"Using cached position for {sym} due to rate limit")
                    return _binance_position_cache[sym]
        else:
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
        global terminate_bot  # FIXED: Declare at function start
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
                # FIXED: Skip monitoring when TP closing is in progress (not a liquidation!)
                if tp_closing_in_progress:
                    zero_cnt_bin = zero_cnt_kc = 0
                    logger.info(f"{datetime.now().isoformat()} Liquidation watcher skipping (TP closing in progress)")
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue
                
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
                    logger.debug(f"{datetime.now().isoformat()} WATCHER BINANCE fetch error -> using last-known bin qty {cur_bin}")
                else:
                    _last_known_positions.setdefault(sym, {})['bin'] = cur_bin
                cur_kc = None
                try:
                    cur_kc = _fetch_signed_kucoin(kc_ccxt_sym)
                    _last_known_positions.setdefault(sym, {})['kc'] = cur_kc
                    _last_known_positions.setdefault(sym, {})['kc_err_count'] = 0
                except Exception as e:
                    _last_known_positions.setdefault(sym, {})['kc_err_count'] = _last_known_positions.setdefault(sym, {}).get('kc_err_count', 0) + 1
                    logger.debug(f"{datetime.now().isoformat()} KUCOIN fetch error for {kc_ccxt_sym}: {e} (consecutive_errors={_last_known_positions[sym]['kc_err_count']})")
                    cur_kc = _last_known_positions.get(sym, {}).get('kc', None)
                    if _last_known_positions[sym]['kc_err_count'] >= KC_TRANSIENT_ERROR_THRESHOLD:
                        logger.warning(f"{datetime.now().isoformat()} KUCOIN persistent errors >= {KC_TRANSIENT_ERROR_THRESHOLD}; backing off")
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
                    logger.debug(f"{datetime.now().isoformat()} WATCHER SKIP (no KuCoin reading available) prev_bin={prev_bin} prev_kc={prev_kc} cur_bin={cur_bin_f} cur_kc=None")
                    prev_bin = cur_bin_f
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue
                cur_bin_abs = abs(cur_bin_f)
                cur_kc_abs = abs(cur_kc_f)
                prev_bin_abs = abs(prev_bin)
                prev_kc_abs = abs(prev_kc)
                bin_initial_nonzero = abs(entry_initial_qtys.get(sym, {}).get('bin', 0.0)) > ZERO_ABS_THRESHOLD
                kc_initial_nonzero = abs(entry_initial_qtys.get(sym, {}).get('kc', 0.0)) > ZERO_ABS_THRESHOLD
                
                # FIXED: Don't trigger liquidation actions if TP closing just finished
                if tp_closing_in_progress:
                    time.sleep(WATCHER_POLL_INTERVAL)
                    continue
                
                if (prev_bin_abs > ZERO_ABS_THRESHOLD or bin_initial_nonzero) and cur_bin_abs <= ZERO_ABS_THRESHOLD:
                    logger.info(f"{datetime.now().isoformat()} Detected immediate ZERO on Binance (prev non-zero -> now zero).")
                    
                    # FIXED: Check if this is a planned closure (TP, OVERRIDE) or a real liquidation
                    if closing_reason in ['TP', 'OVERRIDE'] or tp_closing_in_progress:
                        logger.info(f"✅ PLANNED CLOSURE detected for {sym} (reason: {closing_reason or 'TP'}) - liquidation watcher exiting normally, bot continues scanning")
                        break  # Exit watcher, don't terminate bot
                    
                    # This is a REAL liquidation or stop-loss hit - EMERGENCY
                    logger.warning(f"⚠️⚠️⚠️ EMERGENCY: Unexpected position closure on Binance for {sym} - likely liquidation or stop-loss!")
                    logger.warning(f"⚠️⚠️⚠️ closing_reason={closing_reason}, tp_closing_in_progress={tp_closing_in_progress}, override_execution_in_progress={override_execution_in_progress}")
                    send_telegram(f"🚨 *EMERGENCY CLOSURE* 🚨\n`{sym}` position closed unexpectedly on Binance\nLikely liquidation or stop-loss hit!\nClosing other exchange and stopping bot for safety.\n{timestamp()}")
                    
                    try:
                        ok = close_single_exchange_position(kucoin, kc_ccxt_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after Binance liquidation: %s", e)
                    
                    terminate_bot = True  # STOP BOT on real liquidation
                    break
                if (prev_kc_abs > ZERO_ABS_THRESHOLD or kc_initial_nonzero) and cur_kc_abs <= ZERO_ABS_THRESHOLD:
                    logger.info(f"{datetime.now().isoformat()} Detected immediate ZERO on KuCoin (prev non-zero -> now zero).")
                    
                    # FIXED: Check if this is a planned closure (TP, OVERRIDE) or a real liquidation
                    if closing_reason in ['TP', 'OVERRIDE'] or tp_closing_in_progress:
                        logger.info(f"✅ PLANNED CLOSURE detected for {sym} (reason: {closing_reason or 'TP'}) - liquidation watcher exiting normally, bot continues scanning")
                        break  # Exit watcher, don't terminate bot
                    
                    # This is a REAL liquidation or stop-loss hit - EMERGENCY
                    logger.warning(f"⚠️⚠️⚠️ EMERGENCY: Unexpected position closure on KuCoin for {sym} - likely liquidation or stop-loss!")
                    logger.warning(f"⚠️⚠️⚠️ closing_reason={closing_reason}, tp_closing_in_progress={tp_closing_in_progress}, override_execution_in_progress={override_execution_in_progress}")
                    send_telegram(f"🚨 *EMERGENCY CLOSURE* 🚨\n`{sym}` position closed unexpectedly on KuCoin\nLikely liquidation or stop-loss hit!\nClosing other exchange and stopping bot for safety.\n{timestamp()}")
                    
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after KuCoin liquidation: %s", e)
                    
                    terminate_bot = True  # STOP BOT on real liquidation
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
                    logger.info(f"{datetime.now().isoformat()} Detected sustained ZERO on Binance.")
                    
                    # FIXED: Check if this is a planned closure (TP, OVERRIDE) or a real liquidation
                    if closing_reason in ['TP', 'OVERRIDE'] or tp_closing_in_progress:
                        logger.info(f"✅ PLANNED CLOSURE detected for {sym} (reason: {closing_reason or 'TP'}) - liquidation watcher exiting normally, bot continues scanning")
                        break  # Exit watcher, don't terminate bot
                    
                    # This is a REAL liquidation or stop-loss hit - EMERGENCY
                    logger.warning(f"⚠️⚠️⚠️ EMERGENCY: Sustained position closure on Binance for {sym} - likely liquidation or stop-loss!")
                    logger.warning(f"⚠️⚠️⚠️ closing_reason={closing_reason}, tp_closing_in_progress={tp_closing_in_progress}, override_execution_in_progress={override_execution_in_progress}")
                    send_telegram(f"🚨 *EMERGENCY CLOSURE* 🚨\n`{sym}` position sustained zero on Binance\nLikely liquidation or stop-loss hit!\nClosing other exchange and stopping bot for safety.\n{timestamp()}")
                    
                    try:
                        ok = close_single_exchange_position(kucoin, kc_ccxt_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted KuCoin close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after Binance sustained liquidation: %s", e)
                    
                    terminate_bot = True  # STOP BOT on real liquidation
                    break
                if zero_cnt_kc >= WATCHER_DETECT_CONFIRM:
                    logger.info(f"{datetime.now().isoformat()} Detected sustained ZERO on KuCoin.")
                    
                    # FIXED: Check if this is a planned closure (TP, OVERRIDE) or a real liquidation
                    if closing_reason in ['TP', 'OVERRIDE'] or tp_closing_in_progress:
                        logger.info(f"✅ PLANNED CLOSURE detected for {sym} (reason: {closing_reason or 'TP'}) - liquidation watcher exiting normally, bot continues scanning")
                        break  # Exit watcher, don't terminate bot
                    
                    # This is a REAL liquidation or stop-loss hit - EMERGENCY
                    logger.warning(f"⚠️⚠️⚠️ EMERGENCY: Sustained position closure on KuCoin for {sym} - likely liquidation or stop-loss!")
                    logger.warning(f"⚠️⚠️⚠️ closing_reason={closing_reason}, tp_closing_in_progress={tp_closing_in_progress}, override_execution_in_progress={override_execution_in_progress}")
                    send_telegram(f"🚨 *EMERGENCY CLOSURE* 🚨\n`{sym}` position sustained zero on KuCoin\nLikely liquidation or stop-loss hit!\nClosing other exchange and stopping bot for safety.\n{timestamp()}")
                    
                    try:
                        ok = close_single_exchange_position(binance, bin_sym)
                        if not ok:
                            logger.info(f"{datetime.now().isoformat()} Targeted Binance close failed; falling back to global close.")
                        close_all_and_wait()
                    except Exception as e:
                        logger.exception("Error when closing after KuCoin sustained liquidation: %s", e)
                    
                    terminate_bot = True  # STOP BOT on real liquidation
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
tp_closing_in_progress = False  # NEW: Track if closing for TP (not liquidation)
closing_reason = None  # NEW: Track WHY we're closing: 'TP', 'LIQUIDATION', 'OVERRIDE', or None
override_execution_in_progress = False  # NEW: Track if override trade is executing (prevents race conditions)
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
    'suppress_full_scan': False,
    'plan': None,  # FIXED: Store plan ('binance', 'kucoin') or ('kucoin', 'binance')
    'avg_count': 0,  # NEW: Track averaging count
    'final_averaged_price_bin': None,  # NEW: Track final averaged entry price on Binance
    'final_averaged_price_kc': None,   # NEW: Track final averaged entry price on KuCoin
    # NEW: Accumulated expenses tracking
    'accumulated_expenses_pct': 0.0,  # Total expenses including fees (as % of notional)
    'total_notional': 0.0,  # Total position size for fee calculation
    # NEW: Exit spread tracking
    'exit_trigger_spread': None,  # Spread when exit was triggered
    'exit_real_spread': None,  # Actual spread from executed prices
    'exit_trigger_bin_bid': None,
    'exit_trigger_bin_ask': None,
    'exit_trigger_kc_bid': None,
    'exit_trigger_kc_ask': None,
    # NEW: Balance tracking
    'balance_before_close': None,
    'balance_after_close': None,
    # FIXED: Track individual funding events by timestamp to prevent double counting
    'seen_binance_funding_timestamps': set(),  # Set of timestamps we've already processed
    'seen_kucoin_funding_timestamps': set(),   # Set of timestamps we've already processed
    'binance_funding_events': [],  # List of {'timestamp': ms, 'amount': usd, 'datetime': str}
    'kucoin_funding_events': []    # List of {'timestamp': ms, 'amount': usd, 'datetime': str}
}

# Confirm counters per symbol (consecutive confirms)
confirm_counts = {}
CONFIRM_COUNT = 3

# NEW: TP confirmation counter (need 3 consecutive confirmations to exit)
tp_confirm_count = 0
TP_CONFIRM_COUNT = 3

# NEW: Override confirmation counter (need 3 consecutive confirmations for big spread override)
override_confirm_count = 0
OVERRIDE_CONFIRM_COUNT = 3

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
    ADDED: Minimum 1.8% spread requirement for ALL entries
    CRITICAL FIX: Reject entry if funding rates are None (fetch failed or 0.0000 glitch)
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
    
    # NEW: Check minimum spread threshold FIRST
    if abs(trigger_spread) < MIN_SPREAD_THRESHOLD:
        return None  # Spread too small, reject entry
    
    # NEW: Calculate ACTUAL exit spread and check if entry-exit difference is too large
    # Entry spread uses aggressive prices (e.g., kc_bid - bin_ask for long bin/short kc)
    # Exit spread uses opposite prices (e.g., kc_ask - bin_bid for same position)
    # ONLY reject if exit spread > entry spread (unfavorable), meaning spread widened against us
    # If entry spread > exit spread, that's GOOD (instant profit on exit) - don't reject!
    if plan == ('binance', 'kucoin'):
        # Long binance, short kucoin
        # Entry: kc_bid - bin_ask (already calculated as trigger_spread)
        # Exit: kc_ask - bin_bid
        exit_spread = 100 * (kc_ask - bin_bid) / bin_bid if bin_bid > 0 else 0
    else:
        # Short binance, long kucoin
        # Entry: bin_bid - kc_ask (already calculated as trigger_spread)
        # Exit: bin_ask - kc_bid
        exit_spread = 100 * (bin_ask - kc_bid) / kc_bid if kc_bid > 0 else 0
    
    # ONLY check if exit spread is WORSE than entry spread (exit > entry for positive, exit < entry for negative)
    # This means the spread crossing costs us money
    if exit_spread > trigger_spread:
        # Exit spread is worse - calculate how much worse
        spread_difference = exit_spread - trigger_spread  # How much worse exit is than entry
        profit_target_pct = TAKE_PROFIT_FACTOR * abs(trigger_spread)  # 50% of entry spread
        max_allowed_diff = (MAX_SPREAD_DIFF_PCT_OF_TARGET / 100.0) * profit_target_pct  # 30% of profit target
        
        if spread_difference > max_allowed_diff:
            logger.warning(f"❌ Rejecting {sym}: Exit spread worse than entry by {spread_difference:.4f}% > {MAX_SPREAD_DIFF_PCT_OF_TARGET}% of profit target ({max_allowed_diff:.4f}%) | Entry={trigger_spread:.4f}% Exit={exit_spread:.4f}%")
            return None
    else:
        # Exit spread is better than entry spread - GOOD! No need to check, proceed with trade
        logger.info(f"✅ {sym}: Exit spread better than entry (instant profit!) | Entry={trigger_spread:.4f}% Exit={exit_spread:.4f}%")
    
    # CRITICAL: Fetch funding rates with retry and validation
    bin_fr_info = fetch_binance_funding(sym)
    kc_fr_info = fetch_kucoin_funding(ku_api_sym)
    
    # CRITICAL FIX: If either funding rate fetch failed, reject entry
    # (This includes cases where funding rate was 0.0000 - treated as glitch)
    if bin_fr_info is None:
        logger.warning(f"❌ Cannot evaluate {sym}: Binance funding rate fetch failed (None or 0.0000 glitch)")
        return None
    if kc_fr_info is None:
        logger.warning(f"❌ Cannot evaluate {sym}: KuCoin funding rate fetch failed (None or 0.0000 glitch)")
        return None
    
    bin_fr_pct = bin_fr_info.get('fundingRatePct')
    kc_fr_pct = kc_fr_info.get('fundingRatePct')
    
    # CRITICAL FIX: Double-check that funding rates are not None
    if bin_fr_pct is None or kc_fr_pct is None:
        logger.warning(f"❌ Cannot evaluate {sym}: Funding rates are None (bin={bin_fr_pct}, kc={kc_fr_pct})")
        return None
    
    # Log funding rates for visibility
    logger.info(f"📊 {sym} Funding Rates: Binance={bin_fr_pct:.6f}% KuCoin={kc_fr_pct:.6f}%")
    
    bin_next = bin_fr_info.get('nextFundingTimeMs') if bin_fr_info else None
    kc_next = kc_fr_info.get('nextFundingTimeMs') if kc_fr_info else None
    if plan == ('binance','kucoin'):
        net_fr = compute_net_funding_for_plan(bin_fr_pct, kc_fr_pct, 'binance','kucoin')
    else:
        net_fr = compute_net_funding_for_plan(bin_fr_pct, kc_fr_pct, 'kucoin','binance')
    
    # CRITICAL FIX: If net_fr is None (compute failed), reject entry
    if net_fr is None:
        logger.warning(f"❌ Cannot evaluate {sym}: Net funding calculation failed")
        return None
    
    # FIXED: Don't consider funding rate if difference is below threshold
    if abs(net_fr) < MIN_FR_DIFF_THRESHOLD:
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
    
    # Log entry evaluation for debugging
    logger.info(f"📊 {sym} Entry Eval: spread={trigger_spread:.4f}% net_fr={net_fr:.6f}% time_left={time_left_min}min plan={plan}")
    
    # Evaluate cases in order
    # Case1
    if abs(trigger_spread) >= CASE1_MIN_SPREAD and net_fr is not None and net_fr >= FR_ADVANTAGE_THRESHOLD:
        logger.info(f"✅ {sym} CASE1 triggered: spread={trigger_spread:.4f}% >= {CASE1_MIN_SPREAD}%, net_fr={net_fr:.6f}% >= {FR_ADVANTAGE_THRESHOLD}%")
        return {'case':'case1','plan':plan,'trigger_spread':trigger_spread,'net_fr':net_fr,'time_left_min':time_left_min,'bin_bid':bin_bid,'bin_ask':bin_ask,'kc_bid':kc_bid,'kc_ask':kc_ask,'ku_api_sym':ku_api_sym}
    # Case2
    if net_fr is not None and net_fr < 0 and time_left_min is not None and time_left_min < 30:
        net_dis = abs(net_fr)
        if abs(trigger_spread) >= CASE2_MULT * net_dis:
            logger.info(f"✅ {sym} CASE2 triggered: spread={trigger_spread:.4f}% >= {CASE2_MULT}*{net_dis:.6f}%, time_left={time_left_min:.1f}min < 30")
            return {'case':'case2','plan':plan,'trigger_spread':trigger_spread,'net_fr':net_fr,'time_left_min':time_left_min,'bin_bid':bin_bid,'bin_ask':bin_ask,'kc_bid':kc_bid,'kc_ask':kc_ask,'ku_api_sym':ku_api_sym}
    # Case3
    if net_fr is not None and net_fr < 0 and time_left_min is not None and time_left_min >= 30:
        net_dis = abs(net_fr)
        if abs(trigger_spread) >= CASE3_MULT * net_dis:
            logger.info(f"✅ {sym} CASE3 triggered: spread={trigger_spread:.4f}% >= {CASE3_MULT}*{net_dis:.6f}%, time_left={time_left_min:.1f}min >= 30")
            return {'case':'case3','plan':plan,'trigger_spread':trigger_spread,'net_fr':net_fr,'time_left_min':time_left_min,'bin_bid':bin_bid,'bin_ask':bin_ask,'kc_bid':kc_bid,'kc_ask':kc_ask,'ku_api_sym':ku_api_sym}
    
    logger.debug(f"⏸️  {sym} No case triggered (spread={trigger_spread:.4f}%, net_fr={net_fr:.6f}%)")
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
    
    # FIXED: Calculate executed spread - formula depends on the PLAN (which exchange is long/short)
    # Plan is stored in eval_info
    plan = eval_info.get('plan')  # ('binance', 'kucoin') or ('kucoin', 'binance')
    
    # For plan ('binance', 'kucoin'): long binance, short kucoin → NEGATIVE spread
    # Formula: (kc_price - bin_price) / bin_price → will be NEGATIVE when kc > bin
    # For plan ('kucoin', 'binance'): short binance, long kucoin → POSITIVE spread  
    # Formula: (bin_price - kc_price) / kc_price → will be POSITIVE when bin > kc
    
    if plan == ('binance', 'kucoin'):
        # Long binance, short kucoin → negative spread
        # Use (kc - bin) / bin
        exec_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin if exec_price_bin > 0 else trigger_spread
    else:  # plan == ('kucoin', 'binance')
        # Short binance, long kucoin → positive spread
        # Use (bin - kc) / kc
        exec_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc if exec_price_kc > 0 else trigger_spread
    
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
    active_trade['plan'] = plan  # FIXED: Store plan for later reference in averaging/TP
    active_trade['entry_timestamp_ms'] = int(time.time() * 1000)  # NEW: Store entry time for funding history
    active_trade['last_funding_check_ms'] = 0  # NEW: Track last time we checked funding history
    
    # FIXED: Initialize funding tracking sets and lists to prevent double counting
    active_trade['seen_binance_funding_timestamps'] = set()
    active_trade['seen_kucoin_funding_timestamps'] = set()
    active_trade['binance_funding_events'] = []
    active_trade['kucoin_funding_events'] = []
    
    # COMPLETELY CHANGED: Track accumulated expenses in USD directly (not percentage)
    # This makes comparison with profit target (also in USD) crystal clear
    total_notional = implied_bin_amt + implied_kc_amt
    active_trade['total_notional'] = total_notional
    
    # Calculate fees per exchange IN DOLLARS
    binance_entry_fees_dollars = (TRADING_FEE_PCT_PER_EXCHANGE / 100.0) * implied_bin_amt
    kucoin_entry_fees_dollars = (TRADING_FEE_PCT_PER_EXCHANGE / 100.0) * implied_kc_amt
    total_entry_fees_dollars = binance_entry_fees_dollars + kucoin_entry_fees_dollars
    
    # Store accumulated expenses in USD (not percentage!)
    active_trade['accumulated_expenses_usd'] = total_entry_fees_dollars
    
    logger.info(f"PURE USD TRACKING: Entry fees = ${total_entry_fees_dollars:.4f} (Binance: ${binance_entry_fees_dollars:.4f} + KuCoin: ${kucoin_entry_fees_dollars:.4f})")
    
    # NEW: Check if practical entry spread is less than 1.2% - if so, close immediately
    if abs(practical_entry_spread) < 1.2:
        logger.warning(f"❌ IMMEDIATE EXIT TRIGGERED: Practical entry spread {practical_entry_spread:.4f}% < 1.2% (no profit left due to slippage)")
        send_telegram(f"*IMMEDIATE EXIT* `{sym}`\n❌ Entry spread too low: `{practical_entry_spread:.4f}%` < 1.2%\nNo profit left after slippage. Closing immediately.\n{timestamp()}")
        # Close positions immediately
        try:
            close_all_and_wait()
            reset_active_trade()
        except Exception as e:
            logger.exception(f"Error closing positions after low spread detection: {e}")
        return False  # Return False to indicate entry failed
    
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
            # Use exec price with 25% buffer (simulating liquidation price) if no liq price available
            # This happens when position size is small relative to account balance (liq price would be >100% away)
            simulated_liq_pct = 0.25  # 25% away from execution price as simulated liquidation
            if side == 'long':
                # For long: simulated liq is 25% below exec, stop is 2% above that
                simulated_liq_price = exec_price_fallback * (1 - simulated_liq_pct)
                stop_price = simulated_liq_price * (1 + buffer_pct)
            elif side == 'short':
                # For short: simulated liq is 25% above exec, stop is 2% below that
                simulated_liq_price = exec_price_fallback * (1 + simulated_liq_pct)
                stop_price = simulated_liq_price * (1 - buffer_pct)
            logger.warning("⚠️  FALLBACK: Using simulated liq price (25%% from exec) for stop (no liquidation price available): %s side=%s exec_price=%s simulated_liq=%s stop_price=%s", 
                         exchange.id, side, exec_price_fallback, simulated_liq_price, stop_price)
        
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
    
    # FIXED: Calculate current spread using the PLAN direction (not entry spread sign)
    # For plan ('binance', 'kucoin'): long bin, short kc → use (kc_bid - bin_ask) / bin_ask
    # For plan ('kucoin', 'binance'): short bin, long kc → use (bin_bid - kc_ask) / kc_ask
    plan = active_trade.get('plan')
    if plan == ('binance', 'kucoin'):
        # Long binance, short kucoin
        if bin_ask <= 0:
            return
        current_spread = 100 * (kc_bid - bin_ask) / bin_ask
    else:
        # Short binance, long kucoin
        if kc_ask <= 0:
            return
        current_spread = 100 * (bin_bid - kc_ask) / kc_ask
    
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
        
        # FIXED: Use plan direction for order placement (not entry spread sign)
        if plan == ('binance', 'kucoin'):
            # Long binance, short kucoin
            notional_bin, notional_kc, _, _ = match_base_exposure_per_exchange(binance, kucoin, sym, kc_ccxt, NOTIONAL, bin_ask, kc_bid)
            results = {}
            def exec_kc(): results['kc'] = safe_create_order(kucoin, 'sell', notional_kc, kc_bid, kc_ccxt)
            def exec_bin(): results['bin'] = safe_create_order(binance, 'buy', notional_bin, bin_ask, sym)
        else:
            # Short binance, long kucoin
            notional_bin, notional_kc, _, _ = match_base_exposure_per_exchange(binance, kucoin, sym, kc_ccxt, NOTIONAL, bin_bid, kc_ask)
            results = {}
            def exec_kc(): results['kc'] = safe_create_order(kucoin, 'buy', notional_kc, kc_ask, kc_ccxt)
            def exec_bin(): results['bin'] = safe_create_order(binance, 'sell', notional_bin, bin_bid, sym)
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
                
                # FIXED: Calculate this averaging spread using PLAN direction (not entry spread sign)
                # For plan ('binance', 'kucoin'): long bin, short kc → use (kc - bin) / bin
                # For plan ('kucoin', 'binance'): short bin, long kc → use (bin - kc) / kc
                if plan == ('binance', 'kucoin'):
                    # Long binance, short kucoin
                    this_spread = 100 * (exec_price_kc - exec_price_bin) / exec_price_bin
                else:
                    # Short binance, long kucoin
                    this_spread = 100 * (exec_price_bin - exec_price_kc) / exec_price_kc
                prev_total = prev_bin + prev_kc
                new_total = new_implied_bin + new_implied_kc
                if prev_total + new_total > 0:
                    weighted_prev = prev_avg * prev_total
                    weighted_new = this_spread * new_total
                    new_avg = (weighted_prev + weighted_new) / (prev_total + new_total)
                else:
                    new_avg = this_spread
                active_trade['averages_done'] += 1
                active_trade['avg_count'] = active_trade['averages_done']  # NEW: Track averaging count
                active_trade['avg_entry_spread'] = new_avg
                active_trade['final_implied_notional']['bin'] = prev_bin + new_implied_bin
                active_trade['final_implied_notional']['kc'] = prev_kc + new_implied_kc
                # NEW: Track final averaged entry prices
                active_trade['final_averaged_price_bin'] = exec_price_bin
                active_trade['final_averaged_price_kc'] = exec_price_kc
                
                # PURE USD: Calculate additional fees in dollars
                new_total_notional = active_trade['final_implied_notional']['bin'] + active_trade['final_implied_notional']['kc']
                
                # Calculate fees on the NEW notional added IN DOLLARS
                binance_additional_fees_dollars = (TRADING_FEE_PCT_PER_EXCHANGE / 100.0) * new_implied_bin
                kucoin_additional_fees_dollars = (TRADING_FEE_PCT_PER_EXCHANGE / 100.0) * new_implied_kc
                total_additional_fees_dollars = binance_additional_fees_dollars + kucoin_additional_fees_dollars
                
                # Update total notional
                old_total_notional = active_trade['total_notional']
                active_trade['total_notional'] = new_total_notional
                
                # Add the additional fees to accumulated expenses (in USD)
                active_trade['accumulated_expenses_usd'] += total_additional_fees_dollars
                
                logger.info(f"AVERAGING UPDATE | New Notional: ${new_total_notional:.2f} (was ${old_total_notional:.2f}) | Additional Fees: ${total_additional_fees_dollars:.4f} (Bin: ${binance_additional_fees_dollars:.4f} + KC: ${kucoin_additional_fees_dollars:.4f}) | Total Expenses: ${active_trade['accumulated_expenses_usd']:.4f}")
                
                try_place_stops_after_entry(sym, kc_ccxt, exec_price_bin or None, exec_price_kc or None, None, None)
                send_telegram(f"*AVERAGE ADDED* `{sym}` → new avg spread `{new_avg:.4f}%` (averages_done={active_trade['averages_done']})\nBin: `{exec_price_bin:.6f}` | KC: `{exec_price_kc:.6f}`\n📊 New Notional: `${new_total_notional:.2f}`\n📊 Additional Fees: `${total_additional_fees_dollars:.4f}`\n💰 Total Expenses: `${active_trade['accumulated_expenses_usd']:.4f}`\n{timestamp()}")
            except Exception:
                logger.exception("Error aggregating averaging results")
        else:
            logger.info("Averaging partial/failed execution; will not count as average")

def check_take_profit_or_close_conditions():
    """
    FIXED: Now requires 3 consecutive confirmations before exiting (like entry)
    ADDED: Tracks exit trigger spread and real executed spread with slippage
    """
    global tp_confirm_count, tp_closing_in_progress, closing_reason
    
    sym = active_trade.get('symbol')
    if not sym:
        tp_confirm_count = 0
        return
    
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
            tp_confirm_count = 0
            return
        bin_bid, bin_ask = bin_price
        kc_bid, kc_ask = kc_price
        # Validate prices to prevent division by zero
        if bin_bid <= 0 or bin_ask <= 0 or kc_bid <= 0 or kc_ask <= 0:
            tp_confirm_count = 0
            return
    except Exception:
        tp_confirm_count = 0
        return
    
    # FIXED: Calculate current spread using the PLAN direction (not entry spread sign)
    # For plan ('binance', 'kucoin'): long bin, short kc → use (kc_bid - bin_ask) / bin_ask
    # For plan ('kucoin', 'binance'): short bin, long kc → use (bin_bid - kc_ask) / kc_ask
    plan = active_trade.get('plan')
    if plan == ('binance', 'kucoin'):
        # Long binance, short kucoin (negative spread)
        current_spread = 100 * (kc_bid - bin_ask) / bin_ask if bin_ask > 0 else 0.0
    else:
        # Short binance, long kucoin (positive spread)
        current_spread = 100 * (bin_bid - kc_ask) / kc_ask if kc_ask > 0 else 0.0
    
    logger.info("TP CHECK | sym=%s | entry_spread=%.4f%% current_spread=%.4f%%", 
                sym, avg_entry_spread, current_spread)
    
    # FIXED: Check for sign reversal (spread flipped direction = big profit)
    entry_was_positive = avg_entry_spread > 0
    current_is_positive = current_spread > 0
    
    should_exit = False
    exit_reason = None
    
    if entry_was_positive != current_is_positive:
        # Sign changed! This means spread reversed = captured at least full spread
        should_exit = True
        exit_reason = f"SPREAD SIGN REVERSAL"
        logger.info("SPREAD_SIGN_REVERSAL | %s | entry=%.4f%% current=%.4f%%", sym, avg_entry_spread, current_spread)
    else:
        # FIXED: Calculate convergence properly with signed values
        # For positive entry spread: profit when spread decreases (current < entry)
        # For negative entry spread: profit when spread increases toward zero (current > entry, both negative)
        if avg_entry_spread > 0:
            # Positive spread case: profit = entry_spread - current_spread
            spread_convergence = avg_entry_spread - current_spread
        else:
            # Negative spread case: profit = current_spread - entry_spread (both negative, so this is positive when converging)
            spread_convergence = current_spread - avg_entry_spread
        
        # PURE USD COMPARISON: Calculate profit target and compare with expenses
        # Both in USD - crystal clear comparison!
        
        # Get individual notionals
        bin_notional = active_trade.get('final_implied_notional', {}).get('bin', 0)
        kc_notional = active_trade.get('final_implied_notional', {}).get('kc', 0)
        total_notional = bin_notional + kc_notional
        
        # Profit is earned on SINGLE notional (use larger for conservative estimate)
        profit_base_notional = max(bin_notional, kc_notional)
        
        # Calculate target profit in USD
        target_convergence_pct = abs(avg_entry_spread) * TAKE_PROFIT_FACTOR  # 50% of entry spread
        target_profit_usd = (target_convergence_pct / 100.0) * profit_base_notional
        
        # Get accumulated expenses in USD (already tracked in pure USD)
        accumulated_expenses_usd = active_trade.get('accumulated_expenses_usd', 0.0)
        
        logger.info("TP CHECK (PURE USD) | entry=%.4f%% current=%.4f%% | Target profit: $%.4f (%.2f%% of $%.2f) | Expenses: $%.4f",
                   avg_entry_spread, current_spread, target_profit_usd, target_convergence_pct, profit_base_notional, accumulated_expenses_usd)
        
        if spread_convergence >= target_convergence_pct:
            should_exit = True
            exit_reason = f"TAKE PROFIT (convergence={spread_convergence:.4f}% >= target={target_convergence_pct:.4f}%)"
        else:
            # PURE USD COMPARISON: Compare expenses (USD) vs profit target (USD)
            if accumulated_expenses_usd > target_profit_usd:
                # CRITICAL: Check if spread is still > 1.5x entry (wide enough for averaging)
                # If spread > 1.5x entry, DON'T close - averaging might be triggered
                # Only close if spread has narrowed (< 1.5x entry) and expenses exceed target
                spread_ratio = abs(current_spread) / abs(avg_entry_spread) if avg_entry_spread != 0 else 0
                
                if spread_ratio > 1.5:
                    # Spread is still very wide (> 1.5x entry)
                    # Averaging might be triggered soon, so don't close yet
                    logger.info("Expenses exceed target ($%.4f > $%.4f) BUT spread is still wide (%.4f%% = %.2fx entry %.4f%%) - waiting for averaging or convergence", 
                               accumulated_expenses_usd, target_profit_usd, current_spread, spread_ratio, avg_entry_spread)
                else:
                    # Spread has narrowed (< 1.5x entry) and expenses exceed target
                    # Close position - no more averaging opportunity and expenses too high
                    should_exit = True
                    exit_reason = f"EXPENSE LIMIT (expenses=${accumulated_expenses_usd:.4f} > target=${target_profit_usd:.4f}, spread={current_spread:.4f}% = {spread_ratio:.2f}x entry)"
                    logger.warning("CLOSING DUE TO EXPENSES: expenses=$%.4f > target=$%.4f | current_spread=%.4f%% (%.2fx entry %.4f%%) - spread too narrow for averaging", 
                                 accumulated_expenses_usd, target_profit_usd, current_spread, spread_ratio, avg_entry_spread)
            else:
                # Also check funding accumulation as backup (in USD)
                funding_acc_usd = active_trade.get('funding_accumulated_usd', 0.0)
                
                # Only close if NET PAID funding exceeds target
                if funding_acc_usd > 0 and funding_acc_usd > target_profit_usd:
                    # Check spread ratio - only close if spread < 1.5x entry (no averaging opportunity)
                    spread_ratio = abs(current_spread) / abs(avg_entry_spread) if avg_entry_spread != 0 else 0
                    if spread_ratio <= 1.5:
                        should_exit = True
                        exit_reason = f"FUNDING LOSS (net paid=${funding_acc_usd:.4f} > target=${target_profit_usd:.4f}, spread={spread_ratio:.2f}x entry)"
    
    # NEW: 3-confirmation logic for exit
    if should_exit:
        tp_confirm_count += 1
        logger.info("TP CONFIRMATION %d/%d | %s | %s", tp_confirm_count, TP_CONFIRM_COUNT, sym, exit_reason)
        print(f"⏳ TP Confirmation {tp_confirm_count}/{TP_CONFIRM_COUNT} for {sym}: {exit_reason}", flush=True)
        
        if tp_confirm_count >= TP_CONFIRM_COUNT:
            logger.info("TP CONFIRMED %d times - EXECUTING EXIT for %s", TP_CONFIRM_COUNT, sym)
            print(f"✅ TP CONFIRMED {TP_CONFIRM_COUNT} times - EXITING {sym}", flush=True)
            
            # Store trigger spread before closing
            exit_trigger_spread = current_spread
            exit_trigger_time = timestamp()
            
            # ADDED: Capture pre-trade balance
            pre_total, pre_bin, pre_kc = get_total_futures_balance()
            net_funding_total = active_trade.get('funding_accumulated_pct', 0.0)
            total_notional = active_trade.get('total_notional', NOTIONAL)
            
            # NEW: Store pre-close balance in active_trade
            active_trade['balance_before_close'] = {
                'total': pre_total,
                'binance': pre_bin,
                'kucoin': pre_kc
            }
            
            # FIXED: Set closing reason to TP BEFORE closing positions
            closing_reason = 'TP'
            tp_closing_in_progress = True
            
            # NEW: Store exit trigger prices for slippage calculation
            try:
                bin_book = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
                for item in bin_book:
                    if item['symbol'] == sym:
                        active_trade['exit_trigger_bin_bid'] = float(item['bidPrice'])
                        active_trade['exit_trigger_bin_ask'] = float(item['askPrice'])
                        break
                kc_api = active_trade.get('ku_api') or sym + "M"
                kc_resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={kc_api}", timeout=5).json()
                d = kc_resp.get('data', {})
                active_trade['exit_trigger_kc_bid'] = float(d.get('bestBidPrice', '0') or 0)
                active_trade['exit_trigger_kc_ask'] = float(d.get('bestAskPrice', '0') or 0)
            except Exception as e:
                logger.warning(f"Error storing exit trigger prices: {e}")
            
            # Close positions and capture executed prices
            close_result = close_all_and_wait_with_tracking(sym)
            
            # Reset TP closing flag
            tp_closing_in_progress = False
            
            # ADDED: Capture post-trade balance
            post_total, post_bin, post_kc = get_total_futures_balance()
            
            # NEW: Store post-close balance in active_trade
            active_trade['balance_after_close'] = {
                'total': post_total,
                'binance': post_bin,
                'kucoin': post_kc
            }
            
            # NEW: Calculate balance change breakdown
            balance_change = post_total - pre_total
            
            # PURE USD: Get accumulated expenses directly in USD
            accumulated_expenses_usd = active_trade.get('accumulated_expenses_usd', 0.0)
            
            # Funding fees (convert % to dollars for backward compatibility display)
            net_funding_total = active_trade.get('funding_accumulated_usd', 0.0)
            
            if close_result:
                exit_exec_bin_price = close_result.get('bin_exec_price')
                exit_exec_kc_price = close_result.get('kc_exec_price')
                
                # Calculate actual exit spread from executed prices
                if exit_exec_bin_price and exit_exec_kc_price:
                    # FIXED: Use plan direction for exit spread calculation
                    if plan == ('binance', 'kucoin'):
                        # Long binance, short kucoin: when closing, sell bin at bid, buy kc at ask
                        # Exit spread: (kc_ask - bin_bid) / bin_bid
                        exit_exec_spread = 100 * (exit_exec_kc_price - exit_exec_bin_price) / exit_exec_bin_price
                    else:
                        # Short binance, long kucoin: when closing, buy bin at ask, sell kc at bid
                        # Exit spread: (bin_ask - kc_bid) / kc_bid
                        exit_exec_spread = 100 * (exit_exec_bin_price - exit_exec_kc_price) / exit_exec_kc_price
                    
                    # Calculate slippage (difference between trigger and executed spread)
                    exit_slippage_pct = exit_trigger_spread - exit_exec_spread
                    exit_slippage_dollars = (exit_slippage_pct / 100.0) * total_notional
                    
                    logger.info("EXIT SPREADS | trigger=%.4f%% executed=%.4f%% slippage=%.4f%%", 
                                exit_trigger_spread, exit_exec_spread, exit_slippage_pct)
                    
                    # PURE USD: Calculate net P&L breakdown
                    # accumulated_expenses_usd includes: entry trading fees + actual funding paid/received (all in USD)
                    
                    # Funding status (positive = we paid, negative = we received)
                    funding_status = "received" if net_funding_total < 0 else "paid"
                    
                    # Estimate gross spread profit
                    # balance_change = spread_profit - accumulated_expenses - exit_slippage
                    # Therefore: spread_profit = balance_change + accumulated_expenses + exit_slippage
                    gross_spread_profit = balance_change + accumulated_expenses_usd + exit_slippage_dollars
                    
                    # UPDATED: Enhanced telegram message with accumulated expenses
                    avg_count = active_trade.get('avg_count', 0)
                    averaging_info = ""
                    if avg_count > 0:
                        final_bin_price = active_trade.get('final_averaged_price_bin')
                        final_kc_price = active_trade.get('final_averaged_price_kc')
                        averaging_info = f"Averaging: `×{avg_count}`\n"
                        if final_bin_price:
                            averaging_info += f"Avg Bin: `{final_bin_price:.6f}`\n"
                        if final_kc_price:
                            averaging_info += f"Avg KC: `{final_kc_price:.6f}`\n"
                    
                    msg = (
                        f"*{exit_reason}* `{sym}`\n"
                        f"{averaging_info}"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📊 *SPREADS*\n"
                        f"Entry: `{avg_entry_spread:.4f}%`\n"
                        f"Exit Trigger: `{exit_trigger_spread:.4f}%`\n"
                        f"Exit Executed: `{exit_exec_spread:.4f}%`\n"
                        f"Exit Slippage: `{exit_slippage_pct:.4f}%` (`${exit_slippage_dollars:+.2f}`)\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"💰 *BALANCE CHANGE*\n"
                        f"Before: `${pre_total:.2f}`\n"
                        f"After: `${post_total:.2f}`\n"
                        f"Net Change: `${balance_change:+.2f}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📋 *P&L BREAKDOWN*\n"
                        f"Gross Profit: `${gross_spread_profit:+.2f}`\n"
                        f"Total Expenses: `-${accumulated_expenses_usd:.2f}`\n"
                        f"  → Entry Fees + Funding {funding_status}\n"
                        f"Exit Slippage: `-${exit_slippage_dollars:.2f}`\n"
                        f"Net P&L: `${balance_change:+.2f}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"{exit_trigger_time}"
                    )
                else:
                    # No executed prices available
                    funding_status = "received" if net_funding_total < 0 else "paid"
                    
                    avg_count = active_trade.get('avg_count', 0)
                    averaging_info = ""
                    if avg_count > 0:
                        final_bin_price = active_trade.get('final_averaged_price_bin')
                        final_kc_price = active_trade.get('final_averaged_price_kc')
                        averaging_info = f"Averaging: `×{avg_count}`\n"
                        if final_bin_price:
                            averaging_info += f"Avg Bin: `{final_bin_price:.6f}`\n"
                        if final_kc_price:
                            averaging_info += f"Avg KC: `{final_kc_price:.6f}`\n"
                    
                    msg = (
                        f"*{exit_reason}* `{sym}`\n"
                        f"{averaging_info}"
                        f"Entry: `{avg_entry_spread:.4f}%`\n"
                        f"Exit Trigger: `{exit_trigger_spread:.4f}%`\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"💰 *BALANCE CHANGE*\n"
                        f"Before: `${pre_total:.2f}`\n"
                        f"After: `${post_total:.2f}`\n"
                        f"Net Change: `${balance_change:+.2f}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"📋 *P&L BREAKDOWN*\n"
                        f"Total Expenses: `-${accumulated_expenses_usd:.2f}`\n"
                        f"  → Entry Fees + Funding {funding_status}\n"
                        f"Net P&L: `${balance_change:+.2f}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"{exit_trigger_time}"
                    )
            else:
                close_all_and_wait()
                # NEW: Add averaging information
                avg_count = active_trade.get('avg_count', 0)
                averaging_info = ""
                if avg_count > 0:
                    final_bin_price = active_trade.get('final_averaged_price_bin')
                    final_kc_price = active_trade.get('final_averaged_price_kc')
                    averaging_info = f"Averaging: `{avg_count}x`\n"
                    if final_bin_price:
                        averaging_info += f"Avg Entry Bin: `{final_bin_price:.6f}`\n"
                    if final_kc_price:
                        averaging_info += f"Avg Entry KC: `{final_kc_price:.6f}`\n"
                
                funding_status = "received" if net_funding_total < 0 else "paid"
                msg = (
                    f"*{exit_reason}* `{sym}`\n"
                    f"{averaging_info}"
                    f"Entry: `{avg_entry_spread:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"Total Expenses: `${accumulated_expenses_usd:.2f}`\n"
                    f"  → Entry Fees + Funding {funding_status}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"Pre-Balance: `${pre_total:.2f}`\n"
                    f"Post-Balance: `${post_total:.2f}`\n"
                    f"P&L: `${post_total - pre_total:.2f}`\n"
                    f"{exit_trigger_time}"
                )
            
            send_telegram(msg)
            reset_active_trade()
            tp_confirm_count = 0
    else:
        # Reset confirmation counter if condition not met
        if tp_confirm_count > 0:
            logger.info("TP condition no longer met, resetting counter (was %d)", tp_confirm_count)
        tp_confirm_count = 0

def reset_active_trade():
    global tp_confirm_count, closing_reason
    sym = active_trade.get('symbol')
    if sym:
        positions[sym] = None
    active_trade.update({
        'symbol': None, 'ku_api': None, 'ku_ccxt': None, 'case': None, 'entry_spread': None, 
        'avg_entry_spread': None, 'entry_prices': None, 'notional': None, 'averages_done': 0, 
        'final_implied_notional': None, 'funding_accumulated_pct': 0.0, 'funding_rounds_seen': set(), 
        'suppress_full_scan': False, 'plan': None, 'avg_count': 0, 'final_averaged_price_bin': None, 
        'final_averaged_price_kc': None, 'accumulated_expenses_pct': 0.0, 'total_notional': 0.0,
        'exit_trigger_spread': None, 'exit_real_spread': None, 'exit_trigger_bin_bid': None,
        'exit_trigger_bin_ask': None, 'exit_trigger_kc_bid': None, 'exit_trigger_kc_ask': None,
        'balance_before_close': None, 'balance_after_close': None,
        'entry_timestamp_ms': None, 'last_funding_check_ms': 0,
        'accumulated_expenses_usd': 0.0, 'funding_accumulated_usd': 0.0, 'funding_check_count': 0,
        # FIXED: Clear funding tracking sets and lists to prevent double counting
        'seen_binance_funding_timestamps': set(),
        'seen_kucoin_funding_timestamps': set(),
        'binance_funding_events': [],
        'kucoin_funding_events': []
    })
    tp_confirm_count = 0  # NEW: Reset TP confirmation counter
    closing_reason = None  # FIXED: Reset closing reason
    logger.info("Active trade reset")

def funding_round_accounting_loop():
    """
    COMPLETELY REWRITTEN: Fetches ACTUAL funding fees from exchange history.
    
    Every 30 seconds, it:
    1. Fetches actual funding fee history from both Binance and KuCoin
    2. Identifies NEW funding events (by timestamp deduplication)
    3. Calculates net fees paid/received in USD
    4. Updates accumulated_expenses_usd immediately
    
    - Funding PAID (positive net_fees_usd) increases accumulated expenses
    - Funding RECEIVED (negative net_fees_usd) decreases accumulated expenses
    
    FIXED: Now tracks individual funding events by timestamp to prevent double counting.
    """
    while True:
        try:
            sym = active_trade.get('symbol')
            if not sym:
                time.sleep(5)
                continue
            
            # Get entry timestamp and current time
            entry_timestamp_ms = active_trade.get('entry_timestamp_ms')
            if not entry_timestamp_ms:
                logger.warning("No entry timestamp found, cannot fetch funding history")
                time.sleep(5)
                continue
            
            # Check if it's time to update (every FUNDING_FEE_CHECK_INTERVAL seconds)
            current_time_ms = int(time.time() * 1000)
            last_check = active_trade.get('last_funding_check_ms', 0)
            
            if (current_time_ms - last_check) < (FUNDING_FEE_CHECK_INTERVAL * 1000):
                time.sleep(1)
                continue
            
            # Fetch actual funding fees from both exchanges (with timestamp deduplication)
            ku_api_sym = active_trade.get('ku_api') or sym + "M"
            funding_data = get_total_funding_fees_since_entry(sym, ku_api_sym, entry_timestamp_ms)
            
            if not funding_data:
                logger.warning("Failed to fetch funding data")
                time.sleep(FUNDING_FEE_CHECK_INTERVAL)
                continue
            
            # Extract net fees in USD (already in dollars, deduplicated by timestamp)
            net_fees_usd = funding_data['net_fees_usd']
            binance_fees_usd = funding_data['binance_fees_usd']
            kucoin_fees_usd = funding_data['kucoin_fees_usd']
            new_events_count = funding_data.get('new_events_count', 0)
            binance_events_count = funding_data.get('binance_events_count', 0)
            kucoin_events_count = funding_data.get('kucoin_events_count', 0)
            
            # PURE USD: Update accumulated expenses directly in dollars
            # Get current entry fees (stored in USD)
            bin_notional = active_trade.get('final_implied_notional', {}).get('bin', 0)
            kc_notional = active_trade.get('final_implied_notional', {}).get('kc', 0)
            
            binance_entry_fees = (TRADING_FEE_PCT_PER_EXCHANGE / 100.0) * bin_notional
            kucoin_entry_fees = (TRADING_FEE_PCT_PER_EXCHANGE / 100.0) * kc_notional
            total_entry_fees_usd = binance_entry_fees + kucoin_entry_fees
            
            # New accumulated expenses = entry fees + cumulative funding (all deduplicated)
            new_accumulated_expenses_usd = total_entry_fees_usd + net_fees_usd
            
            # Calculate change since last check
            old_expenses = active_trade.get('accumulated_expenses_usd', total_entry_fees_usd)
            expense_change = new_accumulated_expenses_usd - old_expenses
            
            # Update active trade
            active_trade['accumulated_expenses_usd'] = new_accumulated_expenses_usd
            active_trade['funding_accumulated_usd'] = net_fees_usd  # Store funding portion separately
            active_trade['last_funding_check_ms'] = current_time_ms
            
            # Determine status
            if net_fees_usd > 0:
                funding_status = "PAID"
            elif net_fees_usd < 0:
                funding_status = "RECEIVED"
            else:
                funding_status = "NEUTRAL"
            
            # Log the update
            logger.info(f"FUNDING UPDATE | {sym} | "
                       f"Binance: ${binance_fees_usd:+.4f} ({binance_events_count} events) | "
                       f"KuCoin: ${kucoin_fees_usd:+.4f} ({kucoin_events_count} events) | "
                       f"Net: ${net_fees_usd:+.4f} ({funding_status}) | "
                       f"New events this check: {new_events_count} | "
                       f"Total Expenses: ${new_accumulated_expenses_usd:.4f}")
            
            # Send Telegram if:
            # 1. There are NEW events (new_events_count > 0), OR
            # 2. There's a meaningful change (> $0.01), OR
            # 3. First update (old_expenses == total_entry_fees_usd), OR
            # 4. Every 10 checks even if no change (to show bot is alive)
            check_count = active_trade.get('funding_check_count', 0) + 1
            active_trade['funding_check_count'] = check_count
            
            should_send = (
                new_events_count > 0 or  # NEW: Send if we detected new funding events
                abs(expense_change) > 0.01 or 
                old_expenses == total_entry_fees_usd or
                check_count % 10 == 0  # Every 10 checks (5 minutes)
            )
            
            if should_send:
                logger.info(f"📤 Sending funding update to Telegram (check #{check_count}, new events={new_events_count}, change=${expense_change:.4f})")
                
                # Build event details if there are new events
                event_details = ""
                if new_events_count > 0:
                    event_details = f"\n🆕 NEW: {new_events_count} funding event(s) detected"
                
                send_telegram(
                    f"*FUNDING UPDATE* `{sym}`\n"
                    f"Binance: `${binance_fees_usd:+.4f}` ({binance_events_count} events)\n"
                    f"KuCoin: `${kucoin_fees_usd:+.4f}` ({kucoin_events_count} events)\n"
                    f"Net Funding: `${net_fees_usd:+.4f}` ({funding_status}){event_details}\n"
                    f"💰 Total Expenses: `${new_accumulated_expenses_usd:.4f}`\n"
                    f"  → Entry Fees + Funding ({funding_status})\n"
                    f"{timestamp()}"
                )
            else:
                logger.debug(f"⏭️  Skipping Telegram update (check #{check_count}, no new events or significant change)")
            
            # Sleep for the configured interval
            time.sleep(FUNDING_FEE_CHECK_INTERVAL)
            
        except Exception as e:
            logger.exception(f"Funding accounting loop fatal error: {e}")
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
                funding_acc = active_trade.get('funding_accumulated_usd', 0.0)  # Now in USD
                
                # PURE USD: Get accumulated expenses in dollars
                accumulated_expenses_usd = active_trade.get('accumulated_expenses_usd', 0.0)
                
                # Get current spread (live)
                try:
                    bin_book = requests.get("https://fapi.binance.com/fapi/v1/ticker/bookTicker", timeout=5).json()
                    bin_prices = None
                    for item in bin_book:
                        if item['symbol'] == sym:
                            bin_prices = (float(item['bidPrice']), float(item['askPrice']))
                            break
                    kc_api = active_trade.get('ku_api') or sym + "M"
                    kc_resp = requests.get(f"https://api-futures.kucoin.com/api/v1/ticker?symbol={kc_api}", timeout=5).json()
                    d = kc_resp.get('data', {}) if isinstance(kc_resp, dict) else {}
                    kc_prices = (float(d.get('bestBidPrice') or 0), float(d.get('bestAskPrice') or 0))
                    
                    if bin_prices and kc_prices:
                        bin_bid, bin_ask = bin_prices
                        kc_bid, kc_ask = kc_prices
                        plan = active_trade.get('plan')
                        
                        # Calculate entry spread (what we'd get if entering now)
                        if plan == ('binance', 'kucoin'):
                            entry_spread_live = 100 * (kc_bid - bin_ask) / bin_ask if bin_ask > 0 else 0.0
                            # For exit, reverse positions: close long bin (sell at bid), close short kc (buy at ask)
                            exit_spread_live = 100 * (kc_ask - bin_bid) / bin_bid if bin_bid > 0 else 0.0
                        else:
                            entry_spread_live = 100 * (bin_bid - kc_ask) / kc_ask if kc_ask > 0 else 0.0
                            # For exit, reverse positions: close short bin (buy at ask), close long kc (sell at bid)
                            exit_spread_live = 100 * (bin_ask - kc_bid) / kc_bid if kc_bid > 0 else 0.0
                    else:
                        entry_spread_live = 0.0
                        exit_spread_live = 0.0
                except Exception as e:
                    logger.warning(f"Error fetching live spreads: {e}")
                    entry_spread_live = 0.0
                    exit_spread_live = 0.0
                
                summary = (
                    f"📊 *BOT STATUS SUMMARY*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🟢 *ACTIVE TRADE* `{sym}`\n"
                    f"Case: `{case}` | Averages: `×{avg_count}`\n"
                    f"Entry Spread: `{avg_spread:.4f}%`\n"
                    f"Live Entry Spread: `{entry_spread_live:.4f}%`\n"
                    f"Live Exit Spread: `{exit_spread_live:.4f}%`\n"
                    f"Notional: `${total_not:.2f}`\n"
                    f"Funding: `${funding_acc:+.4f}`\n"
                    f"💰 Total Expenses: `${accumulated_expenses_usd:.4f}`\n"
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
                # No active trade - show entry/exit spreads for max +ve and max -ve
                with candidates_shared_lock:
                    num_candidates = len(candidates_shared)
                
                # Calculate entry and exit spreads for max positive
                max_pos_entry_spread = 0.0
                max_pos_exit_spread = 0.0
                if best_pos:
                    pos_sym, pos_info = best_pos
                    bin_bid = pos_info.get('bin_bid', 0)
                    bin_ask = pos_info.get('bin_ask', 0)
                    kc_bid = pos_info.get('ku_bid', 0)
                    kc_ask = pos_info.get('ku_ask', 0)
                    if bin_bid > 0 and bin_ask > 0 and kc_bid > 0 and kc_ask > 0:
                        # Determine plan based on spread direction
                        if kc_bid > bin_ask:
                            # Long binance, short kucoin
                            max_pos_entry_spread = 100 * (kc_bid - bin_ask) / bin_ask
                            max_pos_exit_spread = 100 * (kc_ask - bin_bid) / bin_bid
                        else:
                            # Short binance, long kucoin
                            max_pos_entry_spread = 100 * (bin_bid - kc_ask) / kc_ask
                            max_pos_exit_spread = 100 * (bin_ask - kc_bid) / kc_bid
                
                # Calculate entry and exit spreads for max negative
                max_neg_entry_spread = 0.0
                max_neg_exit_spread = 0.0
                if best_neg:
                    neg_sym, neg_info = best_neg
                    bin_bid = neg_info.get('bin_bid', 0)
                    bin_ask = neg_info.get('bin_ask', 0)
                    kc_bid = neg_info.get('ku_bid', 0)
                    kc_ask = neg_info.get('ku_ask', 0)
                    if bin_bid > 0 and bin_ask > 0 and kc_bid > 0 and kc_ask > 0:
                        # Determine plan based on spread direction
                        if kc_bid > bin_ask:
                            # Long binance, short kucoin
                            max_neg_entry_spread = 100 * (kc_bid - bin_ask) / bin_ask
                            max_neg_exit_spread = 100 * (kc_ask - bin_bid) / bin_bid
                        else:
                            # Short binance, long kucoin
                            max_neg_entry_spread = 100 * (bin_bid - kc_ask) / kc_ask
                            max_neg_exit_spread = 100 * (bin_ask - kc_bid) / kc_bid
                
                summary = (
                    f"📊 *BOT STATUS SUMMARY*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔍 *SCANNING MODE*\n"
                    f"Active Trade: `None`\n"
                    f"Candidates: `{num_candidates}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📈 *MAX POSITIVE SPREAD*\n"
                    f"`{max_pos_sym}`: `{max_pos_spread:.4f}%`\n"
                    f"Entry Spread: `{max_pos_entry_spread:.4f}%`\n"
                    f"Exit Spread: `{max_pos_exit_spread:.4f}%`\n"
                    f"FR → Bin: `{max_pos_fr_bin:.4f}%` | KC: `{max_pos_fr_kc:.4f}%`\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📉 *MAX NEGATIVE SPREAD*\n"
                    f"`{max_neg_sym}`: `{max_neg_spread:.4f}%`\n"
                    f"Entry Spread: `{max_neg_entry_spread:.4f}%`\n"
                    f"Exit Spread: `{max_neg_exit_spread:.4f}%`\n"
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
                    # CHANGED: Now works for ANY direction (positive or negative spread >= 7.5%)
                    # Keeps the original 4.0% entry restriction as requested
                    send_telegram(f"*TAKEOVER SIGNAL* `{best_sym}` spread `{best_spread:.4f}%` (any direction)\nCurrent trade: `{active_trade['symbol']}` entry `{active_trade.get('avg_entry_spread', 0.0):.4f}%`\nClosing current and entering new with 2x notional\n{timestamp()}")
                    close_all_and_wait()
                    reset_active_trade()
                    eval_info = {'plan': ('binance','kucoin') if best_info['ku_bid'] > best_info['bin_ask'] else ('kucoin','binance'),'ku_api_sym': best_info['ku_sym'],'bin_bid':best_info['bin_bid'],'bin_ask':best_info['bin_ask'],'kc_bid':best_info['ku_bid'],'kc_ask':best_info['ku_ask'],'trigger_spread':best_spread,'net_fr':0.0}
                    execute_pair_trade_with_snapshots(best_sym, eval_info, initial_multiplier=BIG_SPREAD_ENTRY_MULTIPLIER)
            time.sleep(2)  # FIXED: Reduced from 5s to 2s for faster TP confirmations
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
        
        # CRITICAL FIX: Skip all trading logic if override execution is in progress
        if override_execution_in_progress:
            logger.debug("⏸️ Override execution in progress, skipping normal trading logic")
            time.sleep(0.5)
            continue
        
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
                        logger.info("🚨 BIG SPREAD DETECTED (no active trade): %s spread=%.4f%% >= %.1f%%", sym, max_spread, BIG_SPREAD_THRESHOLD)
                        print(f"🚨 BIG SPREAD DETECTED: {sym} @ {max_spread:.4f}% (threshold: {BIG_SPREAD_THRESHOLD}%)", flush=True)
                        break
            
            # If big spread found, reconfirm with entry spread before entering
            if big_spread_candidate and big_spread_info:
                logger.info("🔍 BIG SPREAD ENTRY (no active trade): %s (main spread=%.4f%%, threshold=%.1f%%)", 
                           big_spread_candidate, big_spread_info.get('max_spread', 0.0), BIG_SPREAD_THRESHOLD)
                print(f"🔍 Reconfirming big spread entry for {big_spread_candidate}...", flush=True)
                
                # Get initial prices
                sym = big_spread_candidate
                ku_api_sym = big_spread_info.get('ku_sym')
                bin_bid = big_spread_info.get('bin_bid')
                bin_ask = big_spread_info.get('bin_ask')
                kc_bid = big_spread_info.get('ku_bid')
                kc_ask = big_spread_info.get('ku_ask')
                
                # Determine expected plan from main spread
                if kc_bid > bin_ask:
                    expected_plan = ('binance', 'kucoin')
                else:
                    expected_plan = ('kucoin', 'binance')
                
                # CRITICAL FIX: Reconfirm with fresh entry spread
                success, entry_spread, plan, fresh_bin_bid, fresh_bin_ask, fresh_kc_bid, fresh_kc_ask = reconfirm_entry_spread(
                    sym, ku_api_sym, bin_ask, bin_bid, kc_ask, kc_bid, expected_plan
                )
                
                if not success:
                    logger.warning(f"❌ Big spread entry REJECTED for {sym}: Reconfirmation failed")
                    print(f"❌ Big spread reconfirmation failed for {sym}", flush=True)
                    continue
                
                # Check if entry spread still meets threshold
                if abs(entry_spread) < BIG_SPREAD_THRESHOLD:
                    logger.warning(f"❌ Big spread entry REJECTED for {sym}: Entry spread {entry_spread:.4f}% < {BIG_SPREAD_THRESHOLD}%")
                    print(f"❌ Big spread entry rejected: {sym} @ {entry_spread:.4f}% < {BIG_SPREAD_THRESHOLD}%", flush=True)
                    continue
                
                logger.info(f"✅ Big spread entry CONFIRMED for {sym}: Entry spread {entry_spread:.4f}% >= {BIG_SPREAD_THRESHOLD}%")
                print(f"✅ BIG SPREAD ENTRY CONFIRMED: {sym} @ {entry_spread:.4f}%", flush=True)
                send_telegram(f"*BIG SPREAD ENTRY* `{sym}`\nMain spread: `{big_spread_info.get('max_spread', 0.0):.4f}%`\nEntry spread: `{entry_spread:.4f}%` >= {BIG_SPREAD_THRESHOLD}%\nEntering with 2x notional\n{timestamp()}")
                
                eval_info = {
                    'case': 'big_spread',
                    'plan': plan,
                    'trigger_spread': entry_spread,
                    'net_fr': 0.0,
                    'time_left_min': None,
                    'bin_bid': fresh_bin_bid,
                    'bin_ask': fresh_bin_ask,
                    'kc_bid': fresh_kc_bid,
                    'kc_ask': fresh_kc_ask,
                    'ku_api_sym': ku_api_sym
                }
                
                logger.info(f"🚀 Executing big spread entry for {sym} with 2x notional")
                print(f"🚀 Executing big spread entry: {sym}", flush=True)
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
            # Active trade present
            # NEW: Check for 7.5%+ big spread override - close current trade and enter new one
            # Only if current practical entry spread < 4%
            current_practical_entry = active_trade.get('avg_entry_spread', 0.0)
            
            if abs(current_practical_entry) < 4.0:
                # Check for big spread candidate
                big_spread_candidate = None
                big_spread_info = None
                
                with candidates_shared_lock:
                    for sym, info in candidates_shared.items():
                        # Skip current symbol
                        if sym == active_trade.get('symbol'):
                            continue
                        max_spread = info.get('max_spread', info.get('start_spread', 0.0))
                        if abs(max_spread) >= BIG_SPREAD_THRESHOLD:
                            big_spread_candidate = sym
                            big_spread_info = info
                            logger.info("🚨 BIG SPREAD OVERRIDE DETECTED: %s spread=%.4f%% >= %.1f%% (current trade %s entry=%.4f%%)", 
                                      sym, max_spread, BIG_SPREAD_THRESHOLD, active_trade.get('symbol'), current_practical_entry)
                            print(f"🚨 OVERRIDE DETECTED: {sym} @ {max_spread:.4f}% (current: {active_trade.get('symbol')} @ {current_practical_entry:.4f}%)", flush=True)
                            break
                
                if big_spread_candidate and big_spread_info:
                    # Increment override confirmation counter
                    override_confirm_count += 1
                    logger.info(f"⏳ Confirming override for {big_spread_candidate}: {override_confirm_count}/{OVERRIDE_CONFIRM_COUNT} (main_spread={big_spread_info.get('max_spread', 0.0):.4f}%)")
                    print(f"⏳ Override confirmation {override_confirm_count}/{OVERRIDE_CONFIRM_COUNT} for {big_spread_candidate} @ {big_spread_info.get('max_spread', 0.0):.4f}%", flush=True)
                    
                    if override_confirm_count >= OVERRIDE_CONFIRM_COUNT:
                        logger.info(f"✅ OVERRIDE CONFIRMATION COMPLETE for {big_spread_candidate}! Re-evaluating before execution...")
                        print(f"✅ Override confirmed for {big_spread_candidate}, executing...", flush=True)
                        
                        # Get initial prices
                        sym = big_spread_candidate
                        ku_api_sym = big_spread_info.get('ku_sym')
                        bin_bid = big_spread_info.get('bin_bid')
                        bin_ask = big_spread_info.get('bin_ask')
                        kc_bid = big_spread_info.get('ku_bid')
                        kc_ask = big_spread_info.get('ku_ask')
                        
                        # Determine expected plan from main spread
                        if kc_bid > bin_ask:
                            expected_plan = ('binance', 'kucoin')
                        else:
                            expected_plan = ('kucoin', 'binance')
                        
                        # CRITICAL FIX: Reconfirm with fresh entry spread
                        success, entry_spread, plan, fresh_bin_bid, fresh_bin_ask, fresh_kc_bid, fresh_kc_ask = reconfirm_entry_spread(
                            sym, ku_api_sym, bin_ask, bin_bid, kc_ask, kc_bid, expected_plan
                        )
                        
                        if not success:
                            logger.warning(f"❌ Big spread override REJECTED for {sym}: Reconfirmation failed")
                            print(f"❌ Override reconfirmation failed for {sym}, resetting confirms", flush=True)
                            override_confirm_count = 0
                            pass  # Continue with active trade monitoring
                        elif abs(entry_spread) < BIG_SPREAD_THRESHOLD:
                            logger.warning(f"❌ Big spread override REJECTED for {sym}: Entry spread {entry_spread:.4f}% < {BIG_SPREAD_THRESHOLD}%")
                            print(f"❌ Override rejected: {sym} @ {entry_spread:.4f}% < {BIG_SPREAD_THRESHOLD}%, resetting confirms", flush=True)
                            override_confirm_count = 0
                            pass  # Continue with active trade monitoring
                        else:
                            logger.info(f"✅ Big spread override CONFIRMED for {sym}: Entry spread {entry_spread:.4f}% >= {BIG_SPREAD_THRESHOLD}%")
                            print(f"✅ OVERRIDE CONFIRMED: {sym} @ {entry_spread:.4f}%", flush=True)
                            logger.info(f"🔄 OVERRIDE EXECUTION STARTING: Closing {active_trade.get('symbol')} to enter {sym}")
                            print(f"🔄 Closing {active_trade.get('symbol')} to enter {sym}...", flush=True)
                            send_telegram(f"*BIG SPREAD OVERRIDE*\nClosing `{active_trade.get('symbol')}` (entry={current_practical_entry:.4f}%)\nEntering `{sym}` (main={big_spread_info.get('max_spread', 0.0):.4f}%, entry={entry_spread:.4f}%)\n{timestamp()}")
                            
                            # CRITICAL FIX: Set flags to prevent race conditions and inform liquidation watcher
                            closing_reason = 'OVERRIDE'
                            override_execution_in_progress = True
                            override_confirm_count = 0  # Reset counter before execution
                            logger.info(f"🔒 OVERRIDE FLAGS SET: closing_reason='OVERRIDE', override_execution_in_progress=True")
                            
                            # Close current trade
                            try:
                                logger.info(f"🔄 Closing current position: {active_trade.get('symbol')}")
                                close_all_and_wait()
                                reset_active_trade()
                                logger.info(f"✅ Current position closed successfully")
                            except Exception as e:
                                logger.exception("❌ Error closing current trade for big spread override: %s", e)
                                # Reset flags even on error
                                closing_reason = None
                                override_execution_in_progress = False
                                logger.info(f"🔓 OVERRIDE FLAGS RESET (after error)")
                                continue  # Skip to next iteration
                            finally:
                                # Always reset closing_reason after close completes
                                closing_reason = None
                                logger.info(f"🔓 closing_reason reset to None (close complete)")
                            
                            # CRITICAL: Re-check spread after close to ensure it hasn't decayed
                            logger.info(f"🔍 RE-CHECKING SPREAD after position close for {sym}...")
                            print(f"🔍 Re-checking spread for {sym} after close...", flush=True)
                            
                            success_postclose, entry_spread_postclose, plan_postclose, fresh_bin_bid2, fresh_bin_ask2, fresh_kc_bid2, fresh_kc_ask2 = reconfirm_entry_spread(
                                sym, ku_api_sym, fresh_bin_ask, fresh_bin_bid, fresh_kc_ask, fresh_kc_bid, expected_plan
                            )
                            
                            if not success_postclose:
                                logger.warning(f"❌ POST-CLOSE spread check FAILED for {sym}: Reconfirmation failed after close")
                                print(f"❌ Spread check failed after close for {sym}, aborting entry", flush=True)
                                send_telegram(f"⚠️ *OVERRIDE ABORTED*\n`{sym}` spread check failed after closing position\nBot continues scanning\n{timestamp()}")
                                override_execution_in_progress = False
                                logger.info(f"🔓 OVERRIDE FLAGS RESET (post-close check failed)")
                                continue
                            elif abs(entry_spread_postclose) < BIG_SPREAD_THRESHOLD:
                                logger.warning(f"❌ POST-CLOSE spread check FAILED for {sym}: Entry spread {entry_spread_postclose:.4f}% < {BIG_SPREAD_THRESHOLD}% (SPREAD DECAY)")
                                print(f"❌ Spread decayed after close: {sym} @ {entry_spread_postclose:.4f}% < {BIG_SPREAD_THRESHOLD}%, aborting entry", flush=True)
                                send_telegram(f"⚠️ *OVERRIDE ABORTED - SPREAD DECAY*\n`{sym}` spread fell to {entry_spread_postclose:.4f}% < {BIG_SPREAD_THRESHOLD}%\nBot continues scanning\n{timestamp()}")
                                override_execution_in_progress = False
                                logger.info(f"🔓 OVERRIDE FLAGS RESET (spread decay)")
                                continue
                            else:
                                logger.info(f"✅ POST-CLOSE spread check PASSED for {sym}: Entry spread {entry_spread_postclose:.4f}% >= {BIG_SPREAD_THRESHOLD}%")
                                print(f"✅ Spread still valid: {sym} @ {entry_spread_postclose:.4f}%", flush=True)
                            
                            # Check if bot is terminating before attempting entry
                            if terminate_bot:
                                logger.warning(f"⚠️ Bot terminating, skipping override entry for {sym}")
                                override_execution_in_progress = False
                                logger.info(f"🔓 OVERRIDE FLAGS RESET (bot terminating)")
                                break
                            
                            eval_info = {
                                'case': 'big_spread_override',
                                'plan': plan_postclose,
                                'trigger_spread': entry_spread_postclose,
                                'net_fr': 0.0,
                                'time_left_min': None,
                                'bin_bid': fresh_bin_bid2,
                                'bin_ask': fresh_bin_ask2,
                                'kc_bid': fresh_kc_bid2,
                                'kc_ask': fresh_kc_ask2,
                                'ku_api_sym': ku_api_sym
                            }
                            
                            logger.info(f"🚀 Attempting override entry for {sym} with 2x notional")
                            ok = execute_pair_trade_with_snapshots(sym, eval_info, initial_multiplier=BIG_SPREAD_ENTRY_MULTIPLIER)
                            
                            # Reset override flag after execution completes
                            override_execution_in_progress = False
                            logger.info(f"🔓 override_execution_in_progress reset to False (execution complete)")
                            
                            if ok:
                                logger.info(f"✅ Override entry SUCCESS for {sym}")
                            else:
                                logger.error(f"❌ Override entry FAILED for {sym}")
                            
                            time.sleep(1)
                            continue
                else:
                    # No big spread candidate detected, reset override confirms
                    if override_confirm_count > 0:
                        logger.debug(f"🔄 Resetting override confirms (no candidate)")
                        override_confirm_count = 0
            
            # Otherwise: maintenance threads handle averaging/TP; main loop sleeps
            pass
        time.sleep(1)
except KeyboardInterrupt:
    try:
        close_all_and_wait()
    except Exception:
        pass
except Exception:
    logger.exception("Unhandled exception at top level")
