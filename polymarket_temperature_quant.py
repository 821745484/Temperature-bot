import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any, Optional

import requests

USE_CLOB_V2 = False
try:
    from py_clob_client_v2 import ClobClient, OrderArgs, OrderType, PartialCreateOrderOptions, Side
    USE_CLOB_V2 = True
except ImportError:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL
        PartialCreateOrderOptions = None
        Side = None
    except ImportError:
        ClobClient = None
        OrderArgs = None
        OrderType = None
        PartialCreateOrderOptions = None
        Side = None
        BUY = "BUY"
        SELL = "SELL"

try:
    from eth_account import Account
except ImportError:
    Account = None


BASE_DIR = Path(__file__).resolve().parent


def load_dotenv(dotenv_path: str | None = None) -> None:
    if dotenv_path is None:
        dotenv_path = str(BASE_DIR / ".env")
    env_path = Path(dotenv_path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ["'", '"']:
            value = value[1:-1]
        os.environ.setdefault(key, value)


load_dotenv()

GAMMA_EVENTS_URL = os.getenv("POLY_GAMMA_EVENTS_URL", "https://gamma-api.polymarket.com/events")
HIGH_TEMP_PAGE_URL = os.getenv("POLY_HIGH_TEMP_PAGE_URL", "https://polymarket.com/weather/high-temperature")
CLOB_URL = os.getenv("POLY_CLOB_URL", "https://clob.polymarket.com")
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
LOG_PATH = Path(os.getenv("POLY_LOG_CSV", str(BASE_DIR / "csv" / "polymarket_temperature_signals.csv")))
ORDER_STATE_PATH = Path(os.getenv("POLY_ORDER_STATE_JSON", str(BASE_DIR / "csv" / "polymarket_temperature_order_state.json")))
VERBOSE = os.getenv("POLY_VERBOSE", "false").lower() == "true"


@dataclass
class Config:
    scan_limit: int = int(os.getenv("POLY_SCAN_LIMIT", "200"))
    max_pages: int = int(os.getenv("POLY_MAX_PAGES", "30"))
    sleep_seconds: int = int(os.getenv("POLY_SLEEP_SECONDS", "300"))
    run_once: bool = os.getenv("POLY_RUN_ONCE", "true").lower() == "true"
    only_today: bool = os.getenv("POLY_ONLY_TODAY", "true").lower() == "true"
    stop_new_orders_on_take_profit: bool = os.getenv("POLY_STOP_NEW_ORDERS_ON_TAKE_PROFIT", "true").lower() == "true"
    temp_band_c: Decimal = Decimal(os.getenv("POLY_TEMP_BAND_C", "2"))
    min_edge: Decimal = Decimal(os.getenv("POLY_MIN_EDGE", "0.08"))
    min_ev: Decimal = Decimal(os.getenv("POLY_MIN_EV", "0.03"))
    min_score: Decimal = Decimal(os.getenv("POLY_MIN_SCORE", "0.04"))
    allow_side: str = os.getenv("POLY_ALLOW_SIDE", "AUTO").upper()
    min_price: Decimal = Decimal(os.getenv("POLY_MIN_PRICE", "0.05"))
    yes_min_edge: Decimal = Decimal(os.getenv("POLY_YES_MIN_EDGE", os.getenv("POLY_MIN_EDGE", "0.08")))
    yes_min_ev: Decimal = Decimal(os.getenv("POLY_YES_MIN_EV", os.getenv("POLY_MIN_EV", "0.03")))
    yes_min_score: Decimal = Decimal(os.getenv("POLY_YES_MIN_SCORE", os.getenv("POLY_MIN_SCORE", "0.04")))
    yes_exact_extra_edge: Decimal = Decimal(os.getenv("POLY_YES_EXACT_EXTRA_EDGE", "0.12"))
    yes_exact_extra_ev: Decimal = Decimal(os.getenv("POLY_YES_EXACT_EXTRA_EV", "0.10"))
    yes_exact_min_confidence: Decimal = Decimal(os.getenv("POLY_YES_EXACT_MIN_CONFIDENCE", "0.68"))
    yes_exact_min_price: Decimal = Decimal(os.getenv("POLY_YES_EXACT_MIN_PRICE", "0.10"))
    yes_exact_max_days_ahead: int = int(os.getenv("POLY_YES_EXACT_MAX_DAYS_AHEAD", "1"))
    yes_exact_max_forecast_distance: Decimal = Decimal(os.getenv("POLY_YES_EXACT_MAX_FORECAST_DISTANCE", "0.80"))
    yes_exact_max_history_gap: Decimal = Decimal(os.getenv("POLY_YES_EXACT_MAX_HISTORY_GAP", "0.18"))
    yes_exact_min_history_prob: Decimal = Decimal(os.getenv("POLY_YES_EXACT_MIN_HISTORY_PROB", "0.12"))
    yes_above_min_history_prob: Decimal = Decimal(os.getenv("POLY_YES_ABOVE_MIN_HISTORY_PROB", "0.22"))
    yes_exact_max_history_mean_distance: Decimal = Decimal(os.getenv("POLY_YES_EXACT_MAX_HISTORY_MEAN_DISTANCE", "1.20"))
    yes_city_blacklist: str = os.getenv("POLY_YES_CITY_BLACKLIST", "")
    yes_early_max_price: Decimal = Decimal(os.getenv("POLY_YES_EARLY_MAX_PRICE", "0.10"))
    yes_early_size_multiplier: Decimal = Decimal(os.getenv("POLY_YES_EARLY_SIZE_MULTIPLIER", "0.50"))
    yes_intraday_enabled: bool = os.getenv("POLY_YES_INTRADAY_ENABLED", "true").lower() == "true"
    yes_intraday_confirm_above_price: Decimal = Decimal(os.getenv("POLY_YES_INTRADAY_CONFIRM_ABOVE_PRICE", "0.10"))
    yes_intraday_confirm_distance: Decimal = Decimal(os.getenv("POLY_YES_INTRADAY_CONFIRM_DISTANCE", "0.80"))
    yes_intraday_max_days_ahead: int = int(os.getenv("POLY_YES_INTRADAY_MAX_DAYS_AHEAD", "1"))
    no_min_edge: Decimal = Decimal(os.getenv("POLY_NO_MIN_EDGE", os.getenv("POLY_MIN_EDGE", "0.08")))
    no_min_ev: Decimal = Decimal(os.getenv("POLY_NO_MIN_EV", os.getenv("POLY_MIN_EV", "0.03")))
    no_min_score: Decimal = Decimal(os.getenv("POLY_NO_MIN_SCORE", os.getenv("POLY_MIN_SCORE", "0.04")))
    exact_extra_edge: Decimal = Decimal(os.getenv("POLY_EXACT_EXTRA_EDGE", "0.00"))
    max_spread: Decimal = Decimal(os.getenv("POLY_MAX_SPREAD", "0.06"))
    max_price: Decimal = Decimal(os.getenv("POLY_MAX_PRICE", "0.85"))
    yes_max_price: Decimal = Decimal(os.getenv("POLY_YES_MAX_PRICE", os.getenv("POLY_MAX_PRICE", "0.85")))
    no_max_price: Decimal = Decimal(os.getenv("POLY_NO_MAX_PRICE", os.getenv("POLY_MAX_PRICE", "0.85")))
    min_volume: Decimal = Decimal(os.getenv("POLY_MIN_VOLUME", "500"))
    min_depth_multiplier: Decimal = Decimal(os.getenv("POLY_MIN_DEPTH_MULTIPLIER", "1.0"))
    max_days_ahead: int = int(os.getenv("POLY_MAX_DAYS_AHEAD", "7"))
    bankroll: Decimal = Decimal(os.getenv("POLY_BANKROLL", "1000"))
    kelly_fraction: Decimal = Decimal(os.getenv("POLY_KELLY_FRACTION", "0.20"))
    max_trade_pct: Decimal = Decimal(os.getenv("POLY_MAX_TRADE_PCT", "0.01"))
    probability_shrink: Decimal = Decimal(os.getenv("POLY_PROBABILITY_SHRINK", "0.70"))
    min_model_confidence: Decimal = Decimal(os.getenv("POLY_MIN_MODEL_CONFIDENCE", "0.55"))
    history_enabled: bool = os.getenv("POLY_HISTORY_ENABLED", "true").lower() == "true"
    history_weight: Decimal = Decimal(os.getenv("POLY_HISTORY_WEIGHT", "0.35"))
    history_lookback_days: int = int(os.getenv("POLY_HISTORY_LOOKBACK_DAYS", "365"))
    history_lookback_years: int = int(os.getenv("POLY_HISTORY_LOOKBACK_YEARS", "5"))
    history_window_days: int = int(os.getenv("POLY_HISTORY_WINDOW_DAYS", "15"))
    net_ev_spread_weight: Decimal = Decimal(os.getenv("POLY_NET_EV_SPREAD_WEIGHT", "0.50"))
    net_ev_low_price_cutoff: Decimal = Decimal(os.getenv("POLY_NET_EV_LOW_PRICE_CUTOFF", "0.08"))
    net_ev_low_price_penalty: Decimal = Decimal(os.getenv("POLY_NET_EV_LOW_PRICE_PENALTY", "0.02"))
    exact_cost_penalty: Decimal = Decimal(os.getenv("POLY_EXACT_COST_PENALTY", "0.03"))
    history_gap_reduce: Decimal = Decimal(os.getenv("POLY_HISTORY_GAP_REDUCE", "0.18"))
    history_gap_hard_cap: Decimal = Decimal(os.getenv("POLY_HISTORY_GAP_HARD_CAP", "0.35"))
    strong_signal_edge: Decimal = Decimal(os.getenv("POLY_STRONG_SIGNAL_EDGE", "0.18"))
    strong_signal_ev: Decimal = Decimal(os.getenv("POLY_STRONG_SIGNAL_EV", "0.20"))
    strong_signal_confidence: Decimal = Decimal(os.getenv("POLY_STRONG_SIGNAL_CONFIDENCE", "0.68"))
    max_signal_multiplier: Decimal = Decimal(os.getenv("POLY_MAX_SIGNAL_MULTIPLIER", "1.60"))
    weak_signal_multiplier: Decimal = Decimal(os.getenv("POLY_WEAK_SIGNAL_MULTIPLIER", "0.70"))
    exact_signal_multiplier: Decimal = Decimal(os.getenv("POLY_EXACT_SIGNAL_MULTIPLIER", "0.85"))
    yes_exact_signal_multiplier: Decimal = Decimal(os.getenv("POLY_YES_EXACT_SIGNAL_MULTIPLIER", "0.55"))
    live_min_order_size: Decimal = Decimal(os.getenv("POLY_LIVE_MIN_ORDER_SIZE", "1.00"))
    live_default_tick_size: Decimal = Decimal(os.getenv("POLY_LIVE_DEFAULT_TICK_SIZE", "0.01"))
    live_default_min_shares: Decimal = Decimal(os.getenv("POLY_LIVE_DEFAULT_MIN_SHARES", "5"))
    live_max_orders_per_scan: int = int(os.getenv("POLY_LIVE_MAX_ORDERS_PER_SCAN", "5"))
    live_max_dollars_per_scan: Decimal = Decimal(os.getenv("POLY_LIVE_MAX_DOLLARS_PER_SCAN", "5.00"))
    max_orders_per_city_date: int = int(os.getenv("POLY_MAX_ORDERS_PER_CITY_DATE", "1"))
    live_balance_retry_buffer: Decimal = Decimal(os.getenv("POLY_LIVE_BALANCE_RETRY_BUFFER", "0.98"))
    daily_take_profit_pct: Decimal = Decimal(os.getenv("POLY_DAILY_TAKE_PROFIT_PCT", "0.80"))
    take_profit_close_all_enabled: bool = os.getenv("POLY_TAKE_PROFIT_CLOSE_ALL_ENABLED", "true").lower() == "true"
    take_profit_close_same_day_only: bool = os.getenv("POLY_TAKE_PROFIT_CLOSE_SAME_DAY_ONLY", "true").lower() == "true"
    single_entry_per_slug: bool = os.getenv("POLY_SINGLE_ENTRY_PER_SLUG", "true").lower() == "true"
    auto_order: bool = os.getenv("POLY_AUTO_ORDER", "false").lower() == "true"
    private_key: str = os.getenv("POLY_PRIVATE_KEY", "")
    funder: str = os.getenv("POLY_FUNDER", "")
    signature_type: int = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
    chain_id: int = int(os.getenv("POLY_CHAIN_ID", "137"))


@dataclass
class ParsedMarket:
    city: str
    threshold_c: Decimal
    comparator: str
    target_date: Optional[str]
    temp_kind: str


@dataclass
class ForecastPoint:
    date: str
    temp_c: Decimal


@dataclass
class HistoricalStats:
    samples: int
    avg_c: Decimal
    min_c: Decimal
    max_c: Decimal
    prob_yes: Decimal


@dataclass
class IntradayContext:
    current_c: Optional[Decimal]
    target_peak_c: Optional[Decimal]
    target_low_c: Optional[Decimal]


@dataclass
class BookSide:
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    spread: Optional[Decimal]
    ask_depth_at_limit: Decimal


def http_get(url: str, params: dict[str, Any], timeout: int = 30, retries: int = 3) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                if VERBOSE:
                    print(
                        f"http_get retry={attempt + 1}/{retries} wait={wait}s url={url} err={exc}",
                        flush=True,
                    )
                time.sleep(wait)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"http_get failed without exception url={url}")


GEOCODE_CACHE: dict[str, dict[str, Any]] = {}
HISTORY_TEMP_CACHE: dict[tuple[str, str, str, int, int], list[Decimal]] = {}
INTRADAY_CACHE: dict[tuple[str, str], IntradayContext] = {}


def parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            out = json.loads(value)
            return out if isinstance(out, list) else []
        except json.JSONDecodeError:
            return []
    return []


def dec(value: Any) -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal("0")
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def clamp(value: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    return max(lo, min(hi, value))


def c_to_f(c: Decimal) -> Decimal:
    return c * Decimal("9") / Decimal("5") + Decimal("32")


def f_to_c(f: Decimal) -> Decimal:
    return (f - Decimal("32")) * Decimal("5") / Decimal("9")


def market_volume(market: dict[str, Any]) -> Decimal:
    for key in ["volumeNum", "volume", "liquidityNum", "liquidity"]:
        value = dec(market.get(key))
        if value > 0:
            return value
    return Decimal("0")


def is_temperature_event(event: dict[str, Any]) -> bool:
    text = " ".join(str(event.get(k, "")) for k in ["title", "slug", "description"]).lower()
    # Match the Polymarket page: https://polymarket.com/weather/high-temperature
    # This intentionally excludes low-temp, global climate, sea ice, hurricanes, etc.
    return "highest temperature in" in text


def fetch_high_temperature_event_slugs(cfg: Config) -> list[str]:
    # The High Temp page lazy-loads/virtualizes part of the list, so the raw HTML
    # only contains the first chunk. Combine page slugs with a broad Gamma scan.
    slugs: set[str] = set()
    try:
        html = requests.get(HIGH_TEMP_PAGE_URL, timeout=20).text
        slugs.update(re.findall(r"/event/(highest-temperature-in-[a-z0-9-]+)", html))
    except Exception as exc:
        print(f"high temp page slug fetch failed: {exc}", flush=True)

    for page in range(cfg.max_pages):
        params = {"active": "true", "closed": "false", "limit": cfg.scan_limit, "offset": page * cfg.scan_limit}
        if VERBOSE:
            print(f"scan_gamma_events_for_high_temp page={page + 1}/{cfg.max_pages} offset={page * cfg.scan_limit}", flush=True)
        try:
            data = http_get(GAMMA_EVENTS_URL, params)
        except Exception as exc:
            print(f"gamma events page fetch failed page={page + 1} offset={page * cfg.scan_limit} error={exc}", flush=True)
            continue
        events = data if isinstance(data, list) else data.get("data", [])
        if not events:
            break
        before = len(slugs)
        for event in events:
            slug = str(event.get("slug", ""))
            title = str(event.get("title", ""))
            text = f"{slug} {title}".lower()
            if slug.startswith("highest-temperature-in-") or "highest temperature in" in text:
                slugs.add(slug)
        if VERBOSE:
            print(f"high_temp_slugs={len(slugs)} page_added={len(slugs) - before}", flush=True)
    return sorted(slugs)


def fetch_event_by_slug(slug: str) -> Optional[dict[str, Any]]:
    data = http_get(GAMMA_EVENTS_URL, {"slug": slug})
    events = data if isinstance(data, list) else data.get("data", [])
    return events[0] if events else None


def fetch_temperature_markets(cfg: Config) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    seen = set()
    slugs = fetch_high_temperature_event_slugs(cfg)
    print(f"high_temp_events_total={len(slugs)} source={HIGH_TEMP_PAGE_URL}", flush=True)

    for idx, slug in enumerate(slugs, start=1):
        if VERBOSE:
            print(f"fetch_high_temp_event={idx}/{len(slugs)} slug={slug}", flush=True)
        try:
            event = fetch_event_by_slug(slug)
        except Exception as exc:
            if VERBOSE:
                print(f"event skipped slug={slug} error={exc}", flush=True)
            continue
        if not event:
            continue
        for market in event.get("markets") or []:
            market_slug = market.get("slug")
            if not market_slug or market_slug in seen:
                continue
            if market_volume(market) < cfg.min_volume:
                continue
            seen.add(market_slug)
            merged = dict(market)
            merged["eventTitle"] = event.get("title", "")
            merged["eventSlug"] = event.get("slug", "")
            merged["eventEndDate"] = event.get("endDate", "")
            markets.append(merged)
        if VERBOSE:
            print(f"temperature_markets_collected={len(markets)}", flush=True)
    return markets


def extract_city(text: str, market: dict[str, Any]) -> str:
    for source in [text, str(market.get("eventTitle", "")), str(market.get("eventSlug", ""))]:
        pretty = source.replace("-", " ")
        for pattern in [
            r"\bin\s+([A-Z][A-Za-z .'-]+?)(?:\s+be\b|\s+on\b|\s+for\b|\?|$)",
            r"temperature\s+in\s+([A-Z][A-Za-z .'-]+?)(?:\s+on\b|\?|$)",
        ]:
            m = re.search(pattern, pretty)
            if m:
                city = m.group(1).strip(" .?'")
                if len(city) >= 2:
                    return city
    return ""


def extract_date(text: str, market: dict[str, Any]) -> Optional[str]:
    for source in [text, str(market.get("eventTitle", "")), str(market.get("eventEndDate", "")), str(market.get("endDate", ""))]:
        iso = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", source)
        if iso:
            return iso.group(1)
        m = re.search(r"\b(?:on|for)\s+([A-Z][a-z]+\s+\d{1,2})(?:,\s*(20\d{2}))?", source)
        if m:
            year = int(m.group(2) or datetime.now().year)
            for fmt in ["%B %d", "%b %d"]:
                try:
                    dt = datetime.strptime(m.group(1), fmt)
                    return f"{year:04d}-{dt.month:02d}-{dt.day:02d}"
                except ValueError:
                    pass
    return None


def normalize_degree_text(text: str) -> str:
    # Gamma sometimes returns mojibake for °C as strings like "｡紊".
    return text.replace("º", "°").replace("｡紊", "°C").replace("掳C", "°C").replace("掳F", "°F")


def days_until_target(target_date: Optional[str]) -> int:
    if not target_date:
        return 999
    try:
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
        today = datetime.now().date()
        return (target - today).days
    except Exception:
        return 999


def parse_threshold_from_slug(slug: str) -> tuple[Optional[Decimal], Optional[str]]:
    slug = slug.lower()
    patterns = [
        (r"-(\d+(?:pt\d+)?)forbelow$", "below", "f"),
        (r"-(\d+(?:pt\d+)?)forhigher$", "above", "f"),
        (r"-(\d+(?:pt\d+)?)corbelow$", "below", "c"),
        (r"-(\d+(?:pt\d+)?)corhigher$", "above", "c"),
        (r"-(\d+(?:pt\d+)?)c$", "exact", "c"),
        (r"-(\d+(?:pt\d+)?)f$", "exact", "f"),
    ]
    range_match = re.search(r"-(\d+(?:pt\d+)?)-(\d+(?:pt\d+)?)f$", slug)
    if range_match:
        lo = Decimal(range_match.group(1).replace("pt", "."))
        hi = Decimal(range_match.group(2).replace("pt", "."))
        return f_to_c((lo + hi) / Decimal("2")), "exact"

    for pattern, comparator, unit in patterns:
        match = re.search(pattern, slug)
        if not match:
            continue
        value = Decimal(match.group(1).replace("pt", "."))
        return (value if unit == "c" else f_to_c(value)), comparator
    return None, None


def parse_temperature_market(market: dict[str, Any]) -> Optional[ParsedMarket]:
    question = normalize_degree_text(str(market.get("question") or market.get("title") or ""))
    lower = question.lower()
    slug = str(market.get("slug", ""))

    threshold_c, comparator = parse_threshold_from_slug(slug)
    if threshold_c is None:
        range_match = re.search(r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*(?:?\s*([cf])|degrees?\s*([cf])|([cf])\b)", lower)
        if range_match:
            lo = Decimal(range_match.group(1))
            hi = Decimal(range_match.group(2))
            unit = next((g for g in range_match.groups()[2:] if g), "c")
            mid = (lo + hi) / Decimal("2")
            threshold_c = mid if unit == "c" else f_to_c(mid)
            comparator = "exact"
        else:
            match = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:?\s*([cf])|degrees?\s*([cf])|([cf])\b)", lower)
            if not match:
                return None
            threshold = Decimal(match.group(1))
            unit = next((g for g in match.groups()[1:] if g), "c")
            threshold_c = threshold if unit == "c" else f_to_c(threshold)

    if comparator is None:
        if any(word in lower for word in ["below", "under", "less than", "lower than", "or below"]):
            comparator = "below"
        elif any(word in lower for word in ["or higher", "above", "over", "greater than", "higher than", "at least"]):
            comparator = "above"
        else:
            comparator = "exact"

    temp_kind = "max"
    city = extract_city(question, market)
    date = extract_date(question, market)
    if not city or not date:
        return None
    return ParsedMarket(city=city, threshold_c=threshold_c, comparator=comparator, target_date=date, temp_kind=temp_kind)


def geocode_city(city: str) -> Optional[dict[str, Any]]:
    cache_key = city.strip().lower()
    if cache_key in GEOCODE_CACHE:
        return GEOCODE_CACHE[cache_key]
    data = http_get(OPEN_METEO_GEOCODE_URL, {"name": city, "count": 1, "language": "en", "format": "json"})
    results = data.get("results", []) if isinstance(data, dict) else []
    if not results:
        return None
    GEOCODE_CACHE[cache_key] = results[0]
    return results[0]


def forecast_temperature_c(city: str, temp_kind: str, target_date: str, cfg: Config) -> ForecastPoint:
    geo = geocode_city(city)
    if not geo:
        raise ValueError(f"cannot geocode city={city}")
    params = {
        "latitude": geo["latitude"],
        "longitude": geo["longitude"],
        "daily": "temperature_2m_max,temperature_2m_min",
        "temperature_unit": "celsius",
        "forecast_days": cfg.max_days_ahead,
        "timezone": "auto",
    }
    data = http_get(OPEN_METEO_FORECAST_URL, params=params)
    daily = data.get("daily", {})
    dates = daily.get("time", [])
    values = daily.get("temperature_2m_min" if temp_kind == "min" else "temperature_2m_max", [])
    if not dates or not values:
        raise ValueError(f"no forecast for city={city}")
    idx = dates.index(target_date) if target_date in dates else 0
    return ForecastPoint(date=str(dates[idx]), temp_c=Decimal(str(values[idx])))


def intraday_temperature_context(city: str, target_date: str, cfg: Config) -> Optional[IntradayContext]:
    cache_key = (city.strip().lower(), target_date)
    if cache_key in INTRADAY_CACHE:
        return INTRADAY_CACHE[cache_key]
    geo = geocode_city(city)
    if not geo:
        return None
    data = http_get(
        OPEN_METEO_FORECAST_URL,
        {
            "latitude": geo["latitude"],
            "longitude": geo["longitude"],
            "current": "temperature_2m",
            "hourly": "temperature_2m",
            "temperature_unit": "celsius",
            "forecast_days": max(cfg.max_days_ahead, 2),
            "timezone": "auto",
        },
        timeout=30,
        retries=2,
    )
    current_raw = (data.get("current") or {}).get("temperature_2m")
    current_c = Decimal(str(current_raw)) if current_raw is not None else None
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    values = hourly.get("temperature_2m", [])
    target_values = [
        Decimal(str(value))
        for ts, value in zip(times, values)
        if value is not None and str(ts).startswith(target_date)
    ]
    ctx = IntradayContext(
        current_c=current_c,
        target_peak_c=max(target_values) if target_values else None,
        target_low_c=min(target_values) if target_values else None,
    )
    INTRADAY_CACHE[cache_key] = ctx
    return ctx


def yes_intraday_confirmed(parsed: ParsedMarket, ctx: Optional[IntradayContext], cfg: Config) -> tuple[bool, str]:
    if ctx is None:
        return False, "YES intraday context missing"
    distance = cfg.yes_intraday_confirm_distance
    current = ctx.current_c
    peak = ctx.target_peak_c
    low = ctx.target_low_c
    threshold = parsed.threshold_c
    if parsed.comparator == "exact":
        current_ok = current is not None and abs(current - threshold) <= distance
        peak_ok = peak is not None and abs(peak - threshold) <= distance
        if current_ok or peak_ok:
            return True, f"intraday exact confirmed current={current} peak={peak}"
        return False, f"YES exact intraday not confirmed current={current} peak={peak} threshold={threshold}"
    if parsed.comparator == "above":
        current_ok = current is not None and current >= threshold - distance
        peak_ok = peak is not None and peak >= threshold - distance
        if current_ok or peak_ok:
            return True, f"intraday above confirmed current={current} peak={peak}"
        return False, f"YES above intraday not confirmed current={current} peak={peak} threshold={threshold}"
    current_ok = current is not None and current <= threshold + distance
    peak_ok = peak is not None and peak <= threshold + distance
    low_ok = low is not None and low <= threshold + distance
    if current_ok and (peak_ok or low_ok):
        return True, f"intraday below confirmed current={current} peak={peak} low={low}"
    return False, f"YES below intraday not confirmed current={current} peak={peak} low={low} threshold={threshold}"


def historical_temperature_stats(
    city: str,
    temp_kind: str,
    target_date: str,
    threshold_c: Decimal,
    comparator: str,
    cfg: Config,
) -> Optional[HistoricalStats]:
    if not cfg.history_enabled:
        return None
    geo = geocode_city(city)
    if not geo:
        return None

    cache_key = (city.strip().lower(), temp_kind, target_date, cfg.history_lookback_years, cfg.history_window_days)
    temps = HISTORY_TEMP_CACHE.get(cache_key)
    if temps is None:
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
        temps = []
        for year_back in range(1, cfg.history_lookback_years + 1):
            try:
                center = target.replace(year=target.year - year_back)
            except ValueError:
                # Skip invalid dates such as Feb 29 on non-leap years.
                continue
            start = center - timedelta(days=cfg.history_window_days)
            end = center + timedelta(days=cfg.history_window_days)
            params = {
                "latitude": geo["latitude"],
                "longitude": geo["longitude"],
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "daily": "temperature_2m_max,temperature_2m_min",
                "temperature_unit": "celsius",
                "timezone": "auto",
            }
            try:
                data = http_get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
            except Exception as exc:
                if VERBOSE:
                    print(
                        f"history fetch failed city={city} target={target_date} year_back={year_back} error={exc}",
                        flush=True,
                    )
                continue

            daily = data.get("daily", {})
            values = daily.get("temperature_2m_min" if temp_kind == "min" else "temperature_2m_max", [])
            temps.extend(Decimal(str(value)) for value in values if value is not None)
        HISTORY_TEMP_CACHE[cache_key] = temps
    if not temps:
        return None

    wins = Decimal("0")
    for temp in temps:
        if comparator == "above" and temp >= threshold_c:
            wins += Decimal("1")
        elif comparator == "below" and temp <= threshold_c:
            wins += Decimal("1")
        elif comparator == "exact" and abs(temp - threshold_c) <= Decimal("0.5"):
            wins += Decimal("1")

    # Laplace smoothing avoids 0%/100% overconfidence from a small seasonal sample.
    prob_yes = (wins + Decimal("1")) / (Decimal(len(temps)) + Decimal("2"))
    return HistoricalStats(
        samples=len(temps),
        avg_c=sum(temps, Decimal("0")) / Decimal(len(temps)),
        min_c=min(temps),
        max_c=max(temps),
        prob_yes=prob_yes,
    )


def yes_city_blacklist_set(cfg: Config) -> set[str]:
    return {
        item.strip().lower()
        for item in cfg.yes_city_blacklist.split(",")
        if item.strip()
    }


def probability_from_band(forecast_c: Decimal, threshold_c: Decimal, comparator: str, band_c: Decimal) -> Decimal:
    if comparator == "exact":
        distance = abs(forecast_c - threshold_c)
        if distance <= Decimal("0.5"):
            return Decimal("0.70")
        if distance <= band_c:
            return Decimal("0.45")
        return Decimal("0.15")
    diff = forecast_c - threshold_c
    if comparator == "below":
        diff = -diff
    if diff >= band_c:
        return Decimal("0.85")
    if diff <= -band_c:
        return Decimal("0.15")
    return Decimal("0.50") + (diff / band_c) * Decimal("0.35")


def shrink_probability(p: Decimal, shrink: Decimal) -> Decimal:
    return Decimal("0.50") + (p - Decimal("0.50")) * clamp(shrink, Decimal("0"), Decimal("1"))


def blend_probabilities(forecast_prob: Decimal, history: Optional[HistoricalStats], cfg: Config) -> Decimal:
    if not history:
        return forecast_prob
    weight = clamp(cfg.history_weight, Decimal("0"), Decimal("0.80"))
    return forecast_prob * (Decimal("1") - weight) + history.prob_yes * weight


def get_order_book(token_id: str) -> Optional[dict[str, Any]]:
    try:
        return http_get(f"{CLOB_URL}/book", {"token_id": token_id})
    except Exception as exc:
        if VERBOSE:
            print(f"book fetch failed token={token_id}: {exc}", flush=True)
        return None


def book_side(book: Optional[dict[str, Any]], max_price: Decimal) -> BookSide:
    if not book:
        return BookSide(None, None, None, Decimal("0"))
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    # Polymarket CLOB book arrays are not guaranteed to be best-price first.
    # Use max bid and min ask explicitly.
    bid = max((Decimal(str(level["price"])) for level in bids), default=None)
    ask = min((Decimal(str(level["price"])) for level in asks), default=None)
    spread = ask - bid if ask is not None and bid is not None else None

    depth = Decimal("0")
    for level in asks:
        price = Decimal(str(level["price"]))
        size = Decimal(str(level.get("size", "0")))
        if price <= max_price:
            depth += size
    return BookSide(bid, ask, spread, depth)


def pick_tokens(market: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    outcomes = [str(x).lower() for x in parse_json_list(market.get("outcomes"))]
    token_ids = [str(x) for x in parse_json_list(market.get("clobTokenIds"))]
    if len(outcomes) != len(token_ids):
        return None, None
    yes = no = None
    for outcome, token in zip(outcomes, token_ids):
        if outcome in ["yes", "y"]:
            yes = token
        elif outcome in ["no", "n"]:
            no = token
    return yes, no


def kelly_size(prob: Decimal, price: Decimal, cfg: Config) -> tuple[Decimal, Decimal]:
    if price <= 0 or price >= 1:
        return Decimal("0"), Decimal("0")
    b = (Decimal("1") - price) / price
    k = max(Decimal("0"), (prob * (b + Decimal("1")) - Decimal("1")) / b)
    frac = min(k * cfg.kelly_fraction, cfg.max_trade_pct)
    dollars = (cfg.bankroll * frac).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return frac, dollars


def score(edge: Decimal, ev: Decimal, spread: Optional[Decimal], depth: Decimal, size: Decimal) -> Decimal:
    depth_bonus = min(depth / max(size, Decimal("1")), Decimal("3")) / Decimal("100")
    spread_penalty = spread if spread is not None else Decimal("1")
    ev_component = clamp(ev, Decimal("-1"), Decimal("1.50"))
    return (edge * Decimal("0.40")) + (ev_component * Decimal("0.45")) - (spread_penalty * Decimal("0.20")) + depth_bonus


def execution_cost_penalty(price: Decimal, spread: Optional[Decimal], comparator: str, cfg: Config) -> Decimal:
    penalty = (spread or Decimal("0")) * cfg.net_ev_spread_weight
    if price < cfg.net_ev_low_price_cutoff:
        penalty += cfg.net_ev_low_price_penalty
    if comparator == "exact":
        penalty += cfg.exact_cost_penalty
    return penalty


def signal_size_multiplier(
    prob: Decimal,
    edge: Decimal,
    ev: Decimal,
    comparator: str,
    side: str,
    history_gap: Decimal,
    cfg: Config,
) -> Decimal:
    certainty = abs(prob - Decimal("0.5")) * Decimal("2")
    multiplier = Decimal("1")
    if comparator == "exact":
        multiplier *= cfg.exact_signal_multiplier
    if comparator == "exact" and side == "YES":
        multiplier *= cfg.yes_exact_signal_multiplier
    if history_gap >= cfg.history_gap_hard_cap:
        multiplier *= Decimal("0.50")
    elif history_gap >= cfg.history_gap_reduce:
        multiplier *= Decimal("0.75")
    if edge >= cfg.strong_signal_edge and ev >= cfg.strong_signal_ev and certainty >= cfg.strong_signal_confidence:
        multiplier *= cfg.max_signal_multiplier
    elif edge < (cfg.min_edge * Decimal("1.20")) or ev < (cfg.min_ev * Decimal("1.20")):
        multiplier *= cfg.weak_signal_multiplier
    return clamp(multiplier, Decimal("0.35"), Decimal("2.00"))


def side_thresholds(side: str, cfg: Config) -> tuple[Decimal, Decimal, Decimal]:
    if side == "YES":
        return cfg.yes_min_edge, cfg.yes_min_ev, cfg.yes_min_score
    return cfg.no_min_edge, cfg.no_min_ev, cfg.no_min_score


def side_allowed(side: str, cfg: Config) -> bool:
    if cfg.allow_side in ["AUTO", "BOTH"]:
        return True
    return side == cfg.allow_side


def build_candidate(
    side: str,
    token_id: str,
    prob: Decimal,
    price: Decimal,
    spread: Optional[Decimal],
    depth: Decimal,
    comparator: str,
    history_prob_yes: Optional[Decimal],
    forecast_yes: Decimal,
    cfg: Config,
) -> dict[str, Any]:
    edge = prob - price
    if price <= 0 or price >= 1:
        gross_ev = Decimal("0")
    else:
        odds = (Decimal("1") - price) / price
        gross_ev = (prob * odds) - (Decimal("1") - prob)
    ev = gross_ev - execution_cost_penalty(price, spread, comparator, cfg)
    kfrac, base_order_size = kelly_size(prob, price, cfg)
    history_gap = abs(forecast_yes - history_prob_yes) if history_prob_yes is not None else Decimal("0")
    size_multiplier = signal_size_multiplier(prob, edge, ev, comparator, side, history_gap, cfg)
    if side == "YES" and price <= cfg.yes_early_max_price:
        size_multiplier *= cfg.yes_early_size_multiplier
    size_multiplier = clamp(size_multiplier, Decimal("0.20"), Decimal("2.00"))
    max_dollars = (cfg.bankroll * cfg.max_trade_pct).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    order_size = min((base_order_size * size_multiplier).quantize(Decimal("0.01"), rounding=ROUND_DOWN), max_dollars)
    sig_score = score(edge, ev, spread, depth, max(order_size, Decimal("1")))
    certainty = abs(prob - Decimal("0.5")) * Decimal("2")
    # Selection score chooses the better side before threshold checks.
    selection_score = sig_score + (certainty / Decimal("20")) + (clamp(ev, Decimal("-1"), Decimal("1.50")) / Decimal("8"))
    return {
        "side": side,
        "token_id": token_id,
        "prob": prob,
        "price": price,
        "spread": spread,
        "depth": depth,
        "edge": edge,
        "gross_ev": gross_ev,
        "ev": ev,
        "kelly_fraction": kfrac,
        "base_order_size": base_order_size,
        "size_multiplier": size_multiplier,
        "order_size": order_size,
        "score": sig_score,
        "certainty": certainty,
        "history_gap": history_gap,
        "selection_score": selection_score,
    }


def build_signal(market: dict[str, Any], cfg: Config) -> Optional[dict[str, Any]]:
    parsed = parse_temperature_market(market)
    if not parsed:
        return None
    target_days = days_until_target(parsed.target_date)
    if cfg.only_today and target_days != 0:
        return None
    accepting_orders = market_accepting_orders(market)
    minimum_tick_size = market_tick_size(market, cfg)
    minimum_order_size = market_min_order_size(market, cfg)
    yes_token, no_token = pick_tokens(market)
    if not yes_token or not no_token:
        return None
    yes_book = book_side(get_order_book(yes_token), cfg.yes_max_price)
    no_book = book_side(get_order_book(no_token), cfg.no_max_price)
    forecast = forecast_temperature_c(parsed.city, parsed.temp_kind, parsed.target_date, cfg)
    forecast_yes = probability_from_band(forecast.temp_c, parsed.threshold_c, parsed.comparator, cfg.temp_band_c)
    intraday_ctx = None
    intraday_ok = False
    intraday_reason = "intraday disabled"
    if cfg.yes_intraday_enabled and target_days <= cfg.yes_intraday_max_days_ahead:
        try:
            intraday_ctx = intraday_temperature_context(parsed.city, parsed.target_date, cfg)
            intraday_ok, intraday_reason = yes_intraday_confirmed(parsed, intraday_ctx, cfg)
        except Exception as exc:
            intraday_reason = f"YES intraday fetch failed: {exc}"
            if VERBOSE:
                print(f"intraday fetch failed city={parsed.city} target={parsed.target_date} error={exc}", flush=True)
    history = historical_temperature_stats(
        parsed.city,
        parsed.temp_kind,
        parsed.target_date,
        parsed.threshold_c,
        parsed.comparator,
        cfg,
    )
    raw_yes = blend_probabilities(forecast_yes, history, cfg)
    model_yes = shrink_probability(raw_yes, cfg.probability_shrink)
    model_no = Decimal("1") - model_yes
    forecast_no = Decimal("1") - forecast_yes

    candidates = []
    if yes_book.ask is not None:
        candidates.append(
            build_candidate(
                "YES",
                yes_token,
                model_yes,
                yes_book.ask,
                yes_book.spread,
                yes_book.ask_depth_at_limit,
                parsed.comparator,
                history.prob_yes if history else None,
                forecast_yes,
                cfg,
            )
        )
    if no_book.ask is not None:
        candidates.append(
            build_candidate(
                "NO",
                no_token,
                model_no,
                no_book.ask,
                no_book.spread,
                no_book.ask_depth_at_limit,
                parsed.comparator,
                (Decimal("1") - history.prob_yes) if history else None,
                forecast_no,
                cfg,
            )
        )
    if not candidates:
        return None

    candidates = [item for item in candidates if side_allowed(item["side"], cfg)]
    if not candidates:
        return None

    volume = market_volume(market)
    yes_blacklist = yes_city_blacklist_set(cfg)

    def candidate_reasons(item: dict[str, Any]) -> list[str]:
        item_min_edge, item_min_ev, item_min_score = side_thresholds(item["side"], cfg)
        if parsed.comparator == "exact":
            item_min_edge += cfg.exact_extra_edge
        if item["side"] == "YES" and parsed.comparator == "exact":
            item_min_edge += cfg.yes_exact_extra_edge
            item_min_ev += cfg.yes_exact_extra_ev
        min_depth_for_item = max(item["order_size"], Decimal("1")) * cfg.min_depth_multiplier
        out = []
        if forecast.date != parsed.target_date:
            out.append(f"target_date {parsed.target_date} not in forecast window, using {forecast.date}")
        if max(item["prob"], Decimal("1") - item["prob"]) < cfg.min_model_confidence:
            out.append(f"confidence {item['prob']:.4f} < {cfg.min_model_confidence}")
        if item["side"] == "YES" and parsed.city.strip().lower() in yes_blacklist:
            out.append(f"YES city blacklisted: {parsed.city}")
        if item["side"] == "YES" and parsed.comparator == "exact":
            if item["prob"] < cfg.yes_exact_min_confidence:
                out.append(f"YES exact confidence {item['prob']:.4f} < {cfg.yes_exact_min_confidence}")
            if item["price"] < cfg.yes_exact_min_price:
                out.append(f"YES exact price {item['price']} < {cfg.yes_exact_min_price}")
            if target_days > cfg.yes_exact_max_days_ahead:
                out.append(f"YES exact days_ahead {target_days} > {cfg.yes_exact_max_days_ahead}")
            if abs(forecast.temp_c - parsed.threshold_c) > cfg.yes_exact_max_forecast_distance:
                out.append(
                    f"YES exact forecast_distance {abs(forecast.temp_c - parsed.threshold_c):.4f} > {cfg.yes_exact_max_forecast_distance}"
                )
            if history:
                if history.prob_yes < cfg.yes_exact_min_history_prob:
                    out.append(f"YES exact history_prob {history.prob_yes:.4f} < {cfg.yes_exact_min_history_prob}")
                if abs(history.avg_c - parsed.threshold_c) > cfg.yes_exact_max_history_mean_distance:
                    out.append(
                        f"YES exact history_mean_distance {abs(history.avg_c - parsed.threshold_c):.4f} > {cfg.yes_exact_max_history_mean_distance}"
                    )
        if item["side"] == "YES" and item["price"] > cfg.yes_intraday_confirm_above_price:
            if not cfg.yes_intraday_enabled:
                out.append("YES mid-price requires intraday confirmation but intraday is disabled")
            elif target_days > cfg.yes_intraday_max_days_ahead:
                out.append(f"YES mid-price intraday days_ahead {target_days} > {cfg.yes_intraday_max_days_ahead}")
            elif not intraday_ok:
                out.append(intraday_reason)
        if item["side"] == "YES" and parsed.comparator == "above" and history:
            if history.prob_yes < cfg.yes_above_min_history_prob:
                out.append(f"YES above history_prob {history.prob_yes:.4f} < {cfg.yes_above_min_history_prob}")
        if item["edge"] < item_min_edge:
            out.append(f"{item['side']} edge {item['edge']:.4f} < {item_min_edge}")
        if item["ev"] < item_min_ev:
            out.append(f"{item['side']} ev {item['ev']:.4f} < {item_min_ev}")
        if item["score"] < item_min_score:
            out.append(f"{item['side']} score {item['score']:.4f} < {item_min_score}")
        if not accepting_orders:
            out.append("market not accepting orders")
        if item["history_gap"] > cfg.history_gap_hard_cap:
            out.append(f"history_gap {item['history_gap']:.4f} > {cfg.history_gap_hard_cap}")
        if item["side"] == "YES" and parsed.comparator == "exact" and item["history_gap"] > cfg.yes_exact_max_history_gap:
            out.append(f"YES exact history_gap {item['history_gap']:.4f} > {cfg.yes_exact_max_history_gap}")
        if item["spread"] is None or item["spread"] > cfg.max_spread:
            out.append(f"spread {item['spread']} > {cfg.max_spread}")
        if item["price"] < cfg.min_price:
            out.append(f"price {item['price']} < {cfg.min_price}")
        side_max_price = cfg.yes_max_price if item["side"] == "YES" else cfg.no_max_price
        if item["price"] > side_max_price:
            out.append(f"{item['side']} price {item['price']} > {side_max_price}")
        if volume < cfg.min_volume:
            out.append(f"volume {volume} < {cfg.min_volume}")
        if item["depth"] < min_depth_for_item:
            out.append(f"depth {item['depth']} < {min_depth_for_item}")
        if item["order_size"] <= 0:
            out.append("kelly size is zero")
        return out

    evaluated = [(item, candidate_reasons(item)) for item in candidates]
    passed = [(item, item_reasons) for item, item_reasons in evaluated if not item_reasons]
    if passed:
        candidate, reasons = max(passed, key=lambda pair: pair[0]["selection_score"])
    else:
        candidate, reasons = max(evaluated, key=lambda pair: pair[0]["selection_score"])

    side = candidate["side"]
    token_id = candidate["token_id"]
    prob = candidate["prob"]
    price = candidate["price"]
    spread = candidate["spread"]
    depth = candidate["depth"]
    edge = candidate["edge"]
    ev = candidate["ev"]
    kfrac = candidate["kelly_fraction"]
    order_size = candidate["order_size"]
    sig_score = candidate["score"]
    return {
        "slug": market.get("slug", ""),
        "question": normalize_degree_text(str(market.get("question", ""))),
        "city": parsed.city,
        "side": side,
        "token_id": token_id,
        "action": "BUY" if not reasons else "MONITOR",
        "forecast_date": forecast.date,
        "target_date": parsed.target_date,
        "forecast_c": forecast.temp_c,
        "threshold_c": parsed.threshold_c,
        "comparator": parsed.comparator,
        "model_yes_raw": raw_yes,
        "model_yes": model_yes,
        "model_prob_side": prob,
        "market_price": price,
        "spread": spread,
        "depth": depth,
        "volume": volume,
        "edge": edge,
        "gross_ev": candidate["gross_ev"],
        "ev": ev,
        "kelly_fraction": kfrac,
        "size_multiplier": candidate["size_multiplier"],
        "order_size": order_size,
        "score": sig_score,
        "minimum_tick_size": minimum_tick_size,
        "minimum_order_size": minimum_order_size,
        "accepting_orders": accepting_orders,
        "reason": "; ".join(reasons) if reasons else (
            f"best_side={side} forecast_prob_yes={forecast_yes:.4f} "
            f"history_prob_yes={(history.prob_yes if history else Decimal('0')):.4f} "
            f"history_samples={(history.samples if history else 0)} gross_ev={candidate['gross_ev']:.4f} "
            f"net_ev={ev:.4f} size_multiplier={candidate['size_multiplier']:.2f} "
            f"yes_mode={'early_low_price' if side == 'YES' and price <= cfg.yes_early_max_price else ('intraday_confirmed' if side == 'YES' and intraday_ok else 'standard')} "
            f"intraday={intraday_reason} "
            f"tick={minimum_tick_size} min_order_size={minimum_order_size} EV/Kelly/certainty/liquidity passed"
        ),
    }


def ensure_log() -> None:
    if LOG_PATH.exists():
        return
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            "ts", "slug", "city", "side", "action", "forecast_date", "target_date", "forecast_c", "threshold_c",
            "comparator", "model_yes_raw", "model_yes", "model_prob_side", "market_price", "spread", "depth",
            "volume", "edge", "gross_ev", "ev", "kelly_fraction", "size_multiplier", "order_size", "score",
            "minimum_tick_size", "minimum_order_size", "accepting_orders", "reason", "question",
        ])


def load_order_state() -> dict[str, Any]:
    if not ORDER_STATE_PATH.exists():
        return {}
    try:
        return json.loads(ORDER_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_order_state(state: dict[str, Any]) -> None:
    ORDER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ORDER_STATE_PATH.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def mark_order_state(state: dict[str, Any], row: dict[str, Any], mode: str, status: str) -> None:
    price = dec(row.get("market_price"))
    order_size = dec(row.get("order_size"))
    shares = dec(row.get("shares"))
    if shares <= 0 and price > 0 and order_size > 0:
        shares = (order_size / price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    state[row["slug"]] = {
        "slug": row["slug"],
        "side": row["side"],
        "city": row.get("city", ""),
        "target_date": row["target_date"],
        "price": str(price),
        "order_size": str(order_size),
        "original_shares": str(shares),
        "remaining_shares": str(shares),
        "mode": mode,
        "status": status,
        "take_profit_reduced": bool(row.get("take_profit_reduced", False)),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    save_order_state(state)


def city_date_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("city", "")).strip().lower(), str(row.get("target_date", "")).strip())


def city_date_order_count(order_state: dict[str, Any], row: dict[str, Any]) -> int:
    key = city_date_key(row)
    if not key[0] or not key[1]:
        return 0
    count = 0
    for item in order_state.values():
        if item.get("status") != "SENT":
            continue
        item_key = (str(item.get("city", "")).strip().lower(), str(item.get("target_date", "")).strip())
        if item_key == key:
            count += 1
    return count


def classify_poly_error(exc: Exception) -> str:
    text = str(exc).lower()
    if "not enough balance" in text or "allowance" in text:
        return "INSUFFICIENT_BALANCE"
    if "invalid amount" in text:
        return "INVALID_AMOUNT"
    if "lower than the minimum" in text or "minimum:" in text:
        return "MIN_SIZE"
    return "OTHER"


def parse_balance_error_dollars(exc: Exception) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    text = str(exc)
    balance_match = re.search(r"balance:\s*(\d+)", text, re.IGNORECASE)
    matched_match = re.search(r"sum of matched orders:\s*(\d+)", text, re.IGNORECASE)
    amount_match = re.search(r"order amount:\s*(\d+)", text, re.IGNORECASE)
    scale = Decimal("1000000")
    balance = Decimal(balance_match.group(1)) / scale if balance_match else None
    matched = Decimal(matched_match.group(1)) / scale if matched_match else None
    amount = Decimal(amount_match.group(1)) / scale if amount_match else None
    return balance, matched, amount


def prepare_live_order(row: dict[str, Any], cfg: Config, target_dollars: Optional[Decimal] = None) -> tuple[dict[str, Any], Decimal, Decimal]:
    live_row = dict(row)
    if target_dollars is not None and target_dollars > 0:
        live_row["order_size"] = target_dollars.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if live_row["order_size"] < cfg.live_min_order_size:
        live_row["order_size"] = cfg.live_min_order_size
        print(
            f"ORDER_SIZE_RAISED slug={live_row['slug']} original=${row['order_size']} raised_to=${cfg.live_min_order_size}",
            flush=True,
        )
    tick_size = dec(live_row.get("minimum_tick_size")) or cfg.live_default_tick_size
    min_order_size = dec(live_row.get("minimum_order_size")) or cfg.live_default_min_shares
    price = align_price(live_row["market_price"], tick_size)
    shares = (live_row["order_size"] / price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    min_live_shares = min_shares_for_notional(cfg.live_min_order_size, price)
    if shares < min_live_shares:
        shares = min_live_shares
        live_row["order_size"] = (shares * price).quantize(Decimal("0.01"), rounding=ROUND_UP)
        print(
            f"ORDER_NOTIONAL_RAISED slug={live_row['slug']} min_notional=${cfg.live_min_order_size} adjusted_shares={shares} adjusted_dollars=${live_row['order_size']}",
            flush=True,
        )
    min_market_shares = min_shares_for_floor(max(min_order_size, cfg.live_default_min_shares))
    if shares < min_market_shares:
        shares = min_market_shares
        live_row["order_size"] = (shares * price).quantize(Decimal("0.01"), rounding=ROUND_UP)
        print(
            f"ORDER_SHARE_FLOOR_RAISED slug={live_row['slug']} tick_size={tick_size} min_shares={min_market_shares} adjusted_shares={shares} adjusted_dollars=${live_row['order_size']}",
            flush=True,
        )
    return live_row, price, shares


def log_signal(row: dict[str, Any]) -> None:
    with LOG_PATH.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            datetime.now(timezone.utc).isoformat(), row["slug"], row["city"], row["side"], row["action"],
            row["forecast_date"], row["target_date"], str(row["forecast_c"]), str(row["threshold_c"]), row["comparator"],
            str(row["model_yes_raw"]), str(row["model_yes"]), str(row["model_prob_side"]), str(row["market_price"]),
            str(row["spread"]), str(row["depth"]), str(row["volume"]), str(row["edge"]), str(row["gross_ev"]), str(row["ev"]),
            str(row["kelly_fraction"]), str(row["size_multiplier"]), str(row["order_size"]), str(row["score"]),
            str(row.get("minimum_tick_size", "")), str(row.get("minimum_order_size", "")),
            str(row.get("accepting_orders", "")), row["reason"], row["question"],
        ])


def align_price(price: Decimal, tick: Decimal = Decimal("0.01")) -> Decimal:
    if tick <= 0:
        tick = Decimal("0.01")
    return (price / tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * tick


def min_shares_for_notional(notional: Decimal, price: Decimal, step: Decimal = Decimal("0.01")) -> Decimal:
    if price <= 0:
        return Decimal("0")
    raw = notional / price
    units = (raw / step).quantize(Decimal("1"), rounding=ROUND_UP)
    return units * step


def min_shares_for_floor(min_shares: Decimal, step: Decimal = Decimal("0.01")) -> Decimal:
    if min_shares <= 0:
        return Decimal("0")
    units = (min_shares / step).quantize(Decimal("1"), rounding=ROUND_UP)
    return units * step


def market_tick_size(market: dict[str, Any], cfg: Config) -> Decimal:
    for key in ["minimum_tick_size", "minimumTickSize"]:
        value = dec(market.get(key))
        if value > 0:
            return value
    return cfg.live_default_tick_size


def market_min_order_size(market: dict[str, Any], cfg: Config) -> Decimal:
    for key in ["minimum_order_size", "minimumOrderSize"]:
        value = dec(market.get(key))
        if value > 0:
            return value
    return cfg.live_default_min_shares


def market_accepting_orders(market: dict[str, Any]) -> bool:
    for key in ["accepting_orders", "acceptingOrders", "enableOrderBook", "orderPriceMinTickSize"]:
        if key not in market:
            continue
        value = market.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() == "true"
    return True


def normalize_tick_size_str(tick: Decimal) -> Optional[str]:
    tick = dec(tick)
    allowed = {"0.1", "0.01", "0.001", "0.0001"}
    if tick <= 0:
        return None
    tick_str = format(tick.normalize(), "f")
    return tick_str if tick_str in allowed else None


def build_clob_client(cfg: Config):
    if ClobClient is None:
        return None
    if USE_CLOB_V2:
        base_client = ClobClient(
            host=CLOB_URL,
            chain_id=cfg.chain_id,
            key=cfg.private_key,
            signature_type=cfg.signature_type,
            funder=cfg.funder or None,
        )
        creds = base_client.create_or_derive_api_key()
        return ClobClient(
            host=CLOB_URL,
            chain_id=cfg.chain_id,
            key=cfg.private_key,
            creds=creds,
            signature_type=cfg.signature_type,
            funder=cfg.funder or None,
        )
    client = ClobClient(CLOB_URL, key=cfg.private_key, chain_id=cfg.chain_id, signature_type=cfg.signature_type, funder=cfg.funder or None)
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def post_buy_order(client, token_id: str, price: Decimal, shares: Decimal, tick_size: Decimal):
    if USE_CLOB_V2:
        side = Side.BUY if Side is not None else "BUY"
        options = None
        tick_str = normalize_tick_size_str(tick_size)
        if PartialCreateOrderOptions is not None and tick_str:
            options = PartialCreateOrderOptions(tick_size=tick_str)
        return client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(shares),
                side=side,
            ),
            options=options,
            order_type=OrderType.GTC,
        )
    order = OrderArgs(token_id=token_id, price=float(price), size=float(shares), side=BUY)
    signed = client.create_order(order)
    return client.post_order(signed, OrderType.GTC)


def post_sell_order(client, token_id: str, price: Decimal, shares: Decimal, tick_size: Decimal):
    if USE_CLOB_V2:
        side = Side.SELL if Side is not None else "SELL"
        options = None
        tick_str = normalize_tick_size_str(tick_size)
        if PartialCreateOrderOptions is not None and tick_str:
            options = PartialCreateOrderOptions(tick_size=tick_str)
        return client.create_and_post_order(
            order_args=OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(shares),
                side=side,
            ),
            options=options,
            order_type=OrderType.GTC,
        )
    order = OrderArgs(token_id=token_id, price=float(price), size=float(shares), side=SELL)
    signed = client.create_order(order)
    return client.post_order(signed, OrderType.GTC)


def describe_runtime_wallet(cfg: Config) -> str:
    funder = (cfg.funder or "").strip()
    if not cfg.auto_order:
        return f"mode=DRY_RUN funder={funder or '<unset>'}"
    if not cfg.private_key:
        return f"mode=LIVE funder={funder or '<unset>'} private_key=<missing>"
    if Account is None:
        return f"mode=LIVE funder={funder or '<unset>'} derived=<eth_account missing>"
    try:
        derived = Account.from_key(cfg.private_key).address
    except Exception as exc:
        return f"mode=LIVE funder={funder or '<unset>'} derived=<invalid private key: {exc}>"
    matches = derived.lower() == funder.lower() if funder else False
    return (
        f"mode=LIVE funder={funder or '<unset>'} derived={derived} "
        f"funder_matches_private_key={matches}"
    )


def maybe_order(row: dict[str, Any], cfg: Config, order_state: dict[str, Any]) -> tuple[str, Decimal]:
    if row["action"] != "BUY":
        return "NO_ACTION", Decimal("0")
    if not row.get("accepting_orders", True):
        print(f"ORDER_SKIP market_not_accepting_orders slug={row['slug']}", flush=True)
        return "ORDER_SKIP_NOT_ACCEPTING", Decimal("0")
    if cfg.single_entry_per_slug and row["slug"] in order_state:
        existing = order_state[row["slug"]]
        print(
            f"SKIP_DUPLICATE slug={row['slug']} side={row['side']} existing_status={existing.get('status')} "
            f"existing_mode={existing.get('mode')}",
            flush=True,
        )
        return "SKIP_DUPLICATE", Decimal("0")
    if not cfg.auto_order:
        print(f"DRY_RUN {row['side']} price={row['market_price']} size=${row['order_size']} slug={row['slug']}")
        mark_order_state(order_state, row, mode="DRY_RUN", status="RECORDED")
        return "DRY_RUN_RECORDED", dec(row["order_size"])
    if not cfg.private_key:
        print("AUTO_ORDER requested but POLY_PRIVATE_KEY missing")
        return "AUTO_ORDER_MISSING_KEY", Decimal("0")
    if ClobClient is None:
        print("AUTO_ORDER requested but Polymarket client missing: pip install py_clob_client_v2")
        return "AUTO_ORDER_MISSING_CLIENT", Decimal("0")
    client = build_clob_client(cfg)
    if client is None:
        print("AUTO_ORDER requested but failed to initialize Polymarket client")
        return "AUTO_ORDER_CLIENT_INIT_FAILED", Decimal("0")
    live_row, price, shares = prepare_live_order(row, cfg)
    tick_size = dec(live_row.get("minimum_tick_size")) or cfg.live_default_tick_size
    try:
        resp = post_buy_order(client, row["token_id"], price, shares, tick_size)
    except Exception as exc:
        err_kind = classify_poly_error(exc)
        if err_kind == "INSUFFICIENT_BALANCE":
            balance_dollars, matched_dollars, requested_dollars = parse_balance_error_dollars(exc)
            if balance_dollars is not None:
                retry_budget = (balance_dollars * cfg.live_balance_retry_buffer).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if retry_budget >= cfg.live_min_order_size and retry_budget < dec(live_row["order_size"]):
                    retry_row, retry_price, retry_shares = prepare_live_order(row, cfg, retry_budget)
                    retry_tick_size = dec(retry_row.get("minimum_tick_size")) or cfg.live_default_tick_size
                    try:
                        retry_resp = post_buy_order(client, row["token_id"], retry_price, retry_shares, retry_tick_size)
                        print(
                            f"ORDER_SENT_REDUCED side={retry_row['side']} price={retry_price} shares={retry_shares} dollars={retry_row['order_size']} "
                            f"available=${balance_dollars} matched=${matched_dollars or Decimal('0')} requested=${requested_dollars or Decimal('0')} resp={retry_resp}",
                            flush=True,
                        )
                        mark_order_state(order_state, retry_row, mode="AUTO_ORDER", status="SENT")
                        return "ORDER_SENT", dec(retry_row["order_size"])
                    except Exception as retry_exc:
                        print(
                            f"ORDER_RETRY_FAILED slug={retry_row['slug']} retry_budget=${retry_budget} err={retry_exc}",
                            flush=True,
                        )
            print(
                f"ORDER_SKIP_INSUFFICIENT_BALANCE slug={live_row['slug']} dollars=${live_row['order_size']} "
                f"available=${balance_dollars if balance_dollars is not None else 'unknown'} err={exc}",
                flush=True,
            )
            return "ORDER_SKIP_INSUFFICIENT_BALANCE", Decimal("0")
        print(
            f"ORDER_FAILED kind={err_kind} slug={live_row['slug']} dollars=${live_row['order_size']} err={exc}",
            flush=True,
        )
        return f"ORDER_FAILED_{err_kind}", Decimal("0")
    print(f"ORDER_SENT side={live_row['side']} price={price} shares={shares} dollars={live_row['order_size']} resp={resp}")
    mark_order_state(order_state, live_row, mode="AUTO_ORDER", status="SENT")
    return "ORDER_SENT", dec(live_row["order_size"])


def estimate_today_open_profit(order_state: dict[str, Any], markets: list[dict[str, Any]], cfg: Config) -> tuple[Decimal, Decimal]:
    market_by_slug = {str(m.get("slug", "")): m for m in markets}
    total_cost = Decimal("0")
    total_value = Decimal("0")
    book_cache: dict[str, BookSide] = {}
    today_iso = datetime.now().date().isoformat()

    for row in order_state.values():
        if row.get("status") not in ["SENT", "REDUCED_PARTIAL"]:
            continue
        if row.get("target_date") != today_iso:
            continue
        slug = str(row.get("slug", ""))
        market = market_by_slug.get(slug)
        if not market:
            continue
        yes_token, no_token = pick_tokens(market)
        side = str(row.get("side", ""))
        token_id = yes_token if side == "YES" else no_token
        if not token_id:
            continue
        if token_id not in book_cache:
            book_cache[token_id] = book_side(get_order_book(token_id), cfg.max_price)
        book = book_cache[token_id]
        bid = book.bid
        if bid is None or bid <= 0:
            continue
        cost = dec(row.get("order_size"))
        entry_price = dec(row.get("price"))
        if cost <= 0 or entry_price <= 0:
            continue
        shares = dec(row.get("remaining_shares"))
        if shares <= 0:
            shares = cost / entry_price
        value = shares * bid
        total_cost += (shares * entry_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        total_value += value

    return total_cost, total_value - total_cost


def auto_close_positions_on_take_profit(
    order_state: dict[str, Any],
    markets: list[dict[str, Any]],
    cfg: Config,
) -> None:
    if not cfg.take_profit_close_all_enabled:
        return
    if not cfg.private_key or ClobClient is None:
        print("TAKE_PROFIT_CLOSE_SKIP missing_client_or_key", flush=True)
        return

    market_by_slug = {str(m.get("slug", "")): m for m in markets}
    today_iso = datetime.now().date().isoformat()
    candidates: list[dict[str, Any]] = []

    for slug, row in order_state.items():
        if row.get("status") not in ["SENT", "REDUCED_PARTIAL"]:
            continue
        if row.get("take_profit_closed"):
            continue
        if cfg.take_profit_close_same_day_only and row.get("target_date") != today_iso:
            continue
        market = market_by_slug.get(slug)
        if not market:
            continue
        yes_token, no_token = pick_tokens(market)
        side = str(row.get("side", ""))
        token_id = yes_token if side == "YES" else no_token
        if not token_id:
            continue
        book = book_side(get_order_book(token_id), cfg.max_price)
        bid = book.bid
        if bid is None or bid <= 0:
            continue
        entry_price = dec(row.get("price"))
        remaining_shares = dec(row.get("remaining_shares"))
        if remaining_shares <= 0:
            order_size = dec(row.get("order_size"))
            if entry_price > 0 and order_size > 0:
                remaining_shares = (order_size / entry_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if entry_price <= 0 or remaining_shares <= 0:
            continue
        multiple = (bid / entry_price) if entry_price > 0 else Decimal("0")
        candidates.append({
            "slug": slug,
            "row": row,
            "market": market,
            "token_id": token_id,
            "side": side,
            "bid": bid,
            "entry_price": entry_price,
            "remaining_shares": remaining_shares,
            "multiple": multiple,
            "minimum_tick_size": market_tick_size(market, cfg),
            "minimum_order_size": market_min_order_size(market, cfg),
        })

    candidates.sort(key=lambda item: item["multiple"], reverse=True)
    if not candidates:
        print("TAKE_PROFIT_CLOSE_SKIP no_positions_to_close", flush=True)
        return

    client = build_clob_client(cfg)
    if client is None:
        print("TAKE_PROFIT_CLOSE_SKIP client_init_failed", flush=True)
        return

    for item in candidates:
        remaining_shares = item["remaining_shares"]
        min_market_shares = min_shares_for_floor(max(item["minimum_order_size"], cfg.live_default_min_shares))
        full_notional = (remaining_shares * item["bid"]).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        if remaining_shares < min_market_shares or full_notional < cfg.live_min_order_size:
            print(
                f"TAKE_PROFIT_CLOSE_SKIP slug={item['slug']} side={item['side']} reason=below_min_sell_size remaining_shares={remaining_shares} bid={item['bid']}",
                flush=True,
            )
            order_state[item["slug"]]["take_profit_closed"] = True
            order_state[item["slug"]]["updated_at"] = datetime.now(timezone.utc).isoformat()
            save_order_state(order_state)
            continue

        sell_shares = remaining_shares

        sell_price = align_price(item["bid"], item["minimum_tick_size"])
        if sell_price <= 0:
            print(f"TAKE_PROFIT_CLOSE_SKIP slug={item['slug']} side={item['side']} reason=invalid_bid", flush=True)
            continue

        sell_notional = (sell_shares * sell_price).quantize(Decimal("0.01"), rounding=ROUND_UP)
        try:
            resp = post_sell_order(client, item["token_id"], sell_price, sell_shares, item["minimum_tick_size"])
        except Exception as exc:
            print(
                f"TAKE_PROFIT_CLOSE_FAILED slug={item['slug']} side={item['side']} multiple={item['multiple']:.2f} "
                f"shares={sell_shares} price={sell_price} err={exc}",
                flush=True,
            )
            continue

        state_row = order_state[item["slug"]]
        state_row["remaining_shares"] = "0"
        state_row["take_profit_closed"] = True
        state_row["updated_at"] = datetime.now(timezone.utc).isoformat()
        state_row["status"] = "TAKE_PROFIT_CLOSED"
        save_order_state(order_state)

        print(
            f"TAKE_PROFIT_CLOSE_EXECUTED slug={item['slug']} side={item['side']} multiple={item['multiple']:.2f} "
            f"sell_shares={sell_shares} remaining_shares=0 price={sell_price} notional=${sell_notional} resp={resp}",
            flush=True,
        )


def scan_once(cfg: Config) -> None:
    ensure_log()
    order_state = load_order_state()
    markets = fetch_temperature_markets(cfg)
    print(f"found_temperature_markets={len(markets)}", flush=True)
    if cfg.auto_order and cfg.stop_new_orders_on_take_profit:
        today_cost, today_profit = estimate_today_open_profit(order_state, markets, cfg)
        take_profit_dollars = (cfg.bankroll * cfg.daily_take_profit_pct).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        print(
            f"today_open_cost=${today_cost:.2f} today_open_profit=${today_profit:.2f} take_profit_target=${take_profit_dollars:.2f}",
            flush=True,
        )
        if today_profit >= take_profit_dollars:
            print(
                f"TAKE_PROFIT_REACHED profit=${today_profit:.2f} target=${take_profit_dollars:.2f} stop_new_orders=True",
                flush=True,
            )
            if cfg.take_profit_close_all_enabled:
                auto_close_positions_on_take_profit(order_state, markets, cfg)
            return
    rows = []
    total = len(markets)
    for idx, market in enumerate(markets, start=1):
        if VERBOSE:
            print(f"processing_market={idx}/{total} slug={market.get('slug')}", flush=True)
        try:
            row = build_signal(market, cfg)
            if row:
                rows.append(row)
        except Exception as exc:
            if VERBOSE:
                print(f"market skipped slug={market.get('slug')} error={exc}", flush=True)
    rows.sort(key=lambda r: r["score"], reverse=True)
    buy_count = sum(1 for r in rows if r["action"] == "BUY")
    orders_sent_this_scan = 0
    dollars_sent_this_scan = Decimal("0")
    print(
        f"scan_summary signals={len(rows)} buy_opportunities={buy_count} existing_ordered_slugs={len(order_state)}",
        flush=True,
    )
    for row in rows:
        already_ordered = cfg.single_entry_per_slug and row["slug"] in order_state
        if row["action"] == "BUY" and not already_ordered:
            print(
                f"BUY_OPPORTUNITY {row['side']} score={row['score']:.4f} net_ev={row['ev']:.4f} gross_ev={row['gross_ev']:.4f} edge={row['edge']:.4f} "
                f"p={row['model_prob_side']:.4f} price={row['market_price']} size=${row['order_size']} "
                f"size_mult={row['size_multiplier']:.2f} tick={row.get('minimum_tick_size')} min_shares={row.get('minimum_order_size')} "
                f"city={row['city']} forecast={row['forecast_c']}C "
                f"threshold={row['threshold_c']}C {row['comparator']} slug={row['slug']}",
                flush=True,
            )
        log_signal(row)
        if row["action"] == "BUY" and cfg.auto_order:
            if cfg.max_orders_per_city_date > 0:
                same_city_date_count = city_date_order_count(order_state, row)
                if same_city_date_count >= cfg.max_orders_per_city_date:
                    print(
                        f"SKIP_CITY_DATE_LIMIT city={row['city']} target_date={row['target_date']} "
                        f"existing={same_city_date_count} limit={cfg.max_orders_per_city_date} slug={row['slug']}",
                        flush=True,
                    )
                    continue
            if orders_sent_this_scan >= cfg.live_max_orders_per_scan:
                print(
                    f"SCAN_ORDER_LIMIT_REACHED limit={cfg.live_max_orders_per_scan} dollars_sent=${dollars_sent_this_scan}",
                    flush=True,
                )
                break
            projected = dollars_sent_this_scan + dec(row["order_size"])
            if projected > cfg.live_max_dollars_per_scan:
                print(
                    f"SCAN_DOLLAR_LIMIT_REACHED limit=${cfg.live_max_dollars_per_scan} current=${dollars_sent_this_scan} next=${row['order_size']}",
                    flush=True,
                )
                break
        result, actual_dollars = maybe_order(row, cfg, order_state)
        if result == "ORDER_SENT":
            orders_sent_this_scan += 1
            dollars_sent_this_scan += actual_dollars
        elif result == "ORDER_SKIP_INSUFFICIENT_BALANCE":
            print(
                f"SCAN_STOP_INSUFFICIENT_BALANCE after_orders={orders_sent_this_scan} dollars_sent=${dollars_sent_this_scan}",
                flush=True,
            )
            break


def main() -> None:
    cfg = Config()
    print("Polymarket temperature scanner started")
    print(describe_runtime_wallet(cfg))
    print(
        f"auto_order={cfg.auto_order} run_once={cfg.run_once} only_today={cfg.only_today} band=+/-{cfg.temp_band_c}C "
        f"allow_side={cfg.allow_side} min_price={cfg.min_price} max_price={cfg.max_price} "
        f"yes_max_price={cfg.yes_max_price} no_max_price={cfg.no_max_price} "
        f"history_enabled={cfg.history_enabled} history_weight={cfg.history_weight} "
        f"history_window=+/-{cfg.history_window_days}d "
        f"net_ev_spread_weight={cfg.net_ev_spread_weight} strong_signal_edge={cfg.strong_signal_edge} "
        f"strong_signal_ev={cfg.strong_signal_ev} max_signal_multiplier={cfg.max_signal_multiplier} "
        f"live_min_order_size={cfg.live_min_order_size} daily_take_profit_pct={cfg.daily_take_profit_pct} "
        f"take_profit_close_all={cfg.take_profit_close_all_enabled} clob_sdk={'v2' if USE_CLOB_V2 else 'v1'} "
        f"yes_early_max_price={cfg.yes_early_max_price} yes_early_size_multiplier={cfg.yes_early_size_multiplier} "
        f"yes_intraday_enabled={cfg.yes_intraday_enabled} "
        f"yes_intraday_confirm_above_price={cfg.yes_intraday_confirm_above_price} "
        f"yes_intraday_confirm_distance={cfg.yes_intraday_confirm_distance} "
        f"NO(edge/ev/score)={cfg.no_min_edge}/{cfg.no_min_ev}/{cfg.no_min_score} "
        f"YES(edge/ev/score)={cfg.yes_min_edge}/{cfg.yes_min_ev}/{cfg.yes_min_score}"
    )
    while True:
        scan_once(cfg)
        if cfg.run_once:
            break
        time.sleep(cfg.sleep_seconds)


if __name__ == "__main__":
    main()
