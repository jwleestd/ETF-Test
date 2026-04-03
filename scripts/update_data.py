import json
import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen, Request

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(WORKSPACE_ROOT, "data", "etfs.json")

# 금융위원회_증권상품시세정보 (GetSecuritiesProductInfoService)
# 서비스 URL: https://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService
# ETF 시세 조회: /getETFPriceInfo
DATA_GO_KR_BASE_URL = "https://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService"
ETF_PRICE_ENDPOINT = "/getETFPriceInfo"

# Common field name candidates seen in KRX-related public APIs.
FIELD_CANDIDATES = {
    "ticker": ["srtnCd", "ticker", "code", "itmsNm"],
    "close": ["clpr", "close", "trdPrc", "tdd_clsprc"],
    "change": ["vs", "change", "fluc"],
    "change_pct": ["fltRt", "changeRate", "rate"],
    "volume": ["trqu", "volume", "accTrdVol", "tdd_trdvol"],
    "date": ["basDt", "date", "trdDd"],
}


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def first_existing(item, keys):
    for key in keys:
        if key in item and item[key] not in (None, ""):
            return item[key]
    return None


def to_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        if isinstance(value, str) and "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return None


def fetch_json(url, headers=None):
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_items(payload):
    # Tries typical data.go.kr response shapes.
    if isinstance(payload, dict):
        if "response" in payload:
            payload = payload["response"]
        if "body" in payload:
            payload = payload["body"]
        if "items" in payload:
            payload = payload["items"]
    if isinstance(payload, dict) and "item" in payload:
        payload = payload["item"]
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return payload
    return []


def build_etf_price_url(service_key, ticker=None, bas_dt=None):
    # Parameters per official guide:
    # serviceKey, numOfRows, pageNo, resultType, basDt, likeSrtnCd ...
    params = {
        "serviceKey": service_key,
        "resultType": "json",
        "numOfRows": 100,
        "pageNo": 1,
    }
    if ticker:
        params["likeSrtnCd"] = ticker
    if bas_dt:
        params["basDt"] = bas_dt
    return f"{DATA_GO_KR_BASE_URL}{ETF_PRICE_ENDPOINT}?{urlencode(params)}"


def update_prices(data, items):
    etfs = data.get("etfs", [])
    by_ticker = {e.get("ticker"): e for e in etfs}

    for item in items:
        ticker = first_existing(item, FIELD_CANDIDATES["ticker"])
        if not ticker:
            continue
        if ticker not in by_ticker:
            continue

        etf = by_ticker[ticker]
        price = etf.get("price", {})

        close = to_number(first_existing(item, FIELD_CANDIDATES["close"]))
        change = to_number(first_existing(item, FIELD_CANDIDATES["change"]))
        change_pct = to_number(first_existing(item, FIELD_CANDIDATES["change_pct"]))
        volume = to_number(first_existing(item, FIELD_CANDIDATES["volume"]))
        date_value = first_existing(item, FIELD_CANDIDATES["date"])

        if close is not None:
            price["close"] = close
        if change is not None:
            price["change"] = change
        if change_pct is not None:
            price["change_pct"] = change_pct
        if volume is not None:
            price["volume"] = volume
        if date_value:
            price["date"] = date_value

        etf["price"] = price


def fetch_latest_for_ticker(api_key, ticker, max_lookback_days=5):
    # Try today, then go back a few days to find the latest trading day.
    today = datetime.now().date()
    for delta in range(max_lookback_days + 1):
        bas_dt = (today - timedelta(days=delta)).strftime("%Y%m%d")
        url = build_etf_price_url(api_key, ticker=ticker, bas_dt=bas_dt)
        payload = fetch_json(url)
        items = extract_items(payload)
        if items:
            return items
    return []


def main():
    if not os.path.exists(DATA_PATH):
        print(f"Missing data file: {DATA_PATH}")
        sys.exit(1)

    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key:
        print("DATA_GO_KR_API_KEY is not set. Skipping update.")
        return

    data = load_json(DATA_PATH)

    # Fetch per ticker to avoid pulling the full dataset.
    all_items = []
    for etf in data.get("etfs", []):
        ticker = etf.get("ticker")
        if not ticker:
            continue
        try:
            items = fetch_latest_for_ticker(api_key, ticker)
        except Exception as exc:
            print(f"Failed to fetch ETF price for {ticker}: {exc}")
            continue
        all_items.extend(items)

    if not all_items:
        print("No items returned from ETF price API.")
        return

    update_prices(data, all_items)

    data["as_of"] = datetime.now().date().isoformat()
    data.setdefault("source_notes", {})["price"] = (
        "data.go.kr 증권상품시세정보 getETFPriceInfo"
    )

    save_json(DATA_PATH, data)
    print("Updated ETF prices.")


if __name__ == "__main__":
    main()
