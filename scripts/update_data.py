import json
import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen, Request

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(WORKSPACE_ROOT, "data", "etfs.json")
LEGACY_DATA_PATH = os.path.join(WORKSPACE_ROOT, "public", "data", "etfs.json")

# 금융위원회_증권상품시세정보 (GetSecuritiesProductInfoService)
# 서비스 URL: https://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService
# ETF 시세 조회: /getETFPriceInfo
DATA_GO_KR_BASE_URL = "https://apis.data.go.kr/1160100/service/GetSecuritiesProductInfoService"
ETF_PRICE_ENDPOINT = "/getETFPriceInfo"

# Common field name candidates seen in KRX-related public APIs.
FIELD_CANDIDATES = {
    "ticker": ["srtnCd", "ticker", "code"],
    "name": ["itmsNm", "name", "itmsNm"],
    "isin": ["isinCd", "isin", "isin_code"],
    "close": ["clpr", "close", "trdPrc", "tdd_clsprc"],
    "change": ["vs", "change", "fluc"],
    "change_pct": ["fltRt", "changeRate", "rate"],
    "volume": ["trqu", "volume", "accTrdVol", "tdd_trdvol"],
    "market_cap": ["mrktTotAmt", "marketCap", "tdd_mrktTotAmt"],
    "date": ["basDt", "date", "trdDd"],
}


def load_json(path):
    # Handle UTF-8 BOM that can appear in Windows-saved JSON files.
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def resolve_data_path():
    if os.path.exists(DATA_PATH):
        return DATA_PATH
    if os.path.exists(LEGACY_DATA_PATH):
        return LEGACY_DATA_PATH
    return DATA_PATH


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


def build_etf_price_url(service_key, ticker=None, bas_dt=None, like_name=None, page_no=1, num_rows=1000):
    # Parameters per official guide:
    # serviceKey, numOfRows, pageNo, resultType, basDt, likeSrtnCd ...
    params = {
        "serviceKey": service_key,
        "resultType": "json",
        "numOfRows": num_rows,
        "pageNo": page_no,
    }
    if ticker:
        params["likeSrtnCd"] = ticker
    if like_name:
        params["likeItmsNm"] = like_name
    if bas_dt:
        params["basDt"] = bas_dt
    return f"{DATA_GO_KR_BASE_URL}{ETF_PRICE_ENDPOINT}?{urlencode(params)}"


def build_etf_from_item(item):
    ticker = first_existing(item, FIELD_CANDIDATES["ticker"])
    name = first_existing(item, FIELD_CANDIDATES["name"])
    isin = first_existing(item, FIELD_CANDIDATES["isin"])
    close = to_number(first_existing(item, FIELD_CANDIDATES["close"]))
    change = to_number(first_existing(item, FIELD_CANDIDATES["change"]))
    change_pct = to_number(first_existing(item, FIELD_CANDIDATES["change_pct"]))
    volume = to_number(first_existing(item, FIELD_CANDIDATES["volume"]))
    market_cap = to_number(first_existing(item, FIELD_CANDIDATES["market_cap"]))
    date_value = first_existing(item, FIELD_CANDIDATES["date"])

    if not ticker:
        return None
    # Exclude non-Korean listings by ISIN prefix.
    if isin and not str(isin).startswith("KR"):
        return None
    # Exclude overseas ETFs by name keywords (e.g., 미국/해외/글로벌).
    if name and has_overseas_keyword(name):
        return None

    return {
        "id": f"KRX:{ticker}",
        "ticker": ticker,
        "name_ko": name or "-",
        "issuer": None,
        "listed_date": None,
        "expense_ratio": None,
        "price": {
            "close": close,
            "change": change,
            "change_pct": change_pct,
            "volume": volume,
            "market_cap": market_cap,
            "date": date_value,
        },
        "distribution": {
            "latest_amount": None,
            "latest_record_date": None,
            "latest_pay_date": None,
            "frequency": None,
            "currency": "KRW",
        },
        "covered_call_ratio": {
            "value": None,
            "unit": "percent",
            "date": None,
            "method": "not_available",
        },
        "links": {
            "issuer": None,
            "factsheet": None,
            "disclosure": None,
        },
    }


def has_overseas_keyword(name):
    keywords = [
        "미국", "US", "USA", "U.S", "U S",
        "해외", "글로벌", "월드", "WORLD",
        "S&P", "S&P500", "NASDAQ", "나스닥", "다우", "다우존스", "DOW", "DOWJONES",
        "MSCI", "FTSE", "RUSSELL", "Russell",
        "유럽", "일본", "중국", "홍콩", "베트남", "인도", "브라질", "터키",
        "인도네시아", "동남아", "아시아", "EM", "신흥", "선진", "신흥국",
        "글로벌", "WORLD", "GLOBE", "글로벌",
    ]
    upper = str(name).upper()
    for kw in keywords:
        if kw.upper() in upper:
            return True
    return False


def fetch_latest_for_keyword(api_key, keyword, max_lookback_days=5):
    # Try today, then go back a few days to find the latest trading day.
    today = datetime.now().date()
    for delta in range(max_lookback_days + 1):
        bas_dt = (today - timedelta(days=delta)).strftime("%Y%m%d")
        # paginate in case there are many results
        page_no = 1
        collected = []
        while True:
            url = build_etf_price_url(
                api_key,
                bas_dt=bas_dt,
                like_name=keyword,
                page_no=page_no,
                num_rows=1000,
            )
            payload = fetch_json(url)
            items = extract_items(payload)
            if not items:
                break
            collected.extend(items)
            if len(items) < 1000:
                break
            page_no += 1
        if collected:
            return collected
    return []


def main():
    data_path = resolve_data_path()
    if not os.path.exists(data_path):
        print(f"Missing data file: {data_path}")
        sys.exit(1)

    api_key = os.environ.get("DATA_GO_KR_API_KEY")
    if not api_key:
        print("DATA_GO_KR_API_KEY is not set. Skipping update.")
        return

    data = load_json(data_path)

    # Fetch ETFs whose name includes "배당", then pick top 5 by market cap.
    try:
        items = fetch_latest_for_keyword(api_key, "배당")
    except Exception as exc:
        print(f"Failed to fetch ETF prices: {exc}")
        return

    if not items:
        print("No items returned from ETF price API.")
        return

    etfs = []
    for item in items:
        etf = build_etf_from_item(item)
        if etf:
            etfs.append(etf)

    # Sort by market cap desc and take top 5
    etfs.sort(key=lambda e: (e.get("price", {}).get("market_cap") or 0), reverse=True)
    data["etfs"] = etfs[:5]

    data["as_of"] = datetime.now().date().isoformat()
    data.setdefault("source_notes", {})["price"] = (
        "data.go.kr 증권상품시세정보 getETFPriceInfo (likeItmsNm=배당, top5 by mrktTotAmt)"
    )

    save_json(data_path, data)
    if data_path != DATA_PATH:
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        save_json(DATA_PATH, data)
    print("Updated ETF prices.")


if __name__ == "__main__":
    main()
