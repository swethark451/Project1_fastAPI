import csv
import json
import time
import argparse
import os
import requests
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from dotenv import load_dotenv

# ---------- Helpers ----------
load_dotenv()

def make_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def safe_get(session: requests.Session, url: str, params: dict, timeout: int = 10) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        resp = session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as e:
        return None, str(e)

# ---------- API Functions ----------

def geocode_city(session: requests.Session, city: str, country_code: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": city, "country": country_code, "count": 1}
    data, err = safe_get(session, url, params)
    if err:
        return None, err
    if not data or "results" not in data or not data["results"]:
        return None, "No geocode results"
    return data["results"][0], None

def get_weather(session: requests.Session, lat: float, lon: float) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": lat, "longitude": lon, "current_weather": True}
    data, err = safe_get(session, url, params)
    if err:
        return None, err
    return data.get("current_weather"), None

def convert_fx_to_usd(session: requests.Session, local_currency: str, amount: Decimal, exchangerate_key: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    url = "https://api.exchangerate.host/convert"
    params: Dict[str, str] = {"from": local_currency, "to": "USD", "amount": str(amount)}
    if exchangerate_key:
        params["access_key"] = exchangerate_key

    data, err = safe_get(session, url, params)
    if err:
        return None, err

    result = (data or {}).get("result")
    info = (data or {}).get("info", {}) or {}
    rate = info.get("rate")

    if rate is None and result is not None:
        try:
            rate = float(result) / float(amount)
        except Exception:
            pass

    if result is None or rate is None:
        return None, f"FX conversion failed: {data}"

    return {"fx_rate_to_usd": rate, "amount_usd": result}, None

# ---------- Main ----------

def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich expenses and output CSV")
    parser.add_argument("--input", "-i", default="expenses.csv", help="Input CSV path")
    parser.add_argument("--output", "-o", default="enriched_expenses.csv", help="Output CSV path")
    parser.add_argument("--sleep", type=float, default=0.5, help="Sleep between API calls (seconds)")
    parser.add_argument("--fx-key", type=str, default=None, help="Optional exchangerate.host access key")
    args = parser.parse_args()

    # fallback to env variable
    fx_key = args.fx_key or os.environ.get("FX_API_KEY")

    session = make_session()

    # read CSV
    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    enriched_rows = []
    for idx, row in enumerate(rows, start=1):
        city = row["city"]
        country = row["country_code"]
        currency = row["local_currency"]
        amount = Decimal(row["amount"])

        errors: List[str] = []

        # Geocode
        geo, err = geocode_city(session, city, country)
        if not geo:
            errors.append(f"geocode: {err}")
            continue
        lat, lon = geo.get("latitude"), geo.get("longitude")

        # Weather
        weather, err = get_weather(session, lat, lon)
        if not weather:
            errors.append(f"weather: {err}")

        # FX conversion
        fx, err = convert_fx_to_usd(session, currency, amount, exchangerate_key=fx_key)
        if not fx:
            errors.append(f"fx: {err}")

        enriched_rows.append({
            "city": city,
            "country_code": country,
            "local_currency": currency,
            "amount": amount,
            "latitude": lat,
            "longitude": lon,
            "temperature_c": weather.get("temperature") if weather else None,
            "windspeed_m_s": weather.get("windspeed") if weather else None,
            "fx_rate_to_usd": fx.get("fx_rate_to_usd") if fx else None,
            "amount_usd": fx.get("amount_usd") if fx else None,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "errors": "; ".join(errors) if errors else ""
        })

        time.sleep(args.sleep)

    # write output CSV

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "city", "country_code", "local_currency", "amount",
            "fx_rate_to_usd","amount_usd",
            "latitude", "longitude",
            "temperature_c", "windspeed_m_s",
            "retrieved_at",
              "errors"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)


    print(f"✅ Enriched data written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
