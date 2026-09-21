"""Pull real AWS us-east-1 list prices from the public Price List API.

Prints a compact summary used to fill docs/COST.md. Read-only, no credentials.
"""

from __future__ import annotations

import json
import re
import urllib.request
from typing import Any, cast

BASE = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/us-east-1/index.json"
)


def fetch(service: str) -> dict[str, Any]:
    request = urllib.request.Request(BASE.format(service=service), headers={"User-Agent": "curl/8"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return cast(dict[str, Any], json.load(response))


def walk(service: str, pattern: str, limit: int = 8, extra: str | None = None) -> list[str]:
    try:
        document = fetch(service)
    except Exception as exc:
        return [f"{service}: FETCH FAILED {exc}"]
    products = document.get("products", {})
    terms = document.get("terms", {}).get("OnDemand", {})
    matcher = re.compile(pattern, re.IGNORECASE)
    rows: list[str] = []
    for sku, product in products.items():
        attributes = product.get("attributes", {})
        blob = " ".join(f"{k}={v}" for k, v in attributes.items())
        if not matcher.search(blob):
            continue
        if extra and not re.search(extra, blob, re.IGNORECASE):
            continue
        for offer in terms.get(sku, {}).values():
            for dimension in offer.get("priceDimensions", {}).values():
                price = dimension.get("pricePerUnit", {}).get("USD")
                if price in (None, "0.0000000000"):
                    continue
                rows.append(
                    f"{service} | {attributes.get('usagetype', '?')} | "
                    f"{dimension.get('description', '')[:80]} | USD {price} per "
                    f"{dimension.get('unit', '?')}"
                )
        if len(rows) >= limit:
            break
    return rows[:limit]


TARGETS = [
    ("AWSLambda", r"Request|GB-Second", 6, None),
    ("AmazonS3", r"TimedStorage|Requests-Tier1", 6, "Standard"),
    ("AmazonSES", r"Emails|Message", 6, None),
    ("AmazonCloudWatch", r"DataProcessing|TimedStorage|Metric", 6, None),
    ("AmazonApiGateway", r"HttpApi|ApiGateway", 6, None),
]

if __name__ == "__main__":
    for service, pattern, limit, extra in TARGETS:
        print(f"--- {service} ---")
        for row in walk(service, pattern, limit, extra):
            print(row)
