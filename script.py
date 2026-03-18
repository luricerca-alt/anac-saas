import asyncio
import httpx
import os
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
    except:
        return None

def clean_release(release):
    ocid = release.get("ocid")
    if not ocid:
        return None

    tender = release.get("tender", {})

    if tender.get("status") != "active":
        return None

    amount = tender.get("value", {}).get("amount", 0)
    if amount < 50000:
        return None

    return {
        "ocid": ocid,
        "title": tender.get("title"),
        "amount": amount,
        "published_date": parse_date(release.get("date")),
        "raw": release
    }

async def fetch():
    url = "https://api.anticorruzione.it/opendata/release/tender/12345"
    async with httpx.AsyncClient() as client:
        r = await client.get(url)
        return r.json()

async def insert(data):
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{SUPABASE_URL}/rest/v1/tenders",
            headers=HEADERS,
            json=data
        )

async def main():
    data = await fetch()
    releases = data.get("releases", [])

    cleaned = []
    for r in releases:
        c = clean_release(r)
        if c:
            cleaned.append(c)

    if cleaned:
        await insert(cleaned)
        print("Dati inseriti")

asyncio.run(main())
