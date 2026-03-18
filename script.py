import asyncio
import httpx
import os
import json
import zipfile
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

DATA_URL = "https://dati.anticorruzione.it/opendata/download/dataset/cig-2025/filesystem/cig_json_2025_01.zip"

# ---------- UTILS ----------
def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
    except:
        return None

def clean_record(record):
    ocid = record.get("cig")

    if not ocid:
        return None

    try:
        amount = float(record.get("importo", 0))
    except:
        return None

    if amount < 50000:
        return None

    return {
        "ocid": ocid,
        "title": record.get("oggetto"),
        "amount": amount,
        "published_date": parse_date(record.get("data_pubblicazione")),
        "raw": record
    }

# ---------- DOWNLOAD + EXTRACT ----------
async def download_and_extract():
    async with httpx.AsyncClient() as client:
        r = await client.get(DATA_URL, timeout=60)

        print("DOWNLOAD STATUS:", r.status_code)

        if r.status_code != 200:
            return None

        with open("data.zip", "wb") as f:
            f.write(r.content)

    with zipfile.ZipFile("data.zip", "r") as zip_ref:
        zip_ref.extractall("data")

    # trova il JSON dentro lo zip
    for file in os.listdir("data"):
        if file.endswith(".json"):
            return f"data/{file}"

    return None

# ---------- FETCH ----------
async def fetch():
    json_path = await download_and_extract()

    if not json_path:
        print("Errore download o unzip")
        return []

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print("Totale record:", len(data))

    return data[:100]  # test

# ---------- INSERT ----------
async def insert(data):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/tenders",
            headers=HEADERS,
            json=data
        )

        print("INSERT STATUS:", r.status_code)

        if r.status_code >= 300:
            print("Errore:", r.text)

# ---------- MAIN ----------
async def main():
    records = await fetch()

    cleaned = []
    for r in records:
        c = clean_record(r)
        if c:
            cleaned.append(c)

    print("Dopo filtro:", len(cleaned))

    if not cleaned:
        print("Nessun dato valido")
        return

    await insert(cleaned)
    print(f"Inseriti {len(cleaned)} record")

if __name__ == "__main__":
    asyncio.run(main())
