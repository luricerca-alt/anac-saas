import asyncio
import httpx
import os
import json
import zipfile
from datetime import datetime

# ---------- CONFIG ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

# ---------- DATASET ----------

# CIG mensili (2025 come esempio)
MONTHS = ["01", "02", "03"]

# altri dataset singoli
DATASETS = [
    "cig-2025",
    "subappalti",
    "aggiudicazioni",
    "pubblicazioni"
]

BASE_URL = "https://dati.anticorruzione.it/opendata/download/dataset"

# ---------- UTILS ----------
def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except:
        return None

def is_open(record):
    scadenza = record.get("data_scadenza_offerta")
    if not scadenza:
        return False
    try:
        return datetime.fromisoformat(scadenza) >= datetime.now()
    except:
        return False

def clean_cig(record):
    ocid = record.get("cig")
    if not ocid:
        return None

    amount = record.get("importo_complessivo_gara") or record.get("importo_lotto") or 0
    try:
        amount = float(amount)
    except:
        return None

    if amount < 50000:
        return None
    if not is_open(record):
        return None
    # filtro opzionale solo lavori
    if record.get("oggetto_principale_contratto") != "LAVORI":
        return None

    return {
        "ocid": ocid,
        "title": record.get("oggetto_gara") or record.get("oggetto_lotto"),
        "amount": amount,
        "published_date": parse_date(record.get("data_pubblicazione")).isoformat() if parse_date(record.get("data_pubblicazione")) else None,
        "raw": record
    }

# Per gli altri dataset puoi aggiungere altre funzioni di cleaning
def clean_generic(record):
    ocid = record.get("cig") or record.get("id")
    if not ocid:
        return None
    return {
        "ocid": ocid,
        "title": record.get("oggetto_gara") or record.get("oggetto_lotto") or record.get("denominazione_amministrazione_appaltante"),
        "amount": record.get("importo_complessivo_gara") or record.get("importo_lotto") or 0,
        "published_date": parse_date(record.get("data_pubblicazione")).isoformat() if parse_date(record.get("data_pubblicazione")) else None,
        "raw": record
    }

# ---------- URL BUILDER ----------
def build_url(dataset, month=None):
    if dataset == "cig-2025":
        return f"{BASE_URL}/{dataset}/filesystem/cig_json_2025_{month}.zip"
    else:
        return f"{BASE_URL}/{dataset}/filesystem/{dataset}_json.zip"

# ---------- DOWNLOAD ----------
async def download_zip(url, filename):
    async with httpx.AsyncClient() as client:
        r = await client.get(url, timeout=120)
        print("DOWNLOAD:", url, "STATUS:", r.status_code)
        if r.status_code != 200:
            return None
        with open(filename, "wb") as f:
            f.write(r.content)
    return filename

# ---------- EXTRACT ----------
def extract_zip(zip_path):
    folder = zip_path.replace(".zip", "")
    os.makedirs(folder, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(folder)
    for file in os.listdir(folder):
        if file.endswith(".json"):
            return os.path.join(folder, file)
    return None

# ---------- FETCH MULTI DATASET ----------
async def fetch_all():
    all_records = []

    for dataset in DATASETS:

        if dataset == "cig-2025":
            for month in MONTHS:
                url = build_url(dataset, month)
                zip_file = f"{dataset}_{month}.zip"
                downloaded = await download_zip(url, zip_file)
                if not downloaded:
                    continue
                json_path = extract_zip(downloaded)
                if not json_path:
                    continue
                with open(json_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            record["_dataset"] = dataset
                            all_records.append(record)
                        except:
                            continue
        else:
            url = build_url(dataset)
            zip_file = f"{dataset}.zip"
            downloaded = await download_zip(url, zip_file)
            if not downloaded:
                continue
            json_path = extract_zip(downloaded)
            if not json_path:
                continue
            with open(json_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        record["_dataset"] = dataset
                        all_records.append(record)
                    except:
                        continue

    print("TOTALE RECORD GREZZI:", len(all_records))
    return all_records

# ---------- INSERT ----------
async def insert(data):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/tenders?on_conflict=ocid",
            headers=HEADERS,
            json=data
        )
        print("INSERT STATUS:", r.status_code)
        if r.status_code >= 300:
            print("Errore:", r.text)

# ---------- MAIN ----------
async def main():
    records = await fetch_all()
    cleaned = []

    for r in records:
        if r["_dataset"] == "cig-2025":
            c = clean_cig(r)
        else:
            c = clean_generic(r)
        if c:
            cleaned.append(c)

    print("DOPO FILTRO (UTILI):", len(cleaned))
    if not cleaned:
        print("Nessun bando utile trovato")
        return

    # batch insert
    batch_size = 100
    for i in range(0, len(cleaned), batch_size):
        batch = cleaned[i:i+batch_size]
        await insert(batch)

    print(f"Totale inseriti: {len(cleaned)}")

if __name__ == "__main__":
    asyncio.run(main())
