import asyncio
import httpx
import os
import zipfile
import io
import json
from datetime import datetime

# ---------------- CONFIG ----------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # usare service key per insert
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# URL dei dataset delta o mese corrente
DATASETS = {
    "cig_delta": "https://dati.anticorruzione.it/opendata/download/dataset/CIG%20aggiornamenti%20delta/filesystem/CIG_aggiornamenti_delta_json.zip",
    "subappalti": "https://dati.anticorruzione.it/opendata/download/dataset/subappalti/filesystem/subappalti_json.zip"
}

BATCH_SIZE = 50  # batch insert supabase
IMPORTO_MINIMO = 50000

# ---------------- UTILS ----------------

def parse_date(date_str):
    """Pulisce e normalizza le date"""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None

def clean_record(record):
    """Filtra record validi e normalize"""
    # ogni dataset può avere chiave diversa, uniformiamo con ocid
    ocid = record.get("cig") or record.get("ocid")
    if not ocid:
        return None

    # stato attivo
    stato = record.get("stato") or record.get("esito")
    if stato and stato.lower() not in ["attivo", "aggiudicata"]:
        return None

    # importo
    importo = record.get("importo_lotto") or record.get("importo_complessivo_gara") or 0
    try:
        importo = float(importo)
    except:
        importo = 0
    if importo < IMPORTO_MINIMO:
        return None

    return {
        "ocid": ocid,
        "title": record.get("oggetto_gara") or record.get("oggetto_lotto"),
        "amount": importo,
        "published_date": parse_date(record.get("data_pubblicazione")),
        "raw": record
    }

# ---------------- FETCH & PARSE ----------------

async def fetch_dataset(name, url):
    """Scarica e processa dataset ZIP in streaming"""
    print(f"Scaricando dataset {name} da {url}")
    async with httpx.AsyncClient(timeout=600) as client:
        r = await client.get(url)
        if r.status_code != 200:
            print(f"Errore download {name}: {r.status_code}")
            return []

        print(f"DOWNLOAD STATUS: {r.status_code}")
        data_list = []

        # Apri ZIP in memoria
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for filename in z.namelist():
                if not filename.endswith(".json"):
                    continue
                with z.open(filename) as f:
                    for line in f:
                        try:
                            record = json.loads(line)
                            cleaned = clean_record(record)
                            if cleaned:
                                data_list.append(cleaned)
                        except Exception as e:
                            print(f"Errore parsing record: {e}")
        print(f"Totale record validi da {name}: {len(data_list)}")
        return data_list

# ---------------- INSERT ----------------

async def insert_supabase(records):
    """Inserisce batch di record in Supabase"""
    if not records:
        print("Nessun record da inserire")
        return

    async with httpx.AsyncClient() as client:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            r = await client.post(
                f"{SUPABASE_URL}/rest/v1/tenders",
                headers=HEADERS,
                json=batch
            )
            if r.status_code >= 300:
                print(f"Errore inserimento batch: {r.status_code} {r.text}")
            else:
                print(f"Batch inserito ({len(batch)} record)")

# ---------------- MAIN ----------------

async def main():
    all_records = []
    for name, url in DATASETS.items():
        records = await fetch_dataset(name, url)
        all_records.extend(records)

    print(f"Totale record dopo tutti i dataset: {len(all_records)}")
    await insert_supabase(all_records)

if __name__ == "__main__":
    asyncio.run(main())
