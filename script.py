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

# --------- UTILS ---------

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
    if not amount or amount < 50000:
        return None

    return {
        "ocid": ocid,
        "title": tender.get("title"),
        "amount": amount,
        "published_date": parse_date(release.get("date")),
        "raw": release
    }


# --------- FETCH SICURO ---------

async def fetch():
    url = "https://dati.anticorruzione.it/opendata/ocds/release-package.json"

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(url, timeout=60)
            print("STATUS:", r.status_code)

            if r.status_code != 200:
                print("Errore API:", r.status_code)
                return {}

            # Controllo se la risposta è vuota
            if not r.text.strip():
                print("Risposta vuota")
                return {}

            try:
                data = r.json()
            except Exception as e:
                print("JSONDecodeError:", e)
                print("Prima parte del testo:", r.text[:500])
                return {}

            # Per test: prendi solo prime 10 releases
            releases = data.get("releases", [])[:10]
            print("DEBUG releases:", len(releases))
            return {"releases": releases}

        except Exception as e:
            print("Errore fetch:", str(e))
            return {}


# --------- INSERT ---------

async def insert(data):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/tenders",
            headers=HEADERS,
            json=data
        )

        print("INSERT STATUS:", r.status_code)

        if r.status_code >= 300:
            print("Errore inserimento:", r.text)


# --------- MAIN ---------

async def main():
    data = await fetch()

    if not data:
        print("Nessun dato ricevuto")
        return

    releases = data.get("releases", [])

    cleaned = []
    for r in releases:
        c = clean_release(r)
        if c:
            cleaned.append(c)

    if not cleaned:
        print("Nessun dato valido dopo filtro")
        return

    await insert(cleaned)
    print(f"Inseriti {len(cleaned)} record")


if __name__ == "__main__":
    asyncio.run(main())
