#!/usr/bin/env python3
"""
Scraper para Blocos de Carnaval SP 2026
Fonte: CNN Brasil + dados existentes
Geocodificação: Nominatim (OpenStreetMap)
"""

import json
import os
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from unidecode import unidecode

# ============================================================
# CONFIG
# ============================================================

CNN_URL = "https://www.cnnbrasil.com.br/entretenimento/carnaval/confira-a-programacao-completa-do-carnaval-de-rua-2026-de-sao-paulo/"
EXISTING_DATA_FILE = Path(__file__).parent / "blocos.js"
OUTPUT_FILE = Path(__file__).parent / "blocos.js"
GEOCACHE_FILE = Path(__file__).parent / "geocache.json"

# São Paulo bounding box for geocoding validation
SP_LAT_MIN, SP_LAT_MAX = -24.01, -23.35
SP_LNG_MIN, SP_LNG_MAX = -46.85, -46.35

# Known megablocos and special types
MEGABLOCOS = {
    "ivete sangalo", "alceu valença", "monobloco", "bloco skol",
    "calvin harris", "michel teló", "bloco da pocah", "bloco da pabllo",
    "baianasystem", "jammil", "fitdance", "bloco 89",
    "bicho maluco beleza", "carnafacul", "quem pede pede",
    "bloco bem sertanejo", "galo da madrugada"
}

AFRO_KEYWORDS = {
    "afro", "afoxé", "afoxe", "quilombo", "macumba", "pretas",
    "quintal dos pretos", "africa viva", "carnablack"
}

ROCK_KEYWORDS = {
    "rock", "sabbath", "sargento pimenta", "raulzito", "heavy bloco",
    "mister rock", "rita seixas raul lee"
}

LGBTQIA_KEYWORDS = {
    "lgbtqia", "lgbt", "feminista", "c.u.n.t.", "dramas de sapatão",
    "diversidade"
}

# Day of week mapping (Portuguese)
DIAS_SEMANA = {
    0: "Segunda-feira",
    1: "Terça-feira",
    2: "Quarta-feira",
    3: "Quinta-feira",
    4: "Sexta-feira",
    5: "Sábado",
    6: "Domingo"
}

# ============================================================
# GEOCACHE
# ============================================================

def load_geocache():
    if GEOCACHE_FILE.exists():
        with open(GEOCACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_geocache(cache):
    with open(GEOCACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def cache_key(address):
    return hashlib.md5(address.lower().encode("utf-8")).hexdigest()

# ============================================================
# GEOCODING
# ============================================================

geolocator = Nominatim(user_agent="carnaval_sp_2026_scraper", timeout=10)

def geocode_address(address, bairro, cache):
    """Geocode an address with cache and fallback strategies."""
    key = cache_key(f"{address}, {bairro}")
    if key in cache:
        return cache[key]

    queries = [
        f"{address}, {bairro}, São Paulo, SP, Brasil",
        f"{bairro}, São Paulo, SP, Brasil",
    ]

    for query in queries:
        try:
            time.sleep(1.1)  # Nominatim rate limit
            location = geolocator.geocode(query, exactly_one=True)
            if location:
                lat, lng = location.latitude, location.longitude
                if SP_LAT_MIN <= lat <= SP_LAT_MAX and SP_LNG_MIN <= lng <= SP_LNG_MAX:
                    result = {"lat": round(lat, 6), "lng": round(lng, 6)}
                    cache[key] = result
                    return result
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocoding error for '{query}': {e}")
            time.sleep(2)

    # Fallback: use a general São Paulo center with slight randomization
    print(f"  WARNING: Could not geocode '{address}' in '{bairro}', using bairro center")
    return None

# ============================================================
# SCRAPING CNN BRASIL
# ============================================================

def scrape_cnn():
    """Scrape the CNN Brasil carnival article for bloco data."""
    print("Fetching CNN Brasil article...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
    }
    resp = requests.get(CNN_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the article body
    article = soup.find("div", class_="post__content") or soup.find("article") or soup.body

    blocos = []
    current_date = None

    # CNN structure: <p><strong>Dia DD/MM</strong></p> followed by <li> entries
    # Each <li>: "Nome do Bloco - Bairro - Xh às Yh;"
    for element in article.find_all(["p", "strong", "li"]):
        text = element.get_text(strip=True)
        if not text:
            continue

        # Check for date headers: "Dia 06/02" or "Dia 14/02"
        date_match = parse_date_header(text)
        if date_match:
            current_date = date_match
            continue

        # Only parse <li> elements as bloco entries
        if element.name == "li" and current_date:
            bloco = parse_bloco_entry(text, current_date)
            if bloco:
                blocos.append(bloco)

    print(f"Found {len(blocos)} blocos from CNN Brasil")
    return blocos

def parse_date_header(text):
    """Try to parse a date from a section header."""
    # Pattern: "Dia 06/02" or "Dia 14/02"
    match = re.search(r'[Dd]ia\s+(\d{1,2})/(\d{1,2})', text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        return f"{day:02d}/{month:02d}/2026"

    # Pattern: "6 de fevereiro"
    match = re.search(r'(\d{1,2})\s*de\s*fevereiro', text.lower())
    if match:
        day = int(match.group(1))
        return f"{day:02d}/02/2026"

    return None

def parse_bloco_entry(text, current_date):
    """Parse a bloco entry from a <li> element.
    Format: 'Nome do Bloco - Bairro - Xh às Yh;'
    Some names have dashes, so we parse from the time backwards.
    """
    # Clean up: remove trailing semicolons, periods
    text = text.rstrip(";.").strip()

    # Extract time pattern first (anchored at end)
    time_match = re.search(r'(\d{1,2})h?\s*(?:às|a)\s*(\d{1,2})h?\s*$', text)
    if not time_match:
        # Try single time: "14h"
        time_match = re.search(r'(\d{1,2})h\s*$', text)
        if time_match:
            horario = f"{int(time_match.group(1)):02d}:00"
            horario_fim = None
            before_time = text[:time_match.start()].rstrip(" -–—")
        else:
            return None
    else:
        horario = f"{int(time_match.group(1)):02d}:00"
        horario_fim = f"{int(time_match.group(2)):02d}:00"
        before_time = text[:time_match.start()].rstrip(" -–—")

    # Now split the remaining text into name and bairro
    # The bairro is the last segment before the time, separated by " - "
    # Use the LAST dash separator to split name from bairro
    # Handle cases where names have dashes (e.g., "Batuca- Bresser")
    dash_parts = re.split(r'\s*[-–—]\s*', before_time)

    if len(dash_parts) >= 2:
        bairro = dash_parts[-1].strip()
        nome = " - ".join(dash_parts[:-1]).strip() if len(dash_parts) > 2 else dash_parts[0].strip()
    else:
        return None

    # Validate
    if len(nome) < 2 or len(bairro) < 2:
        return None

    # Clean up name - remove leading/trailing whitespace and special chars
    nome = re.sub(r'\s+', ' ', nome).strip()
    bairro = re.sub(r'\s+', ' ', bairro).strip()

    # Determine type
    tipo = classify_bloco(nome)

    # Build day of week
    parts_date = current_date.split("/")
    dt = datetime(int(parts_date[2]), int(parts_date[1]), int(parts_date[0]))
    dia_semana = DIAS_SEMANA[dt.weekday()]

    return {
        "nome": nome,
        "bairro": bairro,
        "dia": current_date,
        "dia_semana": dia_semana,
        "horario": horario,
        "horario_fim": horario_fim,
        "concentracao": bairro,
        "tipo": tipo,
        "lat": None,
        "lng": None,
        "trajeto_aproximado": []
    }

def parse_time(time_str):
    """Parse time strings like '14h-18h', '14h às 18h', '14:00-18:00'."""
    time_str = time_str.strip().lower()

    # Pattern: 14h-18h or 14h - 18h
    match = re.search(r'(\d{1,2})h\s*[-àa]\s*(\d{1,2})h', time_str)
    if match:
        return f"{int(match.group(1)):02d}:00", f"{int(match.group(2)):02d}:00"

    # Pattern: 14h00 or 14:00
    match = re.search(r'(\d{1,2})[h:](\d{2})', time_str)
    if match:
        start = f"{int(match.group(1)):02d}:{match.group(2)}"
        return start, None

    # Pattern: just 14h
    match = re.search(r'(\d{1,2})h', time_str)
    if match:
        return f"{int(match.group(1)):02d}:00", None

    return None, None

def classify_bloco(nome):
    """Classify a bloco by type based on its name."""
    nome_lower = unidecode(nome.lower())

    for keyword in MEGABLOCOS:
        if keyword in nome_lower:
            return "Megabloco"

    for keyword in AFRO_KEYWORDS:
        if keyword in nome_lower:
            return "Afro"

    for keyword in ROCK_KEYWORDS:
        if keyword in nome_lower:
            return "Rock"

    for keyword in LGBTQIA_KEYWORDS:
        if keyword in nome_lower:
            return "LGBTQIA+"

    return "Tradicional"

# ============================================================
# LOAD EXISTING DATA
# ============================================================

def load_existing_blocos():
    """Load existing blocos from blocos.js."""
    if not EXISTING_DATA_FILE.exists():
        return {"blocos": [], "meta": {}}

    content = EXISTING_DATA_FILE.read_text(encoding="utf-8")
    # Remove "const BLOCOS_DATA = " prefix and trailing ";"
    json_str = re.sub(r'^const\s+BLOCOS_DATA\s*=\s*', '', content).rstrip().rstrip(';')
    try:
        data = json.loads(json_str)
        return data
    except json.JSONDecodeError as e:
        print(f"Error parsing existing data: {e}")
        return {"blocos": [], "meta": {}}

# ============================================================
# MERGE
# ============================================================

def normalize_name(name):
    """Normalize a bloco name for comparison."""
    return unidecode(name.lower().strip())

def merge_blocos(existing_data, scraped_blocos):
    """Merge scraped blocos with existing data, avoiding duplicates."""
    existing_blocos = existing_data.get("blocos", [])
    existing_names = {normalize_name(b["nome"]) for b in existing_blocos}

    new_blocos = []
    updated = 0
    added = 0

    # Keep all existing blocos (they have vetted coordinates and routes)
    for bloco in existing_blocos:
        new_blocos.append(bloco)

    # Add scraped blocos that don't exist yet
    for bloco in scraped_blocos:
        norm_name = normalize_name(bloco["nome"])

        # Check for fuzzy match with existing
        is_duplicate = False
        for existing_name in existing_names:
            if norm_name == existing_name:
                is_duplicate = True
                break
            # Check substring match for very close names
            if len(norm_name) > 8 and (norm_name in existing_name or existing_name in norm_name):
                is_duplicate = True
                break

        if not is_duplicate and bloco["lat"] is not None:
            new_blocos.append(bloco)
            added += 1

    print(f"Merge: {len(existing_blocos)} existing + {added} new = {len(new_blocos)} total")
    return new_blocos

# ============================================================
# OUTPUT
# ============================================================

def write_blocos_js(blocos, existing_data):
    """Write the final blocos.js file."""
    # Sort by date then name
    blocos.sort(key=lambda b: (b["dia"], b["nome"]))

    # Count unique dates
    dates = sorted(set(b["dia"] for b in blocos))

    output_data = {
        "evento": existing_data.get("evento", "Carnaval de Rua de São Paulo 2026"),
        "fonte": "Prefeitura de SP (carnavalsp.com), CNN Brasil, Brasilturis, Billboard Brasil, TMC",
        "total_blocos_cidade": f"{len(blocos)}+",
        "periodo": existing_data.get("periodo", {
            "pre_carnaval": "07-08/02/2026",
            "carnaval": "14-17/02/2026",
            "pos_carnaval": "21-22/02/2026"
        }),
        "blocos": blocos,
        "notas": existing_data.get("notas", {
            "coordenadas": "Coordenadas aproximadas baseadas nos endereços de concentração",
            "trajeto_aproximado": "Trajetos disponíveis apenas para blocos com rota confirmada",
            "fonte_geocodificacao": "OpenStreetMap Nominatim"
        })
    }

    js_content = "const BLOCOS_DATA = " + json.dumps(output_data, ensure_ascii=False, indent=2) + ";\n"
    OUTPUT_FILE.write_text(js_content, encoding="utf-8")
    print(f"\nWrote {len(blocos)} blocos to {OUTPUT_FILE}")
    print(f"Dates covered: {', '.join(dates)}")

    # Type breakdown
    types = {}
    for b in blocos:
        types[b["tipo"]] = types.get(b["tipo"], 0) + 1
    print("Type breakdown:")
    for t, c in sorted(types.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")

# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("Carnaval SP 2026 - Bloco Scraper")
    print("=" * 60)

    # Load existing data
    print("\n1. Loading existing data...")
    existing_data = load_existing_blocos()
    print(f"   Found {len(existing_data.get('blocos', []))} existing blocos")

    # Scrape CNN Brasil
    print("\n2. Scraping CNN Brasil...")
    scraped = scrape_cnn()

    if not scraped:
        print("   No blocos found from scraping. Check the article URL.")
        return

    # Load geocache
    print("\n3. Geocoding addresses...")
    cache = load_geocache()
    geocoded = 0
    failed = 0

    for i, bloco in enumerate(scraped):
        if bloco["lat"] is not None:
            continue

        result = geocode_address(bloco["concentracao"], bloco["bairro"], cache)
        if result:
            bloco["lat"] = result["lat"]
            bloco["lng"] = result["lng"]
            geocoded += 1
        else:
            failed += 1

        # Save cache periodically
        if (i + 1) % 20 == 0:
            save_geocache(cache)
            print(f"   Progress: {i+1}/{len(scraped)} (geocoded: {geocoded}, failed: {failed})")

    save_geocache(cache)
    print(f"   Geocoded: {geocoded}, Failed: {failed}, Cached: {len(cache)}")

    # Filter out blocos without coordinates
    scraped_with_coords = [b for b in scraped if b["lat"] is not None]
    print(f"   Blocos with valid coordinates: {len(scraped_with_coords)}")

    # Merge
    print("\n4. Merging with existing data...")
    merged = merge_blocos(existing_data, scraped_with_coords)

    # Write output
    print("\n5. Writing output...")
    write_blocos_js(merged, existing_data)

    print("\nDone!")

if __name__ == "__main__":
    main()
