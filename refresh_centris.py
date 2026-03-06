#!/usr/bin/env python3
"""
Incremental Centris Commercial Listings Refresh

Designed for GitHub Actions. Runs the full GetMarkers + GetMarkerInfo pipeline
to discover current listings, but skips detail page fetches for listings already
in the cache. Only NEW listings get their detail pages fetched.

Savings: on a typical run with ~5 new listings out of ~660, this skips ~655
detail page fetches (each 0.5s + network), cutting runtime from ~12 min to ~6 min.

Usage:
    python refresh_centris.py              # Normal incremental refresh
    python refresh_centris.py --dry-run    # Show diff without fetching details
    python refresh_centris.py --full       # Force full detail refresh
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
CACHE_FILE = SCRIPT_DIR / 'commercial_latest.json'
INDEX_FILE = SCRIPT_DIR / 'index.html'

BASE_URL = 'https://www.centris.ca'
API_MARKERS = f'{BASE_URL}/api/property/map/GetMarkers'
API_MARKER_INFO = f'{BASE_URL}/property/GetMarkerInfo'

API_DELAY = 0.3
DETAIL_DELAY = 0.5

COMMERCIAL_CATEGORIES = {'batisse commerciale', 'local commercial', 'commerce'}

HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/131.0.0.0 Safari/537.36'
    ),
    'Origin': BASE_URL,
    'Referer': f'{BASE_URL}/fr/propriete-commerciale~a-louer',
}

# All areas used in the full search
AREAS = {
    'ahuntsic': {
        'label': 'Ahuntsic-Cartierville (Montreal)',
        'bounds': {'NorthEast': {'Lat': 45.575, 'Lng': -73.635},
                   'SouthWest': {'Lat': 45.530, 'Lng': -73.730}},
    },
    'laval': {
        'label': 'Laval',
        'bounds': {'NorthEast': {'Lat': 45.640, 'Lng': -73.580},
                   'SouthWest': {'Lat': 45.510, 'Lng': -73.900}},
    },
    'st-eustache': {
        'label': 'Saint-Eustache',
        'bounds': {'NorthEast': {'Lat': 45.590, 'Lng': -73.850},
                   'SouthWest': {'Lat': 45.530, 'Lng': -73.930}},
    },
    'blainville': {
        'label': 'Blainville',
        'bounds': {'NorthEast': {'Lat': 45.700, 'Lng': -73.830},
                   'SouthWest': {'Lat': 45.640, 'Lng': -73.920}},
    },
    'vaudreuil': {
        'label': 'Vaudreuil-Dorion',
        'bounds': {'NorthEast': {'Lat': 45.430, 'Lng': -73.950},
                   'SouthWest': {'Lat': 45.370, 'Lng': -74.100}},
    },
    'montreal': {
        'label': 'Montreal (Ile)',
        'bounds': {'NorthEast': {'Lat': 45.590, 'Lng': -73.470},
                   'SouthWest': {'Lat': 45.410, 'Lng': -73.750}},
    },
    'rive-nord': {
        'label': 'Rive-Nord (Laval + Laurentides)',
        'bounds': {'NorthEast': {'Lat': 45.720, 'Lng': -73.580},
                   'SouthWest': {'Lat': 45.510, 'Lng': -74.100}},
    },
    'brossard': {
        'label': 'Brossard',
        'bounds': {'NorthEast': {'Lat': 45.512, 'Lng': -73.413},
                   'SouthWest': {'Lat': 45.412, 'Lng': -73.523}},
    },
    'longueuil': {
        'label': 'Longueuil',
        'bounds': {'NorthEast': {'Lat': 45.566, 'Lng': -73.473},
                   'SouthWest': {'Lat': 45.496, 'Lng': -73.563}},
    },
    'boucherville': {
        'label': 'Boucherville',
        'bounds': {'NorthEast': {'Lat': 45.631, 'Lng': -73.381},
                   'SouthWest': {'Lat': 45.551, 'Lng': -73.491}},
    },
    'varennes': {
        'label': 'Varennes',
        'bounds': {'NorthEast': {'Lat': 45.728, 'Lng': -73.378},
                   'SouthWest': {'Lat': 45.638, 'Lng': -73.488}},
    },
    'st-lambert': {
        'label': 'Saint-Lambert',
        'bounds': {'NorthEast': {'Lat': 45.520, 'Lng': -73.485},
                   'SouthWest': {'Lat': 45.480, 'Lng': -73.535}},
    },
    'st-hubert': {
        'label': 'Saint-Hubert',
        'bounds': {'NorthEast': {'Lat': 45.530, 'Lng': -73.365},
                   'SouthWest': {'Lat': 45.450, 'Lng': -73.465}},
    },
    'la-prairie': {
        'label': 'La Prairie',
        'bounds': {'NorthEast': {'Lat': 45.445, 'Lng': -73.458},
                   'SouthWest': {'Lat': 45.385, 'Lng': -73.538}},
    },
    'candiac': {
        'label': 'Candiac',
        'bounds': {'NorthEast': {'Lat': 45.407, 'Lng': -73.490},
                   'SouthWest': {'Lat': 45.357, 'Lng': -73.550}},
    },
    'st-constant': {
        'label': 'Saint-Constant',
        'bounds': {'NorthEast': {'Lat': 45.400, 'Lng': -73.525},
                   'SouthWest': {'Lat': 45.340, 'Lng': -73.605}},
    },
    'ste-catherine': {
        'label': 'Sainte-Catherine',
        'bounds': {'NorthEast': {'Lat': 45.420, 'Lng': -73.555},
                   'SouthWest': {'Lat': 45.380, 'Lng': -73.605}},
    },
    'delson': {
        'label': 'Delson',
        'bounds': {'NorthEast': {'Lat': 45.390, 'Lng': -73.525},
                   'SouthWest': {'Lat': 45.354, 'Lng': -73.575}},
    },
    'chateauguay': {
        'label': 'Chateauguay',
        'bounds': {'NorthEast': {'Lat': 45.415, 'Lng': -73.700},
                   'SouthWest': {'Lat': 45.345, 'Lng': -73.800}},
    },
    'rive-sud-proche': {
        'label': 'Rive-Sud proche (Longueuil-Chateauguay)',
        'bounds': {'NorthEast': {'Lat': 45.728, 'Lng': -73.365},
                   'SouthWest': {'Lat': 45.340, 'Lng': -73.800}},
    },
    'st-jean': {
        'label': 'Saint-Jean-sur-Richelieu',
        'bounds': {'NorthEast': {'Lat': 45.360, 'Lng': -73.210},
                   'SouthWest': {'Lat': 45.270, 'Lng': -73.320}},
    },
    'chambly': {
        'label': 'Chambly / Richelieu',
        'bounds': {'NorthEast': {'Lat': 45.480, 'Lng': -73.250},
                   'SouthWest': {'Lat': 45.420, 'Lng': -73.330}},
    },
    'beloeil': {
        'label': 'Beloeil / Mont-Saint-Hilaire / Otterburn Park',
        'bounds': {'NorthEast': {'Lat': 45.600, 'Lng': -73.150},
                   'SouthWest': {'Lat': 45.520, 'Lng': -73.260}},
    },
    'st-basile': {
        'label': 'Saint-Basile-le-Grand',
        'bounds': {'NorthEast': {'Lat': 45.560, 'Lng': -73.260},
                   'SouthWest': {'Lat': 45.510, 'Lng': -73.320}},
    },
    'st-bruno': {
        'label': 'Saint-Bruno-de-Montarville',
        'bounds': {'NorthEast': {'Lat': 45.560, 'Lng': -73.310},
                   'SouthWest': {'Lat': 45.500, 'Lng': -73.390}},
    },
    'st-julie': {
        'label': 'Sainte-Julie',
        'bounds': {'NorthEast': {'Lat': 45.610, 'Lng': -73.280},
                   'SouthWest': {'Lat': 45.560, 'Lng': -73.360}},
    },
    'st-hyacinthe': {
        'label': 'Saint-Hyacinthe',
        'bounds': {'NorthEast': {'Lat': 45.660, 'Lng': -72.880},
                   'SouthWest': {'Lat': 45.570, 'Lng': -72.990}},
    },
    'granby': {
        'label': 'Granby / Bromont / Cowansville',
        'bounds': {'NorthEast': {'Lat': 45.450, 'Lng': -72.630},
                   'SouthWest': {'Lat': 45.330, 'Lng': -72.800}},
    },
    'drummondville': {
        'label': 'Drummondville',
        'bounds': {'NorthEast': {'Lat': 45.920, 'Lng': -72.420},
                   'SouthWest': {'Lat': 45.830, 'Lng': -72.550}},
    },
    'sorel': {
        'label': 'Sorel-Tracy',
        'bounds': {'NorthEast': {'Lat': 46.070, 'Lng': -72.990},
                   'SouthWest': {'Lat': 45.990, 'Lng': -73.120}},
    },
    'valleyfield': {
        'label': 'Salaberry-de-Valleyfield',
        'bounds': {'NorthEast': {'Lat': 45.280, 'Lng': -74.060},
                   'SouthWest': {'Lat': 45.220, 'Lng': -74.170}},
    },
    'mercier': {
        'label': 'Mercier / Beauharnois',
        'bounds': {'NorthEast': {'Lat': 45.360, 'Lng': -73.700},
                   'SouthWest': {'Lat': 45.280, 'Lng': -73.810}},
    },
    'rive-sud': {
        'label': 'Rive-Sud (territoire elargi)',
        'bounds': {'NorthEast': {'Lat': 46.10, 'Lng': -72.35},
                   'SouthWest': {'Lat': 45.25, 'Lng': -74.15}},
    },
}

# Areas to scan: Rive-Sud sub-areas (matching the current viewer data)
# To add clinic areas, append: 'ahuntsic', 'laval', 'st-eustache', 'blainville', 'vaudreuil'
SEARCH_AREAS = [
    'boucherville', 'brossard', 'longueuil', 'st-hubert', 'st-lambert',
    'la-prairie', 'candiac', 'st-constant', 'ste-catherine', 'chateauguay',
]
SELLING_TYPES = ['Rent', 'Sale']


def build_query(selling_type: str) -> Dict:
    """Build the Centris query object."""
    fields = [
        {"fieldId": "Category", "value": "Commercial",
         "fieldConditionId": "", "valueConditionId": ""},
        {"fieldId": "SellingType", "value": selling_type,
         "fieldConditionId": "", "valueConditionId": ""},
    ]
    price_field = "RentPrice" if selling_type == "Rent" else "SalePrice"
    condition = "ForRent" if selling_type == "Rent" else "ForSale"
    fields.extend([
        {"fieldId": price_field, "value": 0,
         "fieldConditionId": condition, "valueConditionId": ""},
        {"fieldId": price_field, "value": 999999999999,
         "fieldConditionId": condition, "valueConditionId": ""},
    ])
    return {
        "SearchName": "",
        "UseGeographyShapes": 0,
        "Filters": [],
        "FieldsValues": fields,
        "BrokerCode": None,
        "OfficeKey": None,
    }


def get_markers(session: requests.Session, bounds: Dict,
                query: Dict, zoom: int = 14) -> List[Dict]:
    """Fetch map markers in a bounding box."""
    payload = {
        "zoomLevel": zoom,
        "mapBounds": bounds,
        "mode": "Result",
        "sort": "None",
        "sortSeed": int(time.time()),
        "query": query,
        "region": "Quebec",
        "openListing": None,
    }
    resp = session.post(API_MARKERS, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get('d', {}).get('Result', {}).get('Markers', [])


def _parse_marker_html(html: str, marker_data: Dict) -> Optional[Dict]:
    """Parse HTML from GetMarkerInfo into a structured listing dict."""
    if not html:
        return None

    mls_match = re.search(r'content="(\d{5,10})"\s*itemprop="sku"', html)
    if not mls_match:
        mls_match = re.search(r'itemprop="sku"\s*content="(\d{5,10})"', html)
    mls = mls_match.group(1) if mls_match else marker_data.get('NoMls')
    if not mls:
        return None

    name_match = re.search(r'content="([^"]+)"\s*itemprop="name"', html)
    if not name_match:
        name_match = re.search(r'itemprop="name"\s*content="([^"]+)"', html)
    title = unescape(name_match.group(1)) if name_match else ''
    title = re.sub(r'\s*-\s*Centris\.ca$', '', title)

    link_match = re.search(r'href="(/fr/[^"]+)"', html)
    listing_url = f'{BASE_URL}{link_match.group(1)}' if link_match else ''

    category = ''
    if link_match:
        url_path = link_match.group(1)
        cat_match = re.search(r'/fr/([^~]+)~', url_path)
        if cat_match:
            category = cat_match.group(1).replace('-', ' ').title()

    price_match = re.search(r'<span class="price">(.*?)</span>', html, re.DOTALL)
    price_display = unescape(price_match.group(1)).strip() if price_match else ''

    price_val_match = re.search(r'itemprop="price"\s*content="([^"]+)"', html)
    if not price_val_match:
        price_val_match = re.search(r'content="([^"]+)"\s*itemprop="price"', html)
    price_value = price_val_match.group(1) if price_val_match else None

    addr_parts = []
    if title:
        parts = title.split(',')
        if len(parts) >= 2:
            addr_parts = [p.strip() for p in parts[1:-1]]

    photo_match = re.search(r'>(\d+)<i class="fa[rl]? fa-camera"', html)
    photo_count = int(photo_match.group(1)) if photo_match else 0

    pos = marker_data.get('Position', {})

    return {
        'mls_number': str(mls),
        'title': title,
        'category': category,
        'address': ', '.join(addr_parts),
        'city': addr_parts[0] if addr_parts else '',
        'price_value': price_value,
        'price_display': price_display,
        'latitude': pos.get('Lat'),
        'longitude': pos.get('Lng'),
        'photo_count': photo_count,
        'listing_url': listing_url,
        'sqft': None,
        'description': None,
        'broker': None,
    }


def get_marker_info(session: requests.Session, marker: Dict,
                    bounds: Dict, query: Dict) -> Optional[Dict]:
    """Fetch listing preview from GetMarkerInfo."""
    pos = marker['Position']
    payload = {
        "pageIndex": 0,
        "zoomLevel": 18,
        "latitude": pos['Lat'],
        "longitude": pos['Lng'],
        "mapBounds": bounds,
        "geoHash": marker.get('GeoHash', ''),
        "sortSeed": int(time.time()),
        "sort": "None",
        "mode": "Result",
        "query": query,
        "region": "Quebec",
    }
    resp = session.post(API_MARKER_INFO, json=payload, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    result = data.get('d', {}).get('Result', {})
    return _parse_marker_html(result.get('Html', ''), result.get('Marker', {}))


def get_cluster_listings(session: requests.Session, marker: Dict,
                         bounds: Dict, query: Dict) -> List[Dict]:
    """Iterate through all listings in a cluster using pageIndex."""
    pos = marker['Position']
    count = marker.get('PointsCount', 1)
    listings = []

    for page_idx in range(count):
        payload = {
            "pageIndex": page_idx,
            "zoomLevel": 18,
            "latitude": pos['Lat'],
            "longitude": pos['Lng'],
            "mapBounds": bounds,
            "geoHash": marker.get('GeoHash', ''),
            "sortSeed": int(time.time()),
            "sort": "None",
            "mode": "Result",
            "query": query,
            "region": "Quebec",
        }
        try:
            resp = session.post(API_MARKER_INFO, json=payload,
                                headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            result = data.get('d', {}).get('Result', {})
            listing = _parse_marker_html(result.get('Html', ''), result.get('Marker', {}))
            if listing:
                listings.append(listing)
            time.sleep(API_DELAY)
        except Exception as e:
            logger.warning(f'  GetMarkerInfo page {page_idx} failed: {e}')
            break

    return listings


def fetch_listing_detail(session: requests.Session, listing: Dict) -> Dict:
    """Fetch detail page for sqft, description, broker."""
    url = listing.get('listing_url')
    if not url:
        return listing

    try:
        resp = session.get(url, headers={
            'User-Agent': HEADERS['User-Agent'],
            'Accept-Language': 'fr-CA,fr;q=0.9',
        }, timeout=15)
        resp.raise_for_status()
        text = resp.text

        price_match = re.search(r'itemprop="price"\s*content="([^"]+)"', text)
        if price_match and not listing.get('price_value'):
            listing['price_value'] = price_match.group(1)

        desc_match = re.search(
            r'<meta\s+(?:property="og:description"|name="description")'
            r'\s+content="([^"]+)"', text, re.IGNORECASE
        )
        if desc_match:
            listing['description'] = unescape(desc_match.group(1))[:300]

        for pattern in [
            r'Superficie commerciale disponible\s*</(?:td|div|span)>\s*'
            r'<(?:td|div|span)[^>]*>\s*([\d\s\xa0]+)\s*pc',
            r'Superficie du b.timent\s*</(?:td|div|span)>\s*'
            r'<(?:td|div|span)[^>]*>\s*([\d\s\xa0]+)\s*pc',
            r'([\d\s\xa0]+)\s*(?:pieds?\s*carr|pi2|pi\xb2|pc)',
        ]:
            sqft_match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if sqft_match:
                sqft_str = (sqft_match.group(1)
                            .replace(' ', '')
                            .replace('\xa0', '')
                            .strip())
                try:
                    val = int(sqft_str)
                    if 50 <= val <= 1_000_000:
                        listing['sqft'] = val
                        break
                except ValueError:
                    pass

        broker_match = re.search(r'itemprop="legalName"\s*content="([^"]+)"', text)
        if not broker_match:
            broker_match = re.search(r'content="([^"]+)"\s*itemprop="legalName"', text)
        if broker_match:
            listing['broker'] = unescape(broker_match.group(1))

    except Exception as e:
        logger.warning(f'  Detail fetch failed for MLS {listing["mls_number"]}: {e}')

    return listing


def search_area(session: requests.Session, area_key: str,
                selling_type: str, cache_lookup: Dict[str, Dict],
                fetch_all_details: bool = False) -> Tuple[List[Dict], int, int, int]:
    """
    Search all commercial listings in an area.
    Returns (listings, new_count, cached_count, api_calls).
    For listings found in cache_lookup, reuses cached data (skips detail fetch).
    """
    area = AREAS[area_key]
    bounds = area['bounds']
    query = build_query(selling_type)
    tx_label = 'location' if selling_type == 'Rent' else 'vente'

    logger.info(f'  {area["label"]} ({tx_label})...')

    markers = get_markers(session, bounds, query, zoom=14)
    api_calls = 1
    total_points = sum(m.get('PointsCount', 0) for m in markers)
    logger.info(f'    {len(markers)} markers, ~{total_points} listings')
    time.sleep(API_DELAY)

    if not markers:
        return [], 0, 0, api_calls

    all_listings = []
    seen_mls = set()
    new_count = 0
    cached_count = 0

    for marker in markers:
        count = marker.get('PointsCount', 0)
        mls = marker.get('NoMls')

        if count == 0:
            continue

        if count == 1 or mls:
            listing = get_marker_info(session, marker, bounds, query)
            api_calls += 1
            if listing and listing.get('category', '').lower() not in COMMERCIAL_CATEGORIES:
                time.sleep(API_DELAY)
                continue
            if listing and listing['mls_number'] not in seen_mls:
                seen_mls.add(listing['mls_number'])

                # Check cache
                if listing['mls_number'] in cache_lookup and not fetch_all_details:
                    cached = cache_lookup[listing['mls_number']]
                    # Update position from fresh marker data (in case it moved)
                    cached['latitude'] = listing.get('latitude') or cached.get('latitude')
                    cached['longitude'] = listing.get('longitude') or cached.get('longitude')
                    # Update price from fresh marker data
                    if listing.get('price_value'):
                        cached['price_value'] = listing['price_value']
                    if listing.get('price_display'):
                        cached['price_display'] = listing['price_display']
                    all_listings.append(cached)
                    cached_count += 1
                else:
                    listing['area'] = area_key
                    listing['area_label'] = area['label']
                    listing['transaction_type'] = 'lease' if selling_type == 'Rent' else 'sale'
                    fetch_listing_detail(session, listing)
                    api_calls += 1
                    time.sleep(DETAIL_DELAY)
                    all_listings.append(listing)
                    new_count += 1

            time.sleep(API_DELAY)
        else:
            cluster_listings = get_cluster_listings(session, marker, bounds, query)
            api_calls += count  # one call per page

            for listing in cluster_listings:
                if listing and listing.get('category', '').lower() not in COMMERCIAL_CATEGORIES:
                    continue
                if listing and listing['mls_number'] not in seen_mls:
                    seen_mls.add(listing['mls_number'])

                    if listing['mls_number'] in cache_lookup and not fetch_all_details:
                        cached = cache_lookup[listing['mls_number']]
                        cached['latitude'] = listing.get('latitude') or cached.get('latitude')
                        cached['longitude'] = listing.get('longitude') or cached.get('longitude')
                        if listing.get('price_value'):
                            cached['price_value'] = listing['price_value']
                        if listing.get('price_display'):
                            cached['price_display'] = listing['price_display']
                        all_listings.append(cached)
                        cached_count += 1
                    else:
                        listing['area'] = area_key
                        listing['area_label'] = area['label']
                        listing['transaction_type'] = 'lease' if selling_type == 'Rent' else 'sale'
                        fetch_listing_detail(session, listing)
                        api_calls += 1
                        time.sleep(DETAIL_DELAY)
                        all_listings.append(listing)
                        new_count += 1

    logger.info(f'    {len(all_listings)} listings ({new_count} new, {cached_count} cached)')
    return all_listings, new_count, cached_count, api_calls


def update_index_html(listings: List[Dict]) -> bool:
    """Update index.html by replacing only the DATA variable and date."""
    if not INDEX_FILE.exists():
        logger.error(f'index.html not found at {INDEX_FILE}')
        return False

    html = INDEX_FILE.read_text(encoding='utf-8')

    new_data = json.dumps({
        'search_date': datetime.now().isoformat(),
        'total_listings': len(listings),
        'source': 'centris.ca (API)',
        'listings': listings,
    }, ensure_ascii=False)

    # Replace const DATA = {...};
    new_html = re.sub(
        r'const DATA = \{.*?\};',
        f'const DATA = {new_data};',
        html,
        count=1,
        flags=re.DOTALL,
    )

    # Replace date display
    new_date = datetime.now().strftime('%Y-%m-%d %H:%M')
    new_html = re.sub(
        r'Mise a jour: [^<]+',
        f'Mise a jour: {new_date}',
        new_html,
        count=1,
    )

    INDEX_FILE.write_text(new_html, encoding='utf-8')
    logger.info(f'Updated index.html ({len(listings)} listings, date: {new_date})')
    return True


def main():
    parser = argparse.ArgumentParser(description='Incremental Centris refresh')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change without fetching details')
    parser.add_argument('--full', action='store_true',
                        help='Force full detail refresh (ignore cache)')
    args = parser.parse_args()

    session = requests.Session()

    # Load cache
    cache_lookup = {}
    if CACHE_FILE.exists() and not args.full:
        with open(CACHE_FILE, encoding='utf-8') as f:
            cache = json.load(f)
        for listing in cache.get('listings', []):
            cache_lookup[listing['mls_number']] = listing
        logger.info(f'Loaded cache: {len(cache_lookup)} listings from {cache.get("search_date", "?")}')
    else:
        logger.info('No cache or --full mode: all listings will get detail pages fetched')

    if args.dry_run:
        logger.info('[DRY RUN] Would scan all areas and show diff. Exiting.')
        logger.info(f'Cache has {len(cache_lookup)} listings. Run without --dry-run to refresh.')
        return

    # Search all areas
    logger.info(f'\nSearching {len(SEARCH_AREAS)} areas x {len(SELLING_TYPES)} transaction types...')
    all_listings = []
    seen_mls = set()
    total_new = 0
    total_cached = 0
    total_api_calls = 0

    for area_key in SEARCH_AREAS:
        for selling_type in SELLING_TYPES:
            listings, new_count, cached_count, calls = search_area(
                session, area_key, selling_type, cache_lookup,
                fetch_all_details=args.full,
            )
            for lst in listings:
                if lst['mls_number'] not in seen_mls:
                    seen_mls.add(lst['mls_number'])
                    all_listings.append(lst)
            total_new += new_count
            total_cached += cached_count
            total_api_calls += calls

    # Identify delisted
    cached_mls = set(cache_lookup.keys())
    current_mls = set(lst['mls_number'] for lst in all_listings)
    delisted = cached_mls - current_mls

    logger.info(f'\nDedup: {len(all_listings)} unique listings')
    if delisted:
        logger.info(f'Delisted ({len(delisted)}): {sorted(delisted)[:10]}{"..." if len(delisted) > 10 else ""}')

    # Save cache
    output = {
        'search_date': datetime.now().isoformat(),
        'total_listings': len(all_listings),
        'source': 'centris.ca (API)',
        'listings': all_listings,
    }
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f'Saved: {CACHE_FILE}')

    # Update index.html
    update_index_html(all_listings)

    # Summary
    logger.info(f'\n{"="*60}')
    logger.info(f'SUMMARY')
    logger.info(f'{"="*60}')
    logger.info(f'  New listings:       {total_new}')
    logger.info(f'  Cached (reused):    {total_cached}')
    logger.info(f'  Delisted:           {len(delisted)}')
    logger.info(f'  Total listings:     {len(all_listings)}')
    logger.info(f'  Total API calls:    {total_api_calls}')
    logger.info(f'  Detail pages saved: ~{total_cached} (skipped for cached listings)')
    logger.info(f'{"="*60}')


if __name__ == '__main__':
    main()
