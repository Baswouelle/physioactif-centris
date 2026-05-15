#!/usr/bin/env python3
"""
Email alert pour les nouveaux listings Centris.

Lu par refresh_centris.py: si new_listings.json existe, envoie un email
HTML aux destinataires via Gmail API (OAuth refresh token).

Variables d'environnement requises (GitHub Secrets):
    GMAIL_CLIENT_ID
    GMAIL_CLIENT_SECRET
    GMAIL_REFRESH_TOKEN

Exit 0 silencieux si new_listings.json absent ou vide.
"""

import base64
import json
import logging
import os
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
NEW_LISTINGS_FILE = SCRIPT_DIR / 'new_listings.json'

SENDER = 'ariel@physioactif.com'
RECIPIENTS = ['ariel@physioactif.com', 'sylvain@physioactif.com']
MAP_URL = 'https://baswouelle.github.io/physioactif-centris/'

TOKEN_URI = 'https://oauth2.googleapis.com/token'
GMAIL_SEND_URL = 'https://gmail.googleapis.com/gmail/v1/users/me/messages/send'


def get_access_token() -> str:
    client_id = os.environ['GMAIL_CLIENT_ID']
    client_secret = os.environ['GMAIL_CLIENT_SECRET']
    refresh_token = os.environ['GMAIL_REFRESH_TOKEN']
    resp = requests.post(TOKEN_URI, data={
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token',
    }, timeout=30)
    resp.raise_for_status()
    return resp.json()['access_token']


def fmt_price(listing: dict) -> str:
    display = listing.get('price_display')
    if display:
        return display
    pv = listing.get('price_value')
    if pv:
        try:
            return f"{float(pv):,.0f} $".replace(',', ' ')
        except (TypeError, ValueError):
            return str(pv)
    return '-'


def fmt_sqft(listing: dict) -> str:
    sqft = listing.get('sqft')
    if sqft:
        return f"{sqft:,} pi²".replace(',', ' ')
    return '-'


def build_html(new_listings: list, search_date: str) -> str:
    date_str = search_date[:10] if search_date else datetime.now().date().isoformat()

    rows = []
    for l in new_listings:
        addr = l.get('address') or l.get('title') or '?'
        city = l.get('area_label') or l.get('city') or ''
        cat = l.get('category') or 'Commercial'
        tx = 'Location' if l.get('transaction_type') == 'lease' else 'Vente'
        url = l.get('listing_url') or '#'
        rows.append(f"""
            <tr>
              <td style="padding:8px 12px;border-bottom:1px solid #e8efec;font-family:monospace;font-size:12px;">{l.get('mls_number', '')}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #e8efec;">
                <div style="font-weight:600;color:#243522;">{addr}</div>
                <div style="font-size:11px;color:#6e6724;">{city}</div>
              </td>
              <td style="padding:8px 12px;border-bottom:1px solid #e8efec;font-size:12px;">
                <div>{cat}</div>
                <div style="font-size:11px;color:#9ba491;">{tx}</div>
              </td>
              <td style="padding:8px 12px;border-bottom:1px solid #e8efec;font-weight:600;color:#243522;">{fmt_price(l)}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #e8efec;color:#6e6724;">{fmt_sqft(l)}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #e8efec;"><a href="{url}" style="color:#243522;text-decoration:underline;">Voir &rarr;</a></td>
            </tr>
        """)

    rows_html = ''.join(rows)
    n = len(new_listings)
    plural = 's' if n > 1 else ''

    return f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#e8efec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:760px;margin:0 auto;background:#fff;">
    <div style="background:#243522;color:#ddc96a;padding:20px 24px;">
      <div style="font-size:20px;font-weight:600;">Physioactif &ndash; Veille Centris</div>
      <div style="font-size:13px;color:#9ba491;margin-top:4px;">{date_str} &middot; {n} nouveau{plural} local{plural} commercial{plural}</div>
    </div>
    <div style="padding:20px 24px;">
      <p style="font-size:14px;color:#243522;margin:0 0 16px 0;">
        {n} nouveau{plural} listing{plural} apparu{plural} sur Centris depuis le dernier scan&nbsp;:
      </p>
      <table style="width:100%;border-collapse:collapse;font-size:13px;color:#243522;">
        <thead>
          <tr style="background:#ddc96a;color:#243522;">
            <th style="padding:8px 12px;text-align:left;font-size:11px;text-transform:uppercase;">MLS</th>
            <th style="padding:8px 12px;text-align:left;font-size:11px;text-transform:uppercase;">Adresse</th>
            <th style="padding:8px 12px;text-align:left;font-size:11px;text-transform:uppercase;">Cat&eacute;gorie</th>
            <th style="padding:8px 12px;text-align:left;font-size:11px;text-transform:uppercase;">Prix</th>
            <th style="padding:8px 12px;text-align:left;font-size:11px;text-transform:uppercase;">Superficie</th>
            <th style="padding:8px 12px;text-align:left;font-size:11px;text-transform:uppercase;">Lien</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      <p style="margin:20px 0 0 0;font-size:13px;">
        <a href="{MAP_URL}" style="display:inline-block;background:#ddc96a;color:#243522;padding:10px 20px;border-radius:50px;text-decoration:none;font-weight:600;">Ouvrir la carte interactive &rarr;</a>
      </p>
    </div>
    <div style="background:#e8efec;color:#6e6724;padding:12px 24px;font-size:11px;text-align:center;">
      Alerte g&eacute;n&eacute;r&eacute;e automatiquement par refresh-centris (GitHub Actions).
    </div>
  </div>
</body>
</html>"""


def send_via_gmail(access_token: str, html_body: str, subject: str) -> None:
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = SENDER
    msg['To'] = ', '.join(RECIPIENTS)
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode('ascii')
    resp = requests.post(
        GMAIL_SEND_URL,
        headers={'Authorization': f'Bearer {access_token}',
                 'Content-Type': 'application/json'},
        json={'raw': raw},
        timeout=30,
    )
    resp.raise_for_status()
    logger.info(f"Sent: id={resp.json().get('id')} to={msg['To']}")


def main() -> int:
    if not NEW_LISTINGS_FILE.exists():
        logger.info('No new_listings.json - nothing to send')
        return 0

    with open(NEW_LISTINGS_FILE, encoding='utf-8') as f:
        data = json.load(f)

    new_listings = data.get('listings', [])
    if not new_listings:
        logger.info('new_listings.json is empty - nothing to send')
        return 0

    n = len(new_listings)
    date_str = (data.get('search_date') or datetime.now().isoformat())[:10]
    subject = f"Centris: {n} nouveau{'x' if n > 1 else ''} local{'aux' if n > 1 else ''} commercial{'aux' if n > 1 else ''} - {date_str}"

    access_token = get_access_token()
    html_body = build_html(new_listings, data.get('search_date', ''))
    send_via_gmail(access_token, html_body, subject)
    return 0


if __name__ == '__main__':
    sys.exit(main())
