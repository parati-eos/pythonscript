"""
smartleads_clay.py — Relay: Smartleads pushes lead events here → writes to Google Sheet

Flow:
  Smartleads webhook  →  POST /webhook/smartleads-inbound  →  Google Sheet "Bizbuysell Data"
    (Email_open / Email_reply / Email_Link_clicked columns get incremented)

Matching strategy:
  If SMARTLEADS_API_KEY is set:
    → fetch lead via API using sl_email_lead_id
    → get linkedin_profile (where LINK TO DEAL URL is stored)
    → match against LINK TO DEAL column in sheet
  If SMARTLEADS_API_KEY is NOT set:
    → fall back to to_email matching against FOUND EMAIL column

Setup — Smartleads side:
  Webhooks → Add Webhook
    Webhook Name : Google Sheet Relay
    Webhook URL  : https://<your-domain>/webhook/smartleads-inbound
    Event Types  : Email Opened, Email Replied, Link Clicked
    Webhook Type : HTTP

Credentials:
  Place google_credentials.json in project root (gitignored)
  OR set GOOGLE_CREDENTIALS env var to the full JSON string.
  GOOGLE_SHEET_ID defaults to the Bizbuysell Scraper sheet.
  GOOGLE_SHEET_TAB defaults to "Bizbuysell Data".
"""

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx
import gspread
from fastapi import APIRouter, Request
from google.oauth2.service_account import Credentials

log = logging.getLogger("smartleads")

router = APIRouter(prefix="/webhook", tags=["smartleads → sheets"])

_SHEET_ID          = os.getenv("GOOGLE_SHEET_ID", "1Cs-qkHoDjnsWHxTjeUf7z7wxJS9W-wvPyWCroxCo4T4")
_SHEET_TAB         = os.getenv("GOOGLE_SHEET_TAB", "Bizbuysell Data")
_SMARTLEADS_BASE   = "https://server.smartlead.ai/api/v1"
_SCOPES            = ["https://www.googleapis.com/auth/spreadsheets"]
_executor          = ThreadPoolExecutor(max_workers=2)


# ── credential loading ────────────────────────────────────────────────────────

def _load_creds() -> Credentials:
    raw = os.getenv("GOOGLE_CREDENTIALS", "")
    if raw:
        info = json.loads(raw)
    else:
        cred_file = Path(__file__).parent / "google_credentials.json"
        if not cred_file.exists():
            raise RuntimeError(
                "No Google credentials found. "
                "Set GOOGLE_CREDENTIALS env var or place google_credentials.json in the project root."
            )
        info = json.loads(cred_file.read_text())
    return Credentials.from_service_account_info(info, scopes=_SCOPES)


# ── Smartleads API ────────────────────────────────────────────────────────────

def _fetch_linkedin_from_smartleads(lead_id: str) -> str:
    """
    Call the Smartleads API to get the lead's linkedin_profile field,
    which stores the LINK TO DEAL (bizbuysell URL).
    Returns empty string if anything fails.
    """
    api_key = os.getenv("SMARTLEADS_API_KEY", "")
    if not api_key:
        return ""
    try:
        resp = httpx.get(
            f"{_SMARTLEADS_BASE}/leads/{lead_id}",
            params={"api_key": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        body = resp.json()
        # Response shape: {"ok": true, "data": [{...lead...}]}
        leads = body.get("data") or []
        lead = leads[0] if leads else {}
        custom = lead.get("custom_fields") or {}
        linkedin = (
            lead.get("linkedin_profile")
            or custom.get("LINK_TO_DEAL")
            or custom.get("linktodeal")
            or lead.get("website")
            or ""
        )
        log.warning("SMARTLEADS API — lead_id=%s  linkedin_profile='%s'", lead_id, linkedin)
        return str(linkedin)
    except Exception as exc:
        log.warning("SMARTLEADS API fetch failed for lead_id=%s: %s", lead_id, exc)
        return ""


# ── sheet helpers (synchronous — run in thread pool) ─────────────────────────

def _get_sheet() -> gspread.Worksheet:
    client = gspread.authorize(_load_creds())
    return client.open_by_key(_SHEET_ID).worksheet(_SHEET_TAB)


def _col_index(headers: list[str], *candidates: str) -> int | None:
    """Return 1-based column index matching any candidate substring (case-insensitive)."""
    for i, h in enumerate(headers, start=1):
        hl = h.lower().replace(" ", "_")
        for c in candidates:
            if c in hl:
                return i
    return None


def _write_to_sheet(event_type: str, identifier: str, match_by: str) -> dict:
    """
    Find the matching row and increment the event column.
    match_by: "link"  → match identifier against LINK TO DEAL column (substring)
              "email" → match identifier against FOUND EMAIL column (exact)
    """
    ws = _get_sheet()
    all_values: list[list[str]] = ws.get_all_values()

    if not all_values:
        return {"status": "error", "detail": "Sheet is empty"}

    headers = all_values[0]
    log.warning("SHEET HEADERS: %s", headers)

    open_col    = _col_index(headers, "email_open")
    reply_col   = _col_index(headers, "email_reply")
    clicked_col = _col_index(headers, "link_click", "link_clicked", "email_link")

    if match_by == "link":
        key_col = _col_index(headers, "link_to_deal", "link to deal", "linktodeal")
        col_label_key = "LINK TO DEAL"
    else:
        key_col = _col_index(headers, "found_email", "found email", "foundemail", "email")
        col_label_key = "FOUND EMAIL"

    log.warning("MATCH_BY=%s  key_col=%s  open=%s  reply=%s  clicked=%s",
                match_by, key_col, open_col, reply_col, clicked_col)

    if key_col is None:
        return {"status": "error", "detail": f"Could not find '{col_label_key}' column in sheet"}

    id_lower = identifier.strip().lower()
    target_row: int | None = None
    for row_idx, row in enumerate(all_values[1:], start=2):
        cell_val = (row[key_col - 1] if len(row) >= key_col else "").strip().lower()
        if match_by == "link":
            # substring match — handles partial URLs
            if cell_val and (cell_val == id_lower or id_lower in cell_val or cell_val in id_lower):
                target_row = row_idx
                break
        else:
            if cell_val == id_lower:
                target_row = row_idx
                break

    if target_row is None:
        log.warning("NO ROW FOUND — match_by=%s  identifier='%s'", match_by, identifier)
        return {"status": "not_found", "match_by": match_by, "identifier": identifier}

    # Map event type → column
    evt = event_type.lower()
    if "open" in evt:
        target_col, col_label = open_col, "Email_open"
    elif "reply" in evt or "replied" in evt:
        target_col, col_label = reply_col, "Email_reply"
    elif "click" in evt:
        target_col, col_label = clicked_col, "Email_Link_clicked"
    else:
        return {"status": "skipped", "reason": f"unhandled event type: {event_type}"}

    if target_col is None:
        return {"status": "error", "detail": f"Column '{col_label}' not found in sheet headers"}

    current_raw = ws.cell(target_row, target_col).value or "0"
    try:
        new_val = int(current_raw) + 1
    except ValueError:
        new_val = 1

    ws.update_cell(target_row, target_col, new_val)

    return {
        "status": "updated",
        "match_by": match_by,
        "identifier": identifier,
        "column": col_label,
        "new_value": new_val,
        "row": target_row,
    }


# ── endpoint ──────────────────────────────────────────────────────────────────

@router.post("/smartleads-inbound")
async def receive_from_smartleads(request: Request):
    """
    Receives Smartleads event webhook.
    If SMARTLEADS_API_KEY is set: fetches lead's linkedin_profile (LINK TO DEAL)
    via API and matches by LINK TO DEAL column.
    Otherwise: matches by to_email against FOUND EMAIL column.
    """
    try:
        payload: Any = await request.json()
    except Exception:
        raw = await request.body()
        payload = raw.decode("utf-8", errors="replace")

    log.warning("=== SMARTLEADS PAYLOAD RECEIVED ===")
    log.warning("RAW PAYLOAD: %s", json.dumps(payload, indent=2) if isinstance(payload, dict) else payload)

    if not isinstance(payload, dict):
        return {"status": "skipped", "reason": "non-JSON payload"}

    event_type = (
        payload.get("event_type")
        or payload.get("event")
        or payload.get("type")
        or ""
    )
    to_email   = payload.get("to_email") or payload.get("to") or ""
    lead_id    = str(payload.get("sl_email_lead_id") or "")

    log.warning("event_type='%s'  to_email='%s'  lead_id='%s'", event_type, to_email, lead_id)

    # Decide matching strategy
    api_key = os.getenv("SMARTLEADS_API_KEY", "")
    if api_key and lead_id:
        # Fetch linkedin_profile (= LINK TO DEAL) from Smartleads API
        loop = asyncio.get_event_loop()
        deal_link = await loop.run_in_executor(_executor, _fetch_linkedin_from_smartleads, lead_id)
        if deal_link:
            log.warning("STRATEGY: link-based match  deal_link='%s'", deal_link)
            result = await loop.run_in_executor(_executor, _write_to_sheet, event_type, deal_link, "link")
            return {"event_type": event_type, "deal_link": deal_link, **result}

    # Fallback: email-based match
    if not to_email:
        return {"status": "skipped", "reason": "no email or lead_id in payload"}

    log.warning("STRATEGY: email-based match  to_email='%s'", to_email)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_executor, _write_to_sheet, event_type, to_email, "email")
    return {"event_type": event_type, "to_email": to_email, **result}


@router.get("/smartleads-inbound/health")
async def relay_health():
    """Verify credentials and sheet are reachable."""
    try:
        loop = asyncio.get_event_loop()
        ws: gspread.Worksheet = await loop.run_in_executor(_executor, _get_sheet)
        headers = await loop.run_in_executor(_executor, ws.row_values, 1)
        api_key_set = bool(os.getenv("SMARTLEADS_API_KEY", ""))
        return {
            "status": "ok",
            "sheet_id": _SHEET_ID,
            "tab": _SHEET_TAB,
            "match_strategy": "link (via Smartleads API)" if api_key_set else "email (fallback)",
            "columns_found": headers,
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
