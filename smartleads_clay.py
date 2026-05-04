"""
smartleads_clay.py — Relay: Smartleads pushes lead events here → writes to Google Sheet

Flow:
  Smartleads webhook  →  POST /webhook/smartleads-inbound  →  Google Sheet "Bizbuysell Data"
    (Email_open / Email_reply / Email_Link_clicked columns get incremented)

Setup — Smartleads side:
  Webhooks → Add Webhook
    Webhook Name : Google Sheet Relay   (anything)
    Webhook URL  : https://<your-domain>/webhook/smartleads-inbound
    Event Types  : Email Opened, Email Replied, Link Clicked  (pick what you need)
    Webhook Type : HTTP

Credentials:
  Place google_credentials.json in the project root  (already gitignored)
  OR set GOOGLE_CREDENTIALS env var to the JSON string.

  GOOGLE_SHEET_ID defaults to the Bizbuysell Scraper sheet.
  GOOGLE_SHEET_TAB defaults to "Bizbuysell Data".
"""

import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import gspread
from fastapi import APIRouter, Request
from google.oauth2.service_account import Credentials

router = APIRouter(prefix="/webhook", tags=["smartleads → sheets"])

_SHEET_ID  = os.getenv("GOOGLE_SHEET_ID", "1Cs-qkHoDjnsWHxTjeUf7z7wxJS9W-wvPyWCroxCo4T4")
_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "Bizbuysell Data")
_SCOPES    = ["https://www.googleapis.com/auth/spreadsheets"]
_executor  = ThreadPoolExecutor(max_workers=2)

# ── credential loading ────────────────────────────────────────────────────────

def _load_creds() -> Credentials:
    """Try env var first, fall back to local credentials file."""
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


# ── sheet helpers (synchronous — run in thread pool) ─────────────────────────

def _get_sheet() -> gspread.Worksheet:
    client = gspread.authorize(_load_creds())
    return client.open_by_key(_SHEET_ID).worksheet(_SHEET_TAB)


def _col_index(headers: list[str], *candidates: str) -> int | None:
    """Return 1-based column index matching any of the candidate substrings (case-insensitive)."""
    for i, h in enumerate(headers, start=1):
        hl = h.lower().replace(" ", "_")
        for c in candidates:
            if c in hl:
                return i
    return None


def _write_to_sheet(event_type: str, lead_email: str) -> dict:
    """
    Find the row whose email matches `lead_email` and increment
    the column that corresponds to `event_type`.
    This runs synchronously inside a thread-pool worker.
    """
    ws = _get_sheet()
    all_values: list[list[str]] = ws.get_all_values()

    if not all_values:
        return {"status": "error", "detail": "Sheet is empty"}

    headers = all_values[0]

    # Locate structural columns
    email_col    = _col_index(headers, "email")
    open_col     = _col_index(headers, "email_open")
    reply_col    = _col_index(headers, "email_reply")
    clicked_col  = _col_index(headers, "link_click", "link_clicked", "email_link")

    if email_col is None:
        return {"status": "error", "detail": "Could not find an 'email' column in the sheet"}

    # Find the matching row (row 1 is headers, data starts at row 2)
    target_row: int | None = None
    for row_idx, row in enumerate(all_values[1:], start=2):
        cell_val = row[email_col - 1] if len(row) >= email_col else ""
        if cell_val.strip().lower() == lead_email.strip().lower():
            target_row = row_idx
            break

    if target_row is None:
        return {"status": "not_found", "email": lead_email}

    # Map event type → column
    evt = event_type.lower()
    if "open" in evt:
        target_col = open_col
        col_label  = "Email_open"
    elif "reply" in evt or "replied" in evt:
        target_col = reply_col
        col_label  = "Email_reply"
    elif "click" in evt:
        target_col = clicked_col
        col_label  = "Email_Link_clicked"
    else:
        return {"status": "skipped", "reason": f"unhandled event type: {event_type}"}

    if target_col is None:
        return {"status": "error", "detail": f"Column for '{col_label}' not found in sheet headers"}

    # Increment the existing value (treat blank as 0)
    current_raw = ws.cell(target_row, target_col).value or "0"
    try:
        new_val = int(current_raw) + 1
    except ValueError:
        new_val = 1

    ws.update_cell(target_row, target_col, new_val)

    return {
        "status": "updated",
        "email": lead_email,
        "column": col_label,
        "new_value": new_val,
        "row": target_row,
    }


# ── event extraction helpers ──────────────────────────────────────────────────

def _extract_event_and_email(payload: Any) -> tuple[str, str]:
    """
    Pull event type and lead email out of whatever Smartleads sends.
    Handles both flat and nested payload shapes.
    """
    if not isinstance(payload, dict):
        return "", ""

    # Event type — try common field names
    event_type = (
        payload.get("event_type")
        or payload.get("event")
        or payload.get("type")
        or ""
    )

    # Lead email — try common field names / nested objects
    lead_email = (
        payload.get("to")
        or payload.get("lead_email")
        or payload.get("email")
        or (payload.get("lead") or {}).get("email")
        or (payload.get("data") or {}).get("email")
        or ""
    )

    return str(event_type), str(lead_email)


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post("/smartleads-inbound")
async def receive_from_smartleads(request: Request):
    """
    Smartleads posts lead-event payloads here.
    We find the matching row in the Google Sheet and increment
    Email_open / Email_reply / Email_Link_clicked.
    """
    try:
        payload: Any = await request.json()
    except Exception:
        raw = await request.body()
        payload = raw.decode("utf-8", errors="replace")

    event_type, lead_email = _extract_event_and_email(payload)

    if not lead_email:
        return {
            "status": "skipped",
            "reason": "could not identify lead email in payload",
            "received": payload,
        }

    # Run the synchronous gspread calls in a thread so we don't block the event loop
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        _executor, _write_to_sheet, event_type, lead_email
    )

    return {"event_type": event_type, "lead_email": lead_email, **result}


@router.get("/smartleads-inbound/health")
async def relay_health():
    """Verify credentials load and the sheet is reachable."""
    try:
        loop = asyncio.get_event_loop()
        ws: gspread.Worksheet = await loop.run_in_executor(_executor, _get_sheet)
        headers = await loop.run_in_executor(_executor, ws.row_values, 1)
        return {
            "status": "ok",
            "sheet_id": _SHEET_ID,
            "tab": _SHEET_TAB,
            "columns_found": headers,
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
