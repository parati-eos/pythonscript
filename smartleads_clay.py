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
import re
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


# ── reply body helpers ───────────────────────────────────────────────────────

def _extract_reply_text(html: str) -> str:
    """Return only the sender's reply — strip HTML tags, quoted thread, and whitespace."""
    # Cut everything from the quoted thread onwards (also trim the wrapping <div>)
    for marker in ('class="gmail_quote', "class='gmail_quote", '<blockquote', 'class=3D"gmail_quote'):
        idx = html.find(marker)
        if idx != -1:
            # Also cut the enclosing <div> immediately before the quote block
            pre = html[:idx].rfind('<div')
            html = html[:pre] if pre != -1 else html[:idx]
    # Remove all HTML tags
    text = re.sub(r'<[^>]+>', '', html)
    # Decode HTML entities and all forms of non-breaking space
    text = (text
            .replace('&nbsp;', ' ')
            .replace(' ', ' ')
            .replace('&#160;', ' ')
            .replace(' ', ' ')
            .replace('&amp;', '&')
            .replace('&lt;', '<')
            .replace('&gt;', '>')
            .replace('&quot;', '"'))
    # Collapse all whitespace into single spaces
    text = re.sub(r'[\s ]+', ' ', text).strip()
    log.warning("REPLY EXTRACTED: '%s'", text[:120])
    return text


# ── sheet helpers (synchronous — run in thread pool) ─────────────────────────

def _get_sheet() -> gspread.Worksheet:
    client = gspread.authorize(_load_creds())
    return client.open_by_key(_SHEET_ID).worksheet(_SHEET_TAB)


def _col_index(headers: list[str], *candidates: str) -> int | None:
    """Return 1-based column index. Exact match wins; falls back to substring."""
    normalized = [h.lower().replace(" ", "_") for h in headers]
    # Pass 1: exact match
    for c in candidates:
        for i, hl in enumerate(normalized, start=1):
            if hl == c:
                return i
    # Pass 2: substring match (candidate is contained in header)
    for c in candidates:
        for i, hl in enumerate(normalized, start=1):
            if c in hl and not hl.startswith("trackb"):
                return i
    return None


def _write_to_sheet(event_type: str, identifier: str, match_by: str,
                    reply_col_name: str = "email_reply", seq_number: str | None = None,
                    reply_body: str | None = None) -> dict:
    """
    Find the matching row and increment the event column.
    match_by: "link"  → match identifier against LINK TO DEAL column (substring)
              "email" → match identifier against FOUND EMAIL column (exact)
    reply_col_name: column name fragment to use for reply events (default "email_reply")
    seq_number: email sequence step e.g. "1", "2" — used for LEAD STATUS value
    reply_body: plain-text reply content; stored verbatim when reply_col_name != "email_reply"
    """
    ws = _get_sheet()
    all_values: list[list[str]] = ws.get_all_values()

    if not all_values:
        return {"status": "error", "detail": "Sheet is empty"}

    headers = all_values[0]
    log.warning("SHEET HEADERS: %s", headers)

    # Map each counter column → its corresponding text column for reply body
    _REPLY_TEXT_COL = {
        "email_reply":          "reply",               # 1st webhook → Reply (AM)
        "trackb_number_reply":  "trackb_email_reply",  # 2nd webhook → TrackB Email Reply (AH)
    }
    open_col       = _col_index(headers, "email_open")
    reply_col      = _col_index(headers, reply_col_name)
    reply_text_col = _col_index(headers, _REPLY_TEXT_COL.get(reply_col_name, ""))
    clicked_col    = _col_index(headers, "link_click", "link_clicked", "email_link")
    status_col    = _col_index(headers, "lead_status", "lead status", "leadstatus")
    category_col  = _col_index(headers, "lead_category", "lead category", "lead_cat")

    if match_by == "link":
        key_col = _col_index(headers, "link_to_deal", "link to deal", "linktodeal")
        col_label_key = "LINK TO DEAL"
    else:
        key_col = _col_index(headers, "found_email", "found email", "foundemail", "email")
        col_label_key = "FOUND EMAIL"

    log.warning("MATCH_BY=%s  key_col=%s  open=%s  reply=%s  clicked=%s  status=%s  category=%s",
                match_by, key_col, open_col, reply_col, clicked_col, status_col, category_col)

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

    # Determine LEAD STATUS from event type (LEAD CATEGORY is not auto-set)
    _STATUS_MAP = [
        ("first",   "Email Sent"),
        ("sent",    "Email Sent"),
        ("open",    "Email Opened"),
        ("click",   "Link Clicked"),
        ("reply",   "Replied"),
        ("replied", "Replied"),
    ]
    evt_lower = event_type.lower()
    lead_status = next((s for k, s in _STATUS_MAP if k in evt_lower), None)

    # Map event type → counter column (None for sent events — no counter column)
    evt = evt_lower
    if "open" in evt:
        target_col, col_label = open_col, "Email_open"
    elif "reply" in evt or "replied" in evt:
        target_col, col_label = reply_col, reply_col_name
    elif "click" in evt:
        target_col, col_label = clicked_col, "Email_Link_clicked"
    elif "sent" in evt:
        target_col, col_label = None, None   # sent: only update LEAD STATUS
    else:
        return {"status": "skipped", "reason": f"unhandled event type: {event_type}"}

    new_val = None
    if target_col is not None:
        if col_label and target_col is None:
            return {"status": "error", "detail": f"Column '{col_label}' not found in sheet headers"}
        current_raw = ws.cell(target_row, target_col).value or "0"
        try:
            new_val = int(current_raw) + 1
        except ValueError:
            new_val = 1
        ws.update_cell(target_row, target_col, new_val)

    # Override LEAD STATUS with sequence label if available e.g. "Email 1"
    if seq_number:
        lead_status = f"Email {seq_number}"

    # Store reply body in the mapped text column (Reply AM for 1st, TrackB Email Reply AH for 2nd)
    is_reply_evt = "reply" in evt_lower or "replied" in evt_lower
    if reply_body and reply_text_col and is_reply_evt:
        ws.update_cell(target_row, reply_text_col, reply_body)
        log.warning("REPLY TEXT saved to col %s at row %s", reply_text_col, target_row)

    # Update LEAD STATUS (LEAD CATEGORY left untouched — not auto-set)
    if lead_status and status_col:
        ws.update_cell(target_row, status_col, lead_status)
        log.warning("LEAD STATUS set to '%s' at row %s", lead_status, target_row)

    return {
        "status": "updated",
        "match_by": match_by,
        "identifier": identifier,
        "column": col_label,
        "new_value": new_val,
        "lead_status": lead_status,
        "row": target_row,
    }


# ── shared request handler ────────────────────────────────────────────────────

async def _handle_smartleads_request(request: Request, reply_col_name: str = "email_reply"):
    try:
        payload: Any = await request.json()
    except Exception:
        raw = await request.body()
        payload = raw.decode("utf-8", errors="replace")

    log.warning("=== SMARTLEADS PAYLOAD RECEIVED (reply_col=%s) ===", reply_col_name)
    log.warning("RAW PAYLOAD: %s", json.dumps(payload, indent=2) if isinstance(payload, dict) else payload)

    if not isinstance(payload, dict):
        return {"status": "skipped", "reason": "non-JSON payload"}

    event_type = (
        payload.get("event_type")
        or payload.get("event")
        or payload.get("type")
        or ""
    )
    to_email = payload.get("to_email") or payload.get("to") or ""
    lead_id  = str(payload.get("sl_email_lead_id") or "")

    # Extract email sequence step: try structured field first, then parse description
    seq_raw = (
        payload.get("seq_number")
        or payload.get("sequence_number")
        or payload.get("email_sequence_step")
    )
    if not seq_raw:
        desc = payload.get("description", "")
        m = re.search(r'[Ee]mail\s+(\d+)', desc)
        seq_raw = m.group(1) if m else None
    seq_number = str(seq_raw).strip() if seq_raw else None

    # Extract and clean reply body
    raw_reply = payload.get("reply_body") or payload.get("reply_text") or ""
    reply_body = _extract_reply_text(raw_reply) if raw_reply else None

    log.warning("event_type='%s'  to_email='%s'  lead_id='%s'  seq='%s'  reply_len=%s",
                event_type, to_email, lead_id, seq_number, len(reply_body) if reply_body else 0)

    api_key = os.getenv("SMARTLEADS_API_KEY", "")
    if api_key and lead_id:
        loop = asyncio.get_event_loop()
        deal_link = await loop.run_in_executor(_executor, _fetch_linkedin_from_smartleads, lead_id)
        if deal_link:
            log.warning("STRATEGY: link-based match  deal_link='%s'", deal_link)
            result = await loop.run_in_executor(
                _executor, _write_to_sheet, event_type, deal_link, "link",
                reply_col_name, seq_number, reply_body
            )
            return {"event_type": event_type, "deal_link": deal_link, **result}

    if not to_email:
        return {"status": "skipped", "reason": "no email or lead_id in payload"}

    log.warning("STRATEGY: email-based match  to_email='%s'", to_email)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        _executor, _write_to_sheet, event_type, to_email, "email",
        reply_col_name, seq_number, reply_body
    )
    return {"event_type": event_type, "to_email": to_email, **result}


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post("/smartleads-inbound")
async def receive_from_smartleads(request: Request):
    """Standard webhook — reply events → Email_reply column."""
    return await _handle_smartleads_request(request, reply_col_name="email_reply")


@router.post("/smartleads-inbound-trackb")
async def receive_from_smartleads_trackb(request: Request):
    """TrackB webhook — counter → TrackB number Reply (AN), text → TrackB Email Reply (AH)."""
    return await _handle_smartleads_request(request, reply_col_name="trackb_number_reply")


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
