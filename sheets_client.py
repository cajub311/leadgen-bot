"""
sheets_client.py
Google Sheets wrapper for LeadGen Bot v3.
Handles all read/write operations to the LeadGen CRM spreadsheet.
Uses gspread + google-auth with service account credentials.
Falls back gracefully to CSV if Sheets is unavailable.
"""

import os
import json
import datetime

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("[SHEETS] gspread not installed -- falling back to CSV mode")

from config import (
    GOOGLE_SHEET_ID,
    SHEET_TAB_LEADS,
    SHEET_TAB_CONTACTED,
    SHEET_TAB_CONFIG,
    LEADS_COLUMNS,
    CONTACTED_COLUMNS,
    FALLBACK_SEARCHES,
    FALLBACK_CITIES,
    get_google_credentials_dict,
)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_client = None
_spreadsheet = None


def _connect():
    """Establish connection to Google Sheets. Returns True if successful."""
    global _client, _spreadsheet

    if _spreadsheet is not None:
        return True

    if not GSPREAD_AVAILABLE:
        print("[SHEETS] gspread not available")
        return False

    if not GOOGLE_SHEET_ID:
        print("[SHEETS] GOOGLE_SHEET_ID not set")
        return False

    creds_dict = get_google_credentials_dict()
    if not creds_dict:
        print("[SHEETS] No valid credentials")
        return False

    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        _client = gspread.authorize(creds)
        _spreadsheet = _client.open_by_key(GOOGLE_SHEET_ID)
        print("[SHEETS] Connected to spreadsheet: {}".format(_spreadsheet.title))
        return True
    except Exception as e:
        print("[SHEETS] Connection failed: {}".format(e))
        _spreadsheet = None
        return False


def is_connected():
    """Check if Sheets connection is active."""
    return _connect()


def _get_worksheet(tab_name):
    """Get a worksheet by name, with fallback for Sheet1."""
    if not _connect():
        return None
    try:
        return _spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        # Fallback: try Sheet1 for the Leads tab
        if tab_name in ("Leads", "Sheet1"):
            try:
                return _spreadsheet.worksheet("Sheet1")
            except Exception:
                pass
        print("[SHEETS] Worksheet \'{}\' not found".format(tab_name))
        return None




# ---------------------------------------------------------------------------
# Sheet Health Check
# ---------------------------------------------------------------------------

def ensure_sheet_health():
    """
    Verify sheet structure at pipeline start:
    - Check that required tabs exist (create if missing)
    - Verify headers are present on Leads and Contacted tabs
    Returns True if healthy, False on critical failure.
    """
    if not _connect():
        return False

    required_tabs = {
        SHEET_TAB_LEADS: LEADS_COLUMNS,
        SHEET_TAB_CONTACTED: CONTACTED_COLUMNS,
        SHEET_TAB_CONFIG: None,  # No header enforcement for Config
    }

    try:
        existing = [ws.title for ws in _spreadsheet.worksheets()]

        for tab_name, expected_headers in required_tabs.items():
            if tab_name not in existing:
                print("[SHEETS] Creating missing tab: {}".format(tab_name))
                ws = _spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=30)
                if expected_headers:
                    ws.append_row(expected_headers, value_input_option="RAW")
                    print("[SHEETS] Added headers to {}".format(tab_name))
            elif expected_headers:
                ws = _spreadsheet.worksheet(tab_name)
                current_headers = ws.row_values(1)
                if not current_headers:
                    ws.append_row(expected_headers, value_input_option="RAW")
                    print("[SHEETS] Added missing headers to {}".format(tab_name))
                else:
                    # Check for missing columns and add them
                    missing = [h for h in expected_headers if h not in current_headers]
                    if missing:
                        for col_name in missing:
                            next_col = len(current_headers) + 1
                            ws.update_cell(1, next_col, col_name)
                            current_headers.append(col_name)
                        print("[SHEETS] Added {} missing columns to {}".format(
                            len(missing), tab_name))

        print("[SHEETS] Health check passed")
        return True

    except Exception as e:
        print("[SHEETS] Health check failed: {}".format(e))
        return False

# ---------------------------------------------------------------------------
# Config Tab -- Read search queries, cities, niches
# ---------------------------------------------------------------------------

def get_search_queries():
    """Read search queries from Config tab. Returns list of query strings."""
    ws = _get_worksheet(SHEET_TAB_CONFIG)
    if not ws:
        print("[SHEETS] Using fallback search queries")
        return FALLBACK_SEARCHES

    try:
        all_values = ws.col_values(1)  # Column A
        queries = []
        in_queries_section = False

        for val in all_values:
            val = val.strip()
            if val == "Search Queries":
                in_queries_section = True
                continue
            if val in ("Cities", "Niches", ""):
                if in_queries_section and val != "":
                    in_queries_section = False
                if val == "" and in_queries_section:
                    in_queries_section = False
                continue
            if in_queries_section and val:
                queries.append(val)

        if queries:
            print("[SHEETS] Loaded {} search queries from Config".format(len(queries)))
            return queries
        else:
            print("[SHEETS] No queries found in Config, using fallback")
            return FALLBACK_SEARCHES
    except Exception as e:
        print("[SHEETS] Error reading queries: {}".format(e))
        return FALLBACK_SEARCHES


def get_cities():
    """Read cities list from Config tab."""
    ws = _get_worksheet(SHEET_TAB_CONFIG)
    if not ws:
        return FALLBACK_CITIES

    try:
        all_values = ws.col_values(1)
        cities = []
        in_cities_section = False

        for val in all_values:
            val = val.strip()
            if val == "Cities":
                in_cities_section = True
                continue
            if val in ("Search Queries", "Niches", ""):
                if in_cities_section and val != "":
                    in_cities_section = False
                if val == "" and in_cities_section:
                    in_cities_section = False
                continue
            if in_cities_section and val:
                cities.append(val)

        return cities if cities else FALLBACK_CITIES
    except Exception as e:
        print("[SHEETS] Error reading cities: {}".format(e))
        return FALLBACK_CITIES


def get_niches():
    """Read niches list from Config tab."""
    ws = _get_worksheet(SHEET_TAB_CONFIG)
    if not ws:
        return []

    try:
        all_values = ws.col_values(1)
        niches = []
        in_niches_section = False

        for val in all_values:
            val = val.strip()
            if val == "Niches":
                in_niches_section = True
                continue
            if val in ("Search Queries", "Cities", ""):
                if in_niches_section and val != "":
                    in_niches_section = False
                if val == "" and in_niches_section:
                    in_niches_section = False
                continue
            if in_niches_section and val:
                niches.append(val)

        return niches
    except Exception as e:
        print("[SHEETS] Error reading niches: {}".format(e))
        return []


# ---------------------------------------------------------------------------
# Leads Tab -- Read / Write / Deduplicate
# ---------------------------------------------------------------------------

def get_existing_leads():
    """Get all existing leads from the Leads tab. Returns list of dicts."""
    ws = _get_worksheet(SHEET_TAB_LEADS)
    if not ws:
        return []

    try:
        records = ws.get_all_records()
        print("[SHEETS] Loaded {} existing leads".format(len(records)))
        return records
    except Exception as e:
        print("[SHEETS] Error reading leads: {}".format(e))
        return []


def get_existing_lead_keys():
    """Get set of (name, city) tuples for deduplication."""
    leads = get_existing_leads()
    keys = set()
    for lead in leads:
        name = str(lead.get("name", "")).strip().lower()
        city = str(lead.get("city", "")).strip().lower()
        if name:
            keys.add((name, city))
    return keys


def append_leads(leads_list):
    """Append new leads to the Leads tab. Each lead is a dict."""
    ws = _get_worksheet(SHEET_TAB_LEADS)
    if not ws:
        print("[SHEETS] Cannot append leads -- not connected")
        return False

    try:
        rows = []
        for lead in leads_list:
            row = [str(lead.get(col, "")) for col in LEADS_COLUMNS]
            rows.append(row)

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            print("[SHEETS] Appended {} leads".format(len(rows)))
        return True
    except Exception as e:
        print("[SHEETS] Error appending leads: {}".format(e))
        return False


def update_lead_field(name, city, field, value):
    """Update a single field for a lead identified by name+city."""
    ws = _get_worksheet(SHEET_TAB_LEADS)
    if not ws:
        return False

    try:
        records = ws.get_all_records()
        headers = ws.row_values(1)

        if field not in headers:
            print("[SHEETS] Field \'{}\' not in headers".format(field))
            return False

        col_idx = headers.index(field) + 1  # 1-indexed

        for i, record in enumerate(records):
            if (str(record.get("name", "")).strip().lower() == name.strip().lower() and
                str(record.get("city", "")).strip().lower() == city.strip().lower()):
                row_idx = i + 2  # +1 for header, +1 for 1-indexing
                ws.update_cell(row_idx, col_idx, str(value))
                return True

        print("[SHEETS] Lead not found: {} / {}".format(name, city))
        return False
    except Exception as e:
        print("[SHEETS] Error updating lead: {}".format(e))
        return False


def update_lead_stage(name, city, new_stage):
    """Update the pipeline_stage for a lead."""
    return update_lead_field(name, city, "pipeline_stage", new_stage)


def update_lead_multiple_fields(name, city, updates_dict):
    """Update multiple fields for a lead using batch update (single API call)."""
    ws = _get_worksheet(SHEET_TAB_LEADS)
    if not ws:
        return False

    try:
        records = ws.get_all_records()
        headers = ws.row_values(1)

        for i, record in enumerate(records):
            if (str(record.get("name", "")).strip().lower() == name.strip().lower() and
                str(record.get("city", "")).strip().lower() == city.strip().lower()):
                row_idx = i + 2  # +1 header, +1 for 1-indexing

                # Build batch update cells
                cells_to_update = []
                for field, value in updates_dict.items():
                    if field in headers:
                        col_idx = headers.index(field) + 1
                        cells_to_update.append(
                            gspread.Cell(row=row_idx, col=col_idx, value=str(value))
                        )

                if cells_to_update:
                    ws.update_cells(cells_to_update, value_input_option="USER_ENTERED")
                return True

        return False
    except Exception as e:
        print("[SHEETS] Error updating multiple fields: {}".format(e))
        return False


def batch_update_leads(updates_list):
    """
    Batch update multiple leads in a single API call.
    updates_list = [{"name": ..., "city": ..., "updates": {field: value, ...}}, ...]
    Much faster than calling update_lead_multiple_fields() in a loop.
    """
    ws = _get_worksheet(SHEET_TAB_LEADS)
    if not ws:
        return False

    if not updates_list:
        return True

    try:
        records = ws.get_all_records()
        headers = ws.row_values(1)

        # Build index of leads by (name_lower, city_lower) -> row_idx
        lead_index = {}
        for i, record in enumerate(records):
            key = (
                str(record.get("name", "")).strip().lower(),
                str(record.get("city", "")).strip().lower(),
            )
            lead_index[key] = i + 2  # +1 header, +1 for 1-indexing

        # Collect all cell updates across all leads
        all_cells = []
        updated_count = 0

        for update in updates_list:
            name = update.get("name", "").strip().lower()
            city = update.get("city", "").strip().lower()
            row_idx = lead_index.get((name, city))

            if row_idx is None:
                print("[SHEETS] Lead not found for batch update: {} / {}".format(
                    update.get("name", "?"), update.get("city", "?")))
                continue

            for field, value in update.get("updates", {}).items():
                if field in headers:
                    col_idx = headers.index(field) + 1
                    all_cells.append(
                        gspread.Cell(row=row_idx, col=col_idx, value=str(value))
                    )
            updated_count += 1

        if all_cells:
            ws.update_cells(all_cells, value_input_option="USER_ENTERED")
            print("[SHEETS] Batch updated {} leads ({} cells) in 1 API call".format(
                updated_count, len(all_cells)))

        return True
    except Exception as e:
        print("[SHEETS] Batch update error: {}".format(e))
        return False


def get_leads_by_stage(stage):
    """Get all leads at a specific pipeline stage."""
    leads = get_existing_leads()
    return [l for l in leads if str(l.get("pipeline_stage", "")).strip().lower() == stage.lower()]


def get_leads_needing_followup(days_since_contact):
    """Get leads that were contacted N+ days ago with no reply."""
    leads = get_existing_leads()
    today = datetime.date.today()
    needs_followup = []

    for lead in leads:
        stage = str(lead.get("pipeline_stage", "")).strip().lower()
        if stage not in ("contacted", "follow_up_1", "follow_up_2"):
            continue

        last_contact = str(lead.get("last_contact", "")).strip()
        if not last_contact:
            continue

        try:
            contact_date = datetime.datetime.strptime(last_contact, "%Y-%m-%d").date()
            days_elapsed = (today - contact_date).days
            if days_elapsed >= days_since_contact:
                lead["days_since_contact"] = days_elapsed
                needs_followup.append(lead)
        except ValueError:
            continue

    return needs_followup


# ---------------------------------------------------------------------------
# Contacted Tab -- Append sent history
# ---------------------------------------------------------------------------

def append_contacted(contacted_list):
    """Append sent email records to the Contacted tab."""
    ws = _get_worksheet(SHEET_TAB_CONTACTED)
    if not ws:
        return False

    try:
        rows = []
        for record in contacted_list:
            row = [str(record.get(col, "")) for col in CONTACTED_COLUMNS]
            rows.append(row)

        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            print("[SHEETS] Appended {} contacted records".format(len(rows)))
        return True
    except Exception as e:
        print("[SHEETS] Error appending contacted: {}".format(e))
        return False


# ---------------------------------------------------------------------------
# Analytics / Stats
# ---------------------------------------------------------------------------

def get_pipeline_stats():
    """Get count of leads at each pipeline stage."""
    leads = get_existing_leads()
    stats = {}
    for lead in leads:
        stage = str(lead.get("pipeline_stage", "unknown")).strip().lower()
        stats[stage] = stats.get(stage, 0) + 1

    stats["total"] = len(leads)
    return stats


def get_contacted_count():
    """Get total number of contacted records."""
    ws = _get_worksheet(SHEET_TAB_CONTACTED)
    if not ws:
        return 0
    try:
        return max(0, len(ws.get_all_values()) - 1)  # minus header
    except Exception:
        return 0


def get_funnel_summary():
    """Get a full funnel summary for reporting."""
    stats = get_pipeline_stats()
    contacted = get_contacted_count()

    return {
        "total_leads": stats.get("total", 0),
        "new": stats.get("new", 0),
        "qualified": stats.get("qualified", 0),
        "draft_ready": stats.get("draft_ready", 0),
        "approved": stats.get("approved", 0),
        "contacted": stats.get("contacted", 0),
        "follow_up_1": stats.get("follow_up_1", 0),
        "follow_up_2": stats.get("follow_up_2", 0),
        "replied": stats.get("replied", 0),
        "meeting": stats.get("meeting", 0),
        "closed": stats.get("closed", 0),
        "unsubscribed": stats.get("unsubscribed", 0),
        "dead": stats.get("dead", 0),
        "total_emails_sent": contacted,
    }


# ---------------------------------------------------------------------------
# Metrics Tab -- Deliverability tracking
# ---------------------------------------------------------------------------

SHEET_TAB_METRICS = "Metrics"
METRICS_COLUMNS = [
    "date", "emails_sent", "emails_bounced", "emails_opened",
    "emails_clicked", "replies_received", "unsubscribes",
    "delivery_rate", "open_rate", "click_rate", "reply_rate",
]


def append_metrics(metrics_dict):
    """Append a daily metrics row to the Metrics tab."""
    # Ensure Metrics tab exists
    if not _connect():
        return False

    try:
        existing = [ws.title for ws in _spreadsheet.worksheets()]
        if SHEET_TAB_METRICS not in existing:
            ws = _spreadsheet.add_worksheet(title=SHEET_TAB_METRICS, rows=1000, cols=15)
            ws.append_row(METRICS_COLUMNS, value_input_option="RAW")
            print("[SHEETS] Created Metrics tab with headers")
        else:
            ws = _spreadsheet.worksheet(SHEET_TAB_METRICS)
            if not ws.row_values(1):
                ws.append_row(METRICS_COLUMNS, value_input_option="RAW")

        row = [str(metrics_dict.get(col, "")) for col in METRICS_COLUMNS]
        ws.append_row(row, value_input_option="USER_ENTERED")
        print("[SHEETS] Appended metrics for {}".format(metrics_dict.get("date", "?")))
        return True
    except Exception as e:
        print("[SHEETS] Error appending metrics: {}".format(e))
        return False


def get_metrics_history(days=30):
    """Get recent metrics rows for trend analysis."""
    if not _connect():
        return []

    try:
        existing = [ws.title for ws in _spreadsheet.worksheets()]
        if SHEET_TAB_METRICS not in existing:
            return []

        ws = _spreadsheet.worksheet(SHEET_TAB_METRICS)
        records = ws.get_all_records()
        # Return last N days
        return records[-days:] if len(records) > days else records
    except Exception as e:
        print("[SHEETS] Error reading metrics: {}".format(e))
        return []
