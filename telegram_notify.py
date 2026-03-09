"""
telegram_notify.py  -- LeadGen Bot v3
Telegram notification and approval interface for lead generation bot.
Features: inline keyboard buttons (Approve/Edit/Skip), callback query handler,
rich lead cards with website quality + competition indicators, funnel scorecard.
"""

import os
import json
import time
import requests

from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    TELEGRAM_API_BASE, TELEGRAM_MESSAGE_LIMIT,
)


# ---------------------------------------------------------------------------
# Core send functions
# ---------------------------------------------------------------------------

def _get_api_url(method):
    """Build Telegram API URL."""
    return TELEGRAM_API_BASE.format(token=TELEGRAM_BOT_TOKEN) + "/" + method


def send_telegram(message, parse_mode="HTML", reply_markup=None):
    """Send a message to Telegram. Handles splitting for long messages."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Missing BOT_TOKEN or CHAT_ID -- notification skipped")
        print("[TELEGRAM] Message would have been:")
        print(message[:500])
        return False

    if len(message) > TELEGRAM_MESSAGE_LIMIT:
        chunks = _split_message(message, TELEGRAM_MESSAGE_LIMIT)
        success = True
        for i, chunk in enumerate(chunks):
            # Only add reply_markup to last chunk
            markup = reply_markup if i == len(chunks) - 1 else None
            if not _send_single(chunk, parse_mode, markup):
                success = False
        return success
    else:
        return _send_single(message, parse_mode, reply_markup)


def _send_single(message, parse_mode="HTML", reply_markup=None):
    """Send a single message (under 4096 chars)."""
    url = _get_api_url("sendMessage")
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)

    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok"):
            return True
        else:
            print("[TELEGRAM] API error: {}".format(data.get("description", "unknown")))
            return False
    except requests.exceptions.RequestException as exc:
        print("[TELEGRAM] Request failed: {}".format(exc))
        return False


def _split_message(text, max_len=4000):
    """Split a long message into chunks at newline boundaries."""
    chunks = []
    while len(text) > max_len:
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


# ---------------------------------------------------------------------------
# Inline keyboard buttons
# ---------------------------------------------------------------------------

def _make_approve_keyboard(lead_name, draft_index=0):
    """Create inline keyboard with Approve/Edit/Skip buttons."""
    return {
        "inline_keyboard": [
            [
                {"text": "Approve (A)", "callback_data": "approve:{}".format(draft_index)},
                {"text": "Edit", "callback_data": "edit:{}".format(draft_index)},
                {"text": "Skip", "callback_data": "skip:{}".format(draft_index)},
            ],
            [
                {"text": "Approve All", "callback_data": "approve_all"},
                {"text": "Skip All", "callback_data": "skip_all"},
            ],
        ]
    }


def _quality_indicator(value, good="yes", label=""):
    """Return emoji-like indicator for quality signals."""
    if value == good:
        return "[OK] {}".format(label)
    elif value == "unknown":
        return "[??] {}".format(label)
    else:
        return "[!!] {}".format(label)


# ---------------------------------------------------------------------------
# Rich lead card notifications
# ---------------------------------------------------------------------------

def notify_lead_card(draft, index=0):
    """Send a detailed lead card with inline approve/edit/skip buttons."""
    name = draft.get("lead_name", "Unknown")
    email = draft.get("to_email", "")
    niche = draft.get("niche", "")
    city = draft.get("city", "")
    score = draft.get("score", 0)
    industry = draft.get("industry", "")
    sequence = draft.get("sequence_num", 1)

    # Website quality indicators
    ssl = draft.get("website_ssl", "unknown")
    mobile = draft.get("website_mobile", "unknown")
    competition = draft.get("competition", "unknown")

    ssl_ind = _quality_indicator(ssl, "yes", "SSL")
    mobile_ind = _quality_indicator(mobile, "yes", "Mobile")
    comp_label = "HIGH" if competition == "high" else "MED" if competition == "medium" else "LOW" if competition == "low" else "??"

    # Build card
    seq_label = ""
    if sequence == 2:
        seq_label = " [FOLLOW-UP #1]"
    elif sequence == 3:
        seq_label = " [BREAK-UP EMAIL]"

    subject_a = draft.get("subject_a", "")
    subject_b = draft.get("subject_b", "")

    card = (
        "<b>--- LEAD CARD #{idx}{seq} ---</b>\n"
        "\n"
        "<b>{name}</b>\n"
        "Industry: {industry} | {city}\n"
        "Email: <code>{email}</code>\n"
        "Score: <b>{score}</b>/100\n"
        "\n"
        "<b>Quality Signals:</b>\n"
        "  {ssl} | {mobile} | Competition: {comp}\n"
        "\n"
        "<b>Subject A:</b> {subj_a}\n"
        "<b>Subject B:</b> {subj_b}\n"
        "\n"
        "<b>Email Preview:</b>\n"
        "<i>{body_preview}</i>\n"
    ).format(
        idx=index + 1,
        seq=seq_label,
        name=name,
        industry=industry.title(),
        city=city,
        email=email,
        score=score,
        ssl=ssl_ind,
        mobile=mobile_ind,
        comp=comp_label,
        subj_a=subject_a,
        subj_b=subject_b,
        body_preview=_truncate(draft.get("body", ""), 300),
    )

    keyboard = _make_approve_keyboard(name, index)
    return send_telegram(card, reply_markup=keyboard)


def _truncate(text, max_len=300):
    """Truncate text and add ellipsis."""
    # Remove CAN-SPAM footer from preview
    if "---" in text:
        text = text.split("---")[0].strip()
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


# ---------------------------------------------------------------------------
# Batch notifications
# ---------------------------------------------------------------------------

def notify_leads_found(leads, error_summary=""):
    """Notify Telegram how many leads were found in this scrape run."""
    count = len(leads)

    if count == 0:
        msg = (
            "<b>LEAD GEN v3 -- Scrape Complete</b>\n\n"
            "No new qualified leads found this run.\n"
        )
        if error_summary:
            msg += "\nErrors: {}".format(error_summary)
        return send_telegram(msg)

    # Summarize by source
    sources = {}
    for lead in leads:
        src = lead.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1

    source_breakdown = " | ".join(["{}: {}".format(k, v) for k, v in sources.items()])

    # Top scored leads
    sorted_leads = sorted(leads, key=lambda x: int(x.get("score", 0)), reverse=True)
    top_leads = sorted_leads[:5]
    top_list = "\n".join([
        "  {} (score: {}, {})".format(l.get("name", "?"), l.get("score", 0), l.get("niche", ""))
        for l in top_leads
    ])

    msg = (
        "<b>LEAD GEN v3 -- Scrape Complete</b>\n\n"
        "<b>{count} new qualified leads found!</b>\n"
        "Sources: {sources}\n\n"
        "<b>Top 5 by score:</b>\n"
        "{top}\n"
    ).format(count=count, sources=source_breakdown, top=top_list)

    if error_summary:
        msg += "\nErrors: {}".format(error_summary)

    return send_telegram(msg)


def notify_drafts_ready(drafts, skipped):
    """Send individual lead cards for each draft."""
    if not drafts:
        msg = (
            "<b>EMAIL DRAFTS</b>\n\n"
            "No drafts generated this run. ({} skipped)\n"
        ).format(skipped)
        return send_telegram(msg)

    # Header message
    msg = (
        "<b>EMAIL DRAFTS -- {} Ready for Review</b>\n\n"
        "Review each card below and tap Approve, Edit, or Skip.\n"
        "{} leads skipped (invalid email or low score)\n"
    ).format(len(drafts), skipped)
    send_telegram(msg)

    # Send individual cards with inline buttons
    for i, draft in enumerate(drafts):
        notify_lead_card(draft, i)
        time.sleep(0.5)  # Rate limit

    return True


def notify_followups_ready(drafts, skipped):
    """Send follow-up draft cards."""
    if not drafts:
        return True

    msg = (
        "<b>FOLLOW-UP DRAFTS -- {} Ready</b>\n\n"
        "These leads haven\'t replied. Review follow-up drafts below.\n"
        "{} skipped\n"
    ).format(len(drafts), skipped)
    send_telegram(msg)

    for i, draft in enumerate(drafts):
        notify_lead_card(draft, i + 100)  # Offset index to distinguish from initial drafts
        time.sleep(0.5)

    return True


# ---------------------------------------------------------------------------
# Pipeline stats & funnel scorecard
# ---------------------------------------------------------------------------

def notify_pipeline_stats(leads_count=0, drafts_count=0, skipped_count=0,
                          followups_count=0, error_summary="", funnel=None):
    """Send pipeline run summary with funnel scorecard."""
    msg = (
        "<b>PIPELINE SUMMARY</b>\n"
        "========================\n"
        "Leads scraped: {leads}\n"
        "Drafts generated: {drafts}\n"
        "Follow-ups generated: {followups}\n"
        "Skipped: {skipped}\n"
    ).format(
        leads=leads_count,
        drafts=drafts_count,
        followups=followups_count,
        skipped=skipped_count,
    )

    if error_summary:
        msg += "Errors: {}\n".format(error_summary)

    # Add funnel scorecard if available
    if funnel:
        msg += (
            "\n<b>FUNNEL SCORECARD</b>\n"
            "========================\n"
            "Total leads in CRM: {total}\n"
            "  New:          {new}\n"
            "  Qualified:    {qualified}\n"
            "  Draft Ready:  {draft_ready}\n"
            "  Approved:     {approved}\n"
            "  Contacted:    {contacted}\n"
            "  Follow-up 1:  {f1}\n"
            "  Follow-up 2:  {f2}\n"
            "  Replied:      {replied}\n"
            "  Meeting:      {meeting}\n"
            "  Closed:       {closed}\n"
            "  Unsubscribed: {unsub}\n"
            "  Dead:         {dead}\n"
            "------------------------\n"
            "Emails sent:    {emails}\n"
        ).format(
            total=funnel.get("total_leads", 0),
            new=funnel.get("new", 0),
            qualified=funnel.get("qualified", 0),
            draft_ready=funnel.get("draft_ready", 0),
            approved=funnel.get("approved", 0),
            contacted=funnel.get("contacted", 0),
            f1=funnel.get("follow_up_1", 0),
            f2=funnel.get("follow_up_2", 0),
            replied=funnel.get("replied", 0),
            meeting=funnel.get("meeting", 0),
            closed=funnel.get("closed", 0),
            unsub=funnel.get("unsubscribed", 0),
            dead=funnel.get("dead", 0),
            emails=funnel.get("total_emails_sent", 0),
        )

    msg += "\n<i>LeadGen Bot v3 | Twin Cities Web Co</i>"
    return send_telegram(msg)


# ---------------------------------------------------------------------------
# Error alert
# ---------------------------------------------------------------------------

def notify_error(error_msg, traceback_str=""):
    """Send error alert to Telegram."""
    msg = (
        "<b>[ERROR] LeadGen Bot v3</b>\n\n"
        "{}\n"
    ).format(error_msg[:1000])

    if traceback_str:
        msg += "\n<pre>{}</pre>".format(traceback_str[:1500])

    return send_telegram(msg)


# ---------------------------------------------------------------------------
# Callback query handler (for inline button responses)
# ---------------------------------------------------------------------------

def poll_callback_queries(timeout_seconds=30):
    """
    Poll for callback query responses from inline buttons.
    Returns list of (action, draft_index) tuples.
    Note: In GitHub Actions (cron), this runs for a short window.
    For real-time approval, a webhook-based approach would be better.
    """
    if not TELEGRAM_BOT_TOKEN:
        return []

    url = _get_api_url("getUpdates")
    responses = []
    offset = 0
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            params = {
                "timeout": 5,
                "offset": offset,
                "allowed_updates": ["callback_query"],
            }
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()

            if not data.get("ok"):
                break

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                callback = update.get("callback_query", {})
                cb_data = callback.get("data", "")

                if ":" in cb_data:
                    action, index = cb_data.split(":", 1)
                    responses.append((action, index))

                    # Acknowledge the callback
                    _answer_callback(callback.get("id", ""), action)

                elif cb_data in ("approve_all", "skip_all"):
                    responses.append((cb_data, "all"))
                    _answer_callback(callback.get("id", ""), cb_data)

            if not data.get("result"):
                time.sleep(2)

        except Exception as e:
            print("[TELEGRAM] Poll error: {}".format(e))
            break

    return responses


def _answer_callback(callback_query_id, action):
    """Acknowledge a callback query so the button stops spinning."""
    if not callback_query_id:
        return

    url = _get_api_url("answerCallbackQuery")
    text_map = {
        "approve": "Approved! Will send.",
        "edit": "Reply with your edits.",
        "skip": "Skipped.",
        "approve_all": "All approved!",
        "skip_all": "All skipped.",
    }

    try:
        requests.post(url, json={
            "callback_query_id": callback_query_id,
            "text": text_map.get(action, "Got it."),
        }, timeout=5)
    except Exception:
        pass
