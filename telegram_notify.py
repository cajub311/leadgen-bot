"""
telegram_notify.py
Telegram notification and approval interface for the lead generation bot.
Sends lead cards with business details, draft previews, and pipeline stats.
Charles reviews leads in Telegram and approves outreach.
"""

import os
import json
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"


# ---------------------------------------------------------------------------
# Core send functions
# ---------------------------------------------------------------------------

def send_telegram(message, parse_mode="HTML"):
    """
    Send a message to the configured Telegram chat.
    Handles message splitting for long messages (Telegram 4096 char limit).
    Returns True if sent successfully, False otherwise.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Missing BOT_TOKEN or CHAT_ID -- notification skipped")
        print("[TELEGRAM] Message would have been:")
        print(message[:500])
        return False

    if len(message) > 4000:
        chunks = _split_message(message, 4000)
        success = True
        for chunk in chunks:
            if not _send_single(chunk, parse_mode):
                success = False
        return success
    else:
        return _send_single(message, parse_mode)


def _send_single(message, parse_mode="HTML"):
    """Send a single message (under 4096 chars)."""
    url = TELEGRAM_API_BASE.format(token=TELEGRAM_BOT_TOKEN) + "/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

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
# Lead card notifications
# ---------------------------------------------------------------------------

def notify_lead_card(draft):
    """
    Send a detailed lead card to Telegram for a single draft.
    Shows business details + email preview so Charles can decide.
    """
    name = draft.get("lead_name", "Unknown")
    email = draft.get("to_email", "")
    niche = draft.get("niche", "")
    city = draft.get("city", "")
    score = draft.get("score", 0)
    rating = draft.get("rating", "N/A")
    reviews = draft.get("reviews", 0)
    website = draft.get("website", "")
    phone = draft.get("phone", "")
    source = draft.get("source", "")
    subject = draft.get("subject", "")
    body = draft.get("body", "")

    if int(score) >= 70:
        badge = "[HOT]"
    elif int(score) >= 60:
        badge = "[WARM]"
    else:
        badge = "[COOL]"

    web_status = website[:50] if website else "(no website)"

    body_preview = body[:300].replace("\n", " ") if body else "(no preview)"
    if len(body) > 300:
        body_preview += "..."

    lines = [
        "<b>{} LEAD CARD</b>".format(badge),
        "",
        "<b>{}</b>".format(name),
        "Niche: {} | {}".format(niche.title(), city),
        "Score: <b>{}</b> | {} stars ({} reviews)".format(score, rating, reviews),
        "Web: {}".format(web_status),
        "Phone: {}".format(phone or "(none)"),
        "Email: {}".format(email),
        "Source: {}".format(source),
        "",
        "<b>--- Draft Preview ---</b>",
        "Subject: <i>{}</i>".format(subject),
        "",
        "<code>{}</code>".format(body_preview),
        "",
        "Reply with the business name to approve sending.",
    ]

    message = "\n".join(lines)
    return send_telegram(message)


def notify_leads_found(leads):
    """
    Send a Telegram summary of how many leads were scraped.
    Shows the top 5 by score with key details.
    """
    count = len(leads)

    if count == 0:
        message = (
            "<b>Lead Gen Bot v2</b> -- Scrape Complete\n\n"
            "No qualified leads found this run.\n"
            "All results were below the score threshold or already contacted."
        )
        return send_telegram(message)

    top5 = sorted(leads, key=lambda x: int(x.get("score", 0)), reverse=True)[:5]

    lines = []
    lines.append("<b>Lead Gen Bot v2</b> -- Scrape Complete")
    lines.append("")
    lines.append("Found <b>{}</b> qualified lead(s) today.".format(count))
    lines.append("")
    lines.append("<b>Top 5 by Score:</b>")

    for i, lead in enumerate(top5, 1):
        name = lead.get("name", "Unknown")
        score = lead.get("score", 0)
        niche = lead.get("niche", "")
        city = lead.get("city", "")
        rating = lead.get("rating", "N/A")
        reviews = lead.get("reviews", 0)
        has_email = bool((lead.get("email") or "").strip())
        has_website = bool((lead.get("website") or "").strip())
        source = lead.get("source", "")

        badge = "(no site)" if not has_website else ""
        email_tag = " | has email" if has_email else ""

        lines.append("")
        lines.append(
            "{}. <b>{}</b> {} | Score: {}".format(i, name, badge, score)
        )
        lines.append(
            "   {} - {} | {} stars ({} reviews){} | src:{}".format(
                niche.title(), city, rating, reviews, email_tag, source
            )
        )

    message = "\n".join(lines)
    return send_telegram(message)


# ---------------------------------------------------------------------------
# Draft batch notification
# ---------------------------------------------------------------------------

def notify_drafts_ready(drafts, skipped):
    """
    Send a summary of generated drafts, then individual lead cards
    for each draft awaiting approval.
    """
    count = len(drafts)

    if count == 0:
        message = (
            "<b>Lead Gen Bot v2</b> -- Draft Generation Complete\n\n"
            "No email drafts were generated this run.\n"
            "({} leads skipped due to invalid emails or empty drafts)".format(skipped)
        )
        return send_telegram(message)

    summary_lines = [
        "<b>Lead Gen Bot v2</b> -- Drafts Ready for Review",
        "",
        "<b>{}</b> email draft(s) generated.".format(count),
    ]
    if skipped:
        summary_lines.append("{} leads skipped (invalid email or empty draft).".format(skipped))
    summary_lines.append("")
    summary_lines.append("Individual lead cards coming next...")
    summary_lines.append("Reply with a business name to approve sending that email.")

    send_telegram("\n".join(summary_lines))

    for draft in drafts:
        notify_lead_card(draft)

    return True


# ---------------------------------------------------------------------------
# Pipeline stats notification
# ---------------------------------------------------------------------------

def notify_pipeline_stats(leads_count, drafts_count, skipped_count,
                          google_count=0, yelp_count=0):
    """
    Send end-of-run pipeline statistics summary.
    """
    lines = [
        "<b>Lead Gen Bot v2</b> -- Pipeline Stats",
        "",
        "<b>Scraping:</b>",
        "  Google Maps results: {}".format(google_count),
        "  Yelp results: {}".format(yelp_count),
        "  Qualified leads: {}".format(leads_count),
        "",
        "<b>Email Drafts:</b>",
        "  Drafts generated: {}".format(drafts_count),
        "  Leads skipped: {}".format(skipped_count),
        "",
        "<b>Status:</b> Awaiting your review in Telegram.",
        "Reply with a business name to approve, or ignore to skip.",
    ]

    message = "\n".join(lines)
    return send_telegram(message)


# ---------------------------------------------------------------------------
# Error notification
# ---------------------------------------------------------------------------

def notify_error(error_msg):
    """
    Send a Telegram error alert when the pipeline encounters an exception.
    """
    max_error_len = 800
    if len(error_msg) > max_error_len:
        error_msg = error_msg[:max_error_len] + "... [truncated]"

    message = (
        "<b>Lead Gen Bot v2 -- ERROR</b>\n\n"
        "The pipeline encountered an error and may not have completed fully.\n\n"
        "<b>Error details:</b>\n"
        "<code>{}</code>\n\n"
        "Check the GitHub Actions log for the full traceback."
    ).format(error_msg)

    return send_telegram(message)


# ---------------------------------------------------------------------------
# Entry point (manual test)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Testing Telegram notifications v2...")

    sample_draft = {
        "lead_name": "Acme Plumbing LLC",
        "to_email": "owner@acmeplumbing.example",
        "subject": "Quick question about Acme Plumbing's online presence",
        "body": "Hi there,\n\nI came across Acme Plumbing while looking for plumbing services in Saint Paul...\n\nBest,\nAlex\nTwin Cities Web Co",
        "niche": "plumber",
        "city": "Saint Paul",
        "score": 70,
        "rating": 3.1,
        "reviews": 18,
        "website": "",
        "phone": "+16515550101",
        "source": "google",
    }

    notify_lead_card(sample_draft)
    notify_pipeline_stats(
        leads_count=8, drafts_count=5, skipped_count=3,
        google_count=45, yelp_count=30
    )
    notify_error("Test error: simulated exception for verification")

    print("Done. Check your Telegram chat.")
