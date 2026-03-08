"""
telegram_notify.py
Telegram notification sender for the lead generation bot.
Sends pipeline status updates, lead summaries, and error alerts.
"""

import os
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/sendMessage"


# ---------------------------------------------------------------------------
# Core send function
# ---------------------------------------------------------------------------

def send_telegram(message, parse_mode="HTML"):
    """
    Send a message to the configured Telegram chat.

    Args:
        message (str): The message text. Supports HTML formatting when
                       parse_mode='HTML' (bold, italic, code, links).
        parse_mode (str): 'HTML' or 'Markdown'. Defaults to 'HTML'.

    Returns:
        bool: True if sent successfully, False otherwise.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Missing BOT_TOKEN or CHAT_ID - notification skipped")
        print("[TELEGRAM] Message would have been:")
        print(message)
        return False

    url = TELEGRAM_API_BASE.format(token=TELEGRAM_BOT_TOKEN)
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
            print("[TELEGRAM] Notification sent successfully")
            return True
        else:
            print("[TELEGRAM] API returned error: {}".format(data.get("description", "unknown")))
            return False
    except requests.exceptions.RequestException as exc:
        print("[TELEGRAM] Request failed: {}".format(exc))
        return False


# ---------------------------------------------------------------------------
# Notification helpers
# ---------------------------------------------------------------------------

def notify_leads_found(leads):
    """
    Send a Telegram summary of how many leads were scraped and
    list the top 5 by score.

    Args:
        leads (list): List of lead dicts as returned by run_scraper().

    Returns:
        bool: True if message sent successfully.
    """
    count = len(leads)

    if count == 0:
        message = (
            "<b>Lead Gen Bot</b> - Scrape Complete\n\n"
            "No qualified leads found this run.\n"
            "All results were below the score threshold or already contacted."
        )
        return send_telegram(message)

    # Sort by score descending (should already be sorted, but defensive)
    top5 = sorted(leads, key=lambda x: int(x.get("score", 0)), reverse=True)[:5]

    lines = []
    lines.append("<b>Lead Gen Bot</b> - Scrape Complete")
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

        badge = "[no site]" if not has_website else ""
        email_tag = " | has email" if has_email else ""

        lines.append("")
        lines.append(
            "{}. <b>{}</b> {} | Score: {}".format(i, name, badge, score)
        )
        lines.append(
            "   {} - {} | {} stars ({} reviews){}".format(
                niche.title(), city, rating, reviews, email_tag
            )
        )

    message = "\n".join(lines)
    return send_telegram(message)


def notify_emails_sent(count, failed):
    """
    Send a Telegram summary of how many emails were sent and how many failed.

    Args:
        count (int): Number of emails successfully sent.
        failed (int): Number of emails that failed to send.

    Returns:
        bool: True if message sent successfully.
    """
    total = count + failed
    success_rate = int((count / total) * 100) if total > 0 else 0

    if count == 0 and failed == 0:
        status_line = "No emails were queued for sending today."
    elif failed == 0:
        status_line = "All <b>{}</b> email(s) sent successfully.".format(count)
    else:
        status_line = (
            "<b>{}</b> sent, <b>{}</b> failed ({} success rate).".format(
                count, failed, "{}%".format(success_rate)
            )
        )

    message = (
        "<b>Lead Gen Bot</b> - Email Run Complete\n\n"
        + status_line
        + "\n\nExpect replies within 24-72 hours. "
        "Check your Gmail inbox for responses."
    )
    return send_telegram(message)


def notify_error(error_msg):
    """
    Send a Telegram error alert when the pipeline encounters an exception.

    Args:
        error_msg (str): The error message or exception string.

    Returns:
        bool: True if message sent successfully.
    """
    # Truncate very long error messages to avoid Telegram 4096-char limit
    max_error_len = 800
    if len(error_msg) > max_error_len:
        error_msg = error_msg[:max_error_len] + "... [truncated]"

    message = (
        "<b>Lead Gen Bot - ERROR</b>\n\n"
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
    print("Testing Telegram notifications...")

    # Test leads found notification
    sample_leads = [
        {
            "name": "Acme Plumbing LLC",
            "score": 70,
            "niche": "plumber",
            "city": "Saint Paul",
            "rating": 3.1,
            "reviews": 18,
            "email": "owner@acmeplumbing.example",
            "website": "",
        },
        {
            "name": "Quick Fix Auto",
            "score": 65,
            "niche": "auto repair",
            "city": "Minneapolis",
            "rating": 2.8,
            "reviews": 9,
            "email": "",
            "website": "",
        },
    ]

    notify_leads_found(sample_leads)
    notify_emails_sent(2, 0)
    notify_error("Test error: simulated exception for verification")

    print("Done. Check your Telegram chat.")
