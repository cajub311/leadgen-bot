"""
email_bot.py
AI-powered cold email personalizer with CAN-SPAM compliance.
Generates personalized outreach drafts via Claude -- does NOT auto-send.
Drafts are sent to Telegram for human approval before any email goes out.

Sending flow:
  1. Generate AI draft for each lead (Claude claude-3-5-haiku-latest)
  2. Validate email address format
  3. Append CAN-SPAM required footer (physical address + unsubscribe)
  4. Return drafts for Telegram approval queue
  5. Only approved drafts get sent (via send_approved_email)
"""

import os
import re
import csv
import json
import time
import smtplib
import datetime
import tempfile
import shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

LEADS_FILE = "leads.csv"
CONTACTED_FILE = "contacted.csv"
DRAFTS_FILE = "drafts.json"
MAX_DRAFTS_PER_RUN = 25

ANTHROPIC_ENDPOINT = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-5-haiku-latest"

CONTACTED_COLUMNS = [
    "name", "email", "niche", "city", "score", "sent_date", "subject"
]

# ---------------------------------------------------------------------------
# CAN-SPAM Compliance Footer
# ---------------------------------------------------------------------------

CAN_SPAM_FOOTER = (
    "\n\n---\n"
    "Twin Cities Web Co | Saint Paul, MN 55104\n"
    "You're receiving this because your business was found in a public directory.\n"
    "To stop future emails, reply with 'unsubscribe' and we'll remove you immediately.\n"
    "This is a one-time outreach -- we do not send follow-ups without your permission."
)


# ---------------------------------------------------------------------------
# Email validation (lightweight, no paid API)
# ---------------------------------------------------------------------------

def validate_email(email):
    """
    Basic email validation:
    1. Format check (regex)
    2. Domain has MX or A record (DNS check)
    Returns (is_valid: bool, reason: str)
    """
    if not email or not isinstance(email, str):
        return False, "empty email"

    email = email.strip().lower()

    # Format check
    pattern = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "invalid format"

    # Check for obviously fake domains
    domain = email.split("@")[1]
    fake_domains = {
        "example.com", "example.org", "test.com", "fake.com",
        "noemail.com", "none.com", "na.com",
    }
    if domain in fake_domains:
        return False, "fake domain"

    # DNS check for MX record
    try:
        import socket
        socket.getaddrinfo(domain, None)
        return True, "valid"
    except socket.gaierror:
        return False, "domain does not resolve"
    except Exception:
        # If DNS check fails for any reason, still allow (might be network issue in CI)
        return True, "dns check skipped"


# ---------------------------------------------------------------------------
# AI email generation
# ---------------------------------------------------------------------------

def generate_email(lead):
    """
    Call Claude claude-3-5-haiku to generate a personalized cold email
    for a local business lead.

    Returns a dict with keys: 'subject' and 'body'.
    Falls back to a template if no API key is set or the call fails.
    """
    name = lead.get("name", "there")
    niche = lead.get("niche", "business")
    city = lead.get("city", "your city")
    rating = lead.get("rating", "")
    reviews = lead.get("reviews", "")
    website = (lead.get("website") or "").strip()
    has_website = bool(website)
    phone = (lead.get("phone") or "").strip()
    source = lead.get("source", "online directory")

    # Build context-aware service angle
    if not has_website:
        service_angle = "build you a professional website so customers can find you online"
        pain_point = "Many customers search online before calling -- without a website, you may be losing jobs to competitors every week."
    elif rating and float(rating) < 3.5:
        service_angle = "help you manage and improve your online reputation"
        pain_point = "A low star rating can quietly cost you 30-40% of potential customers before they even call."
    else:
        service_angle = "strengthen your online presence and bring in more local customers"
        pain_point = "In today's market, the businesses that show up first online win most of the calls."

    prompt = (
        "You are a friendly, local digital marketing consultant based in the Twin Cities, MN. "
        "Write a short, genuine cold email to a local business owner. "
        "Do NOT use hype, all-caps, or pushy sales language. Sound like a real neighbor, not a bot.\n\n"
        "Business details:\n"
        "- Business name: {name}\n"
        "- Industry/niche: {niche}\n"
        "- City: {city}\n"
        "- Google rating: {rating} stars ({reviews} reviews)\n"
        "- Has website: {has_website}\n"
        "- Service to offer: {service_angle}\n"
        "- Pain point to mention: {pain_point}\n\n"
        "Requirements:\n"
        "1. Subject line: short, specific, no clickbait (max 60 chars)\n"
        "2. Email body: 3-4 short paragraphs, under 200 words total\n"
        "3. Open with a genuine compliment or observation about their business\n"
        "4. Mention the pain point naturally in one sentence\n"
        "5. Offer a free 15-minute call or free audit - no commitment\n"
        "6. Sign off as 'Alex' from 'Twin Cities Web Co'\n"
        "7. Do NOT include any footer or unsubscribe text (that is added automatically)\n\n"
        "Respond ONLY with valid JSON in this exact format:\n"
        "{{\"subject\": \"...\", \"body\": \"...\"}}\n"
        "Use \\n for line breaks inside the body string."
    ).format(
        name=name,
        niche=niche,
        city=city,
        rating=rating,
        reviews=reviews,
        has_website=has_website,
        service_angle=service_angle,
        pain_point=pain_point,
    )

    if not ANTHROPIC_API_KEY:
        print("  [WARN] No ANTHROPIC_API_KEY -- using fallback template email")
        return _fallback_email(name, niche, city, has_website)

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 512,
        "messages": [
            {"role": "user", "content": prompt}
        ],
    }

    try:
        resp = requests.post(ANTHROPIC_ENDPOINT, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        text = data["content"][0]["text"].strip()

        # Strip markdown code fences if present
        if text.startswith("```"):
            lines = text.splitlines()
            text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

        result = json.loads(text)
        if "subject" in result and "body" in result:
            return result
        raise ValueError("Missing subject or body in Claude response")

    except Exception as exc:
        print("  [WARN] Claude API error: {} -- using fallback template".format(exc))
        return _fallback_email(name, niche, city, has_website)


def _fallback_email(name, niche, city, has_website):
    """Plain-text template used when Anthropic API is unavailable."""
    if not has_website:
        subject = "Quick question about {}'s online presence".format(name)
        body = (
            "Hi there,\n\n"
            "I came across {} while looking for {} services in {} and noticed you "
            "don't have a website yet.\n\n"
            "A lot of customers search online before they call -- a simple, professional "
            "site can make a real difference in how many new jobs you land each week.\n\n"
            "I help local businesses in the Twin Cities get set up online quickly and "
            "affordably. Would you be open to a free 15-minute call to see if it could "
            "be a good fit?\n\n"
            "No pressure at all -- just a quick chat.\n\n"
            "Best,\nAlex\nTwin Cities Web Co"
        ).format(name, niche, city)
    else:
        subject = "Helping {} attract more local customers".format(name)
        body = (
            "Hi there,\n\n"
            "I found {} while researching {} businesses in {} -- looks like you've "
            "been serving the community for a while.\n\n"
            "I help local businesses improve their online presence so more customers "
            "find them first on Google. Even small tweaks can bring in a few extra "
            "calls per week.\n\n"
            "Would you be open to a free 15-minute audit? I'll tell you exactly what's "
            "working and what could be improved -- no strings attached.\n\n"
            "Best,\nAlex\nTwin Cities Web Co"
        ).format(name, niche, city)
    return {"subject": subject, "body": body}


# ---------------------------------------------------------------------------
# CAN-SPAM compliant email body builder
# ---------------------------------------------------------------------------

def build_compliant_body(body):
    """
    Append the CAN-SPAM required footer to the email body.
    Includes physical address and unsubscribe mechanism.
    """
    return body + CAN_SPAM_FOOTER


# ---------------------------------------------------------------------------
# Email sender (ONLY called after Telegram approval)
# ---------------------------------------------------------------------------

def send_approved_email(to_address, subject, body):
    """
    Send a single approved email via Gmail SMTP.
    This is ONLY called when a lead is approved via Telegram.
    The body should already include the CAN-SPAM footer.

    NOTE: Uses Gmail SMTP with App Password. Since we only send
    approved emails (not bulk), this stays within Gmail's acceptable
    use policy.

    Returns True on success, False on failure.
    """
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD", "")

    if not GMAIL_ADDRESS:
        print("  [ERROR] GMAIL_ADDRESS not set")
        return False

    if not gmail_app_password:
        # If no app password, save as draft instead of sending
        print("  [INFO] No GMAIL_APP_PASSWORD -- email saved as draft only")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = "{} <{}>".format("Alex | Twin Cities Web Co", GMAIL_ADDRESS)
    msg["To"] = to_address
    msg["Reply-To"] = GMAIL_ADDRESS

    part = MIMEText(body, "plain")
    msg.attach(part)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(GMAIL_ADDRESS, gmail_app_password)
            server.sendmail(GMAIL_ADDRESS, to_address, msg.as_string())
        print("  [SENT] {} -> {}".format(subject[:50], to_address))
        return True
    except smtplib.SMTPException as exc:
        print("  [FAIL] SMTP error sending to {}: {}".format(to_address, exc))
        return False
    except Exception as exc:
        print("  [FAIL] Unexpected error sending to {}: {}".format(to_address, exc))
        return False


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_leads():
    """
    Read leads.csv and return rows where status == 'new' and email is not empty.
    Returns a list of dicts.
    """
    if not os.path.exists(LEADS_FILE):
        print("[WARN] {} not found -- run lead_scraper.py first".format(LEADS_FILE))
        return []

    leads = []
    try:
        with open(LEADS_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                status = (row.get("status") or "").strip().lower()
                email = (row.get("email") or "").strip()
                if status == "new" and email:
                    leads.append(row)
    except Exception as exc:
        print("[ERROR] Could not read {}: {}".format(LEADS_FILE, exc))

    return leads


def mark_contacted(lead, subject):
    """
    1. Append the lead to contacted.csv.
    2. Update the lead's status to 'sent' in leads.csv.
    """
    file_exists = os.path.exists(CONTACTED_FILE)
    try:
        with open(CONTACTED_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CONTACTED_COLUMNS)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "name": lead.get("name", ""),
                "email": lead.get("email", ""),
                "niche": lead.get("niche", ""),
                "city": lead.get("city", ""),
                "score": lead.get("score", ""),
                "sent_date": datetime.date.today().isoformat(),
                "subject": subject,
            })
    except Exception as exc:
        print("  [WARN] Could not write to {}: {}".format(CONTACTED_FILE, exc))

    _update_lead_status(lead.get("name", ""), "sent")


def mark_draft_ready(lead_name):
    """Update the lead's status to 'draft_ready' in leads.csv."""
    _update_lead_status(lead_name, "draft_ready")


def _update_lead_status(name, new_status):
    """
    Rewrite leads.csv updating the row matching `name` to `new_status`.
    Uses a temp file to avoid data loss on error.
    """
    if not os.path.exists(LEADS_FILE):
        return

    try:
        rows = []
        fieldnames = None
        with open(LEADS_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if (row.get("name") or "").strip().lower() == name.strip().lower():
                    row["status"] = new_status
                rows.append(row)

        if fieldnames is None:
            return

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
        os.close(tmp_fd)
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        shutil.move(tmp_path, LEADS_FILE)

    except Exception as exc:
        print("  [WARN] Could not update status in {}: {}".format(LEADS_FILE, exc))


# ---------------------------------------------------------------------------
# Draft generation pipeline (replaces the old auto-send pipeline)
# ---------------------------------------------------------------------------

def save_drafts(drafts):
    """Save draft emails to drafts.json for the Telegram approval flow."""
    try:
        with open(DRAFTS_FILE, "w", encoding="utf-8") as f:
            json.dump(drafts, f, indent=2, ensure_ascii=False)
        print("[INFO] {} drafts saved to {}".format(len(drafts), DRAFTS_FILE))
    except Exception as exc:
        print("[ERROR] Could not save drafts: {}".format(exc))


def load_drafts():
    """Load draft emails from drafts.json."""
    if not os.path.exists(DRAFTS_FILE):
        return []
    try:
        with open(DRAFTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print("[ERROR] Could not load drafts: {}".format(exc))
        return []


def run_email_bot():
    """
    Draft generation pipeline (NO auto-sending):
      1. Load new leads with emails from leads.csv
      2. Cap at MAX_DRAFTS_PER_RUN
      3. Validate each email address
      4. Generate AI-personalized email draft
      5. Append CAN-SPAM footer
      6. Save all drafts to drafts.json
      7. Mark leads as 'draft_ready'
      8. Return list of draft dicts for Telegram notification

    Returns (drafts: list, skipped: int)
    """
    print("\n" + "=" * 60)
    print("EMAIL BOT v2 -- Generating drafts (no auto-send)")
    print("=" * 60)

    leads = load_leads()
    if not leads:
        print("[INFO] No actionable leads found. Exiting.")
        return [], 0

    batch = leads[:MAX_DRAFTS_PER_RUN]
    print("[INFO] {} leads available, generating up to {} drafts".format(
        len(leads), MAX_DRAFTS_PER_RUN
    ))

    drafts = []
    skipped = 0

    for idx, lead in enumerate(batch, 1):
        name = lead.get("name", "Unknown")
        email = (lead.get("email") or "").strip()

        print("\n[{}/{}] Processing: {} <{}>".format(idx, len(batch), name, email))

        # Validate email
        is_valid, reason = validate_email(email)
        if not is_valid:
            print("  [SKIP] Invalid email ({}): {}".format(reason, email))
            _update_lead_status(name, "invalid_email")
            skipped += 1
            continue

        # Generate personalized email
        print("  [AI] Generating email draft...")
        email_content = generate_email(lead)
        subject = email_content.get("subject", "Helping your business online")
        body = email_content.get("body", "")

        if not body:
            print("  [SKIP] Empty email body generated -- skipping")
            skipped += 1
            continue

        # Add CAN-SPAM footer
        compliant_body = build_compliant_body(body)

        draft = {
            "lead_name": name,
            "to_email": email,
            "subject": subject,
            "body": compliant_body,
            "niche": lead.get("niche", ""),
            "city": lead.get("city", ""),
            "score": lead.get("score", 0),
            "rating": lead.get("rating", ""),
            "reviews": lead.get("reviews", ""),
            "website": lead.get("website", ""),
            "phone": lead.get("phone", ""),
            "source": lead.get("source", ""),
            "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "status": "pending_approval",
        }
        drafts.append(draft)
        mark_draft_ready(name)

        print("  [DRAFT] '{}' -> {}".format(subject[:50], email))

        # Small delay between AI calls to be nice to the API
        if idx < len(batch):
            time.sleep(1)

    # Save all drafts
    if drafts:
        save_drafts(drafts)

    print("\n" + "=" * 60)
    print("EMAIL BOT v2 COMPLETE: {} drafts generated, {} skipped".format(
        len(drafts), skipped
    ))
    print("Drafts are saved to {} -- awaiting Telegram approval".format(DRAFTS_FILE))
    print("=" * 60)

    return drafts, skipped


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    drafts, skipped = run_email_bot()
    print("\nFinal: {} drafts ready for approval, {} skipped".format(
        len(drafts), skipped
    ))
