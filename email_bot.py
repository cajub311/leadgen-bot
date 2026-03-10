"""
email_bot.py  -- LeadGen Bot v3
AI-powered cold email personalizer with CAN-SPAM compliance.
Features: industry-specific templates, A/B subject lines, 3-email drip sequences,
Google review personalization. Drafts via Claude -- does NOT auto-send.
"""

import os
import re
import json
import time
import csv
import datetime
import requests

import imaplib
import email as email_lib
from email.header import decode_header
import hashlib

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from config import (
    ANTHROPIC_API_KEY, ANTHROPIC_ENDPOINT, ANTHROPIC_MODEL,
    GMAIL_ADDRESS, GMAIL_APP_PASSWORD, MAX_DRAFTS_PER_RUN, CAN_SPAM_FOOTER,
    INDUSTRY_ANGLES, MIN_SCORE_FOR_DRAFT, FOLLOW_UP_RULES,
    EMAIL_WARMUP_SCHEDULE, EMAIL_MAX_PER_DAY_DEFAULT,
    EMAIL_SEND_DAYS, EMAIL_SEND_HOURS, EMAIL_SEND_TIMEZONE,
    EMAIL_WARMUP_START,
    TRACKING_PIXEL_BASE_URL, LINK_TRACKER_BASE_URL,
    AB_MIN_SENDS_FOR_WINNER, AB_WIN_THRESHOLD,
    ENGAGEMENT_WEIGHTS,
)
from sheets_client import (
    is_connected as sheets_connected,
    get_leads_by_stage, update_lead_multiple_fields,
    get_leads_needing_followup,
)


# ---------------------------------------------------------------------------
# Google Reviews scraping (for personalization)
# ---------------------------------------------------------------------------

def scrape_google_reviews(business_name, city):
    """Scrape a few Google reviews for personalization hooks."""
    try:
        import httpx
        from bs4 import BeautifulSoup
        from urllib.parse import quote_plus

        query = "{} {} reviews".format(business_name, city)
        url = "https://www.google.com/search?q={}".format(quote_plus(query))

        client = httpx.Client(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=15,
            follow_redirects=True,
        )
        resp = client.get(url)
        client.close()

        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        reviews = []

        # Look for review snippets in Google search results
        for span in soup.find_all("span", class_="review-snippet"):
            text = span.get_text(strip=True)
            if len(text) > 20:
                reviews.append(text[:200])

        # Also check for data-review-text attributes
        for div in soup.find_all(attrs={"data-review-id": True}):
            text = div.get_text(strip=True)
            if len(text) > 20:
                reviews.append(text[:200])

        # Fallback: look for quoted text that looks like reviews
        for q_tag in soup.find_all("q"):
            text = q_tag.get_text(strip=True)
            if len(text) > 20:
                reviews.append(text[:200])

        return reviews[:3]  # Max 3 reviews for context
    except Exception as e:
        print("[EMAIL] Review scraping failed for {}: {}".format(business_name, e))
        return []

# ---------------------------------------------------------------------------
# Industry detection
# ---------------------------------------------------------------------------

def detect_industry(niche):
    """Match a lead's niche to an industry template."""
    niche_lower = niche.lower().strip()

    # Direct match
    if niche_lower in INDUSTRY_ANGLES:
        return niche_lower

    # Fuzzy matching
    keyword_map = {
        "restaurant": ["restaurant", "cafe", "diner", "bistro", "eatery", "food", "pizza", "sushi", "bakery", "catering"],
        "hair salon": ["salon", "hair", "barber", "beauty", "spa", "nail"],
        "plumber": ["plumb", "pipe", "drain"],
        "electrician": ["electric", "wiring", "electrical"],
        "auto repair": ["auto", "car", "mechanic", "tire", "transmission", "body shop", "auto detailing"],
        "landscaping": ["landscape", "lawn", "garden", "tree", "mowing"],
        "cleaning service": ["clean", "maid", "janitorial", "housekeep"],
        "roofing contractor": ["roof", "gutter", "siding"],
        "hvac": ["hvac", "heating", "cooling", "furnace", "air condition"],
        "general contractor": ["contractor", "remodel", "renovation", "construction", "handyman"],
        "dentist": ["dentist", "dental", "orthodont"],
        "chiropractor": ["chiropractic", "chiropractor", "spine", "adjustment"],
    }

    for industry, keywords in keyword_map.items():
        for kw in keywords:
            if kw in niche_lower:
                return industry

    return "default"

# ---------------------------------------------------------------------------
# Claude API -- Email drafting
# ---------------------------------------------------------------------------

def _call_claude(prompt, max_tokens=1500):
    """Call Claude API and return response text."""
    if not ANTHROPIC_API_KEY:
        print("[EMAIL] No ANTHROPIC_API_KEY -- skipping AI generation")
        return None

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        resp = requests.post(ANTHROPIC_ENDPOINT, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]
    except requests.exceptions.RequestException as e:
        print("[EMAIL] Claude API error: {}".format(e))
        return None
    except (KeyError, IndexError) as e:
        print("[EMAIL] Claude response parse error: {}".format(e))
        return None

# ---------------------------------------------------------------------------
# Draft generation -- Initial outreach with A/B subjects
# ---------------------------------------------------------------------------

def generate_initial_draft(lead, reviews=None):
    """Generate initial cold email with A/B subject lines and industry-specific content."""
    name = lead.get("name", "Business Owner")
    niche = lead.get("niche", "")
    city = lead.get("city", "")
    website = lead.get("website", "")
    rating = lead.get("rating", "")
    review_count = lead.get("reviews", "")
    ssl = lead.get("website_ssl", "unknown")
    mobile = lead.get("website_mobile", "unknown")
    blog = lead.get("website_blog", "unknown")
    competition = lead.get("competition_density", "unknown")

    # Website content for hyper-personalization (RAG-lite)
    tagline = lead.get("tagline", "")
    services_list = lead.get("services_list", "")
    about_text = lead.get("about_text", "")

    industry = detect_industry(niche)
    angles = INDUSTRY_ANGLES.get(industry, INDUSTRY_ANGLES["default"])

    # Build context for Claude
    review_context = ""
    if reviews:
        review_context = "\nRecent customer reviews:\n" + "\n".join(["- \"{}\"".format(r) for r in reviews[:3]])

    website_issues = []
    if ssl == "no":
        website_issues.append("no SSL/HTTPS (shows 'Not Secure' in browsers)")
    if mobile == "no":
        website_issues.append("not mobile-optimized (loses 60% of traffic)")
    if blog == "no":
        website_issues.append("no blog or content section (missing SEO opportunity)")

    website_context = ""
    if website_issues:
        website_context = "\nWebsite issues found: " + "; ".join(website_issues)

    competition_context = ""
    if competition in ("high", "medium"):
        competition_context = "\nCompetition level: {} (they need to stand out)".format(competition)

    # Website content context (RAG-lite -- gives Claude real business intel)
    content_context = ""
    content_parts = []
    if tagline:
        content_parts.append("Tagline/description: {}".format(tagline[:200]))
    if services_list:
        content_parts.append("Services offered: {}".format(services_list[:300]))
    if about_text:
        # Truncate to keep prompt tokens reasonable
        content_parts.append("About the business: {}".format(about_text[:500]))
    if content_parts:
        content_context = "\n\nWEBSITE CONTENT (scraped from their site -- use for personalization):\n" + "\n".join(content_parts)

    prompt = """You are a friendly, professional web services consultant writing a cold outreach email to a local business.

BUSINESS INFO:
- Name: {name}
- Industry: {industry}
- City: {city}
- Website: {website}
- Rating: {rating} ({review_count} reviews)
- Industry pain points: {pain_points}{review_context}{website_context}{competition_context}{content_context}

INSTRUCTIONS:
1. Write TWO different subject lines (labeled SUBJECT_A and SUBJECT_B). Make them different approaches:
   - Subject A: Direct/benefit-focused
   - Subject B: Question/curiosity-driven
2. Write ONE email body that:
   - Opens with a personalized observation about their business (use their tagline, services, or about info if available -- this shows you actually visited their site)
   - Reference a SPECIFIC service they offer or something from their website content
   - Mentions 1-2 specific pain points relevant to their industry
   - If website issues were found, briefly mention ONE specific improvement
   - Keeps it under 150 words
   - Ends with a soft CTA (free audit, quick call, no pressure)
   - Tone: friendly, local, not salesy
   - Sign from: Charles G, Twin Cities Web Co
3. Do NOT include any greeting like "Dear" -- start with their business name or a hook

FORMAT YOUR RESPONSE EXACTLY LIKE THIS:
SUBJECT_A: [first subject line]
SUBJECT_B: [second subject line]
BODY:
[email body here]

Hook suggestion: {hook}""".format(
        name=name, industry=industry, city=city, website=website,
        rating=rating, review_count=review_count,
        pain_points=", ".join(angles["pain_points"]),
        review_context=review_context,
        website_context=website_context,
        competition_context=competition_context,
        content_context=content_context,
        hook=angles["hook"].format(name=name),
    )

    response = _call_claude(prompt)
    if not response:
        return None

    # Parse response
    draft = _parse_draft_response(response, lead)
    return draft

def _parse_draft_response(response, lead):
    """Parse Claude's response into structured draft dict."""
    subject_a = ""
    subject_b = ""
    body = ""

    lines = response.strip().split("\n")
    in_body = False
    body_lines = []

    for line in lines:
        if line.startswith("SUBJECT_A:"):
            subject_a = line.replace("SUBJECT_A:", "").strip()
        elif line.startswith("SUBJECT_B:"):
            subject_b = line.replace("SUBJECT_B:", "").strip()
        elif line.startswith("BODY:"):
            in_body = True
        elif in_body:
            body_lines.append(line)

    body = "\n".join(body_lines).strip()

    if not body:
        # Fallback: use entire response as body
        body = response.strip()
        subject_a = "Quick question about {}'s online presence".format(lead.get("name", "your business"))
        subject_b = "Noticed something about {} on Google".format(lead.get("name", "your business"))

    # Add CAN-SPAM footer
    body += CAN_SPAM_FOOTER

    return {
        "lead_name": lead.get("name", ""),
        "to_email": lead.get("email", ""),
        "from_email": GMAIL_ADDRESS,
        "subject_a": subject_a,
        "subject_b": subject_b,
        "body": body,
        "niche": lead.get("niche", ""),
        "city": lead.get("city", ""),
        "score": lead.get("score", 0),
        "industry": detect_industry(lead.get("niche", "")),
        "sequence_num": 1,
        "website_ssl": lead.get("website_ssl", ""),
        "website_mobile": lead.get("website_mobile", ""),
        "competition": lead.get("competition_density", ""),
        "generated_at": datetime.datetime.now().isoformat(),
    }

# ---------------------------------------------------------------------------
# Follow-up sequence generation
# ---------------------------------------------------------------------------

def generate_followup_draft(lead, sequence_num):
    """Generate follow-up email (sequence 2 or 3)."""
    name = lead.get("name", "Business Owner")
    niche = lead.get("niche", "")
    city = lead.get("city", "")
    industry = detect_industry(niche)
    angles = INDUSTRY_ANGLES.get(industry, INDUSTRY_ANGLES["default"])
    days_since = lead.get("days_since_contact", "a few")

    if sequence_num == 2:
        # Value-add follow-up
        prompt = """Write a SHORT follow-up email (sequence #{seq}) for a local business that hasn't replied to my initial outreach.

BUSINESS: {name} ({industry} in {city})
DAYS SINCE LAST EMAIL: {days}
INDUSTRY PAIN POINTS: {pain_points}

This is the VALUE-ADD follow-up. Include:
- Brief reference to your previous email (1 sentence)
- Share ONE specific, actionable tip they can implement themselves (related to their industry)
- Example: "One quick win: add your hours to Google My Business if you haven't -- businesses with complete profiles get 7x more clicks"
- Keep it under 100 words
- End with: "Happy to help if you'd like more ideas. No strings attached."
- Sign from: Charles G, Twin Cities Web Co

FORMAT:
SUBJECT: [follow-up subject line]
BODY:
[email body]""".format(
            seq=sequence_num, name=name, industry=industry, city=city,
            days=days_since, pain_points=", ".join(angles["pain_points"]),
        )
    else:
        # Break-up email (sequence 3)
        prompt = """Write a FINAL break-up email (sequence #{seq}) for a local business that hasn't replied to 2 previous emails.

BUSINESS: {name} ({industry} in {city})

This is the BREAK-UP email. Keep it:
- Ultra short (3-4 sentences max)
- Respectful and professional
- "I don't want to be a pest" tone
- Mention you won't email again unless they reach out
- Leave the door open: "If timing is ever right, I'm here"
- Sign from: Charles G, Twin Cities Web Co

FORMAT:
SUBJECT: [break-up subject line]
BODY:
[email body]""".format(seq=sequence_num, name=name, industry=industry, city=city)

    response = _call_claude(prompt, max_tokens=800)
    if not response:
        return None

    # Parse
    subject = ""
    body = ""
    lines = response.strip().split("\n")
    in_body = False
    body_lines = []

    for line in lines:
        if line.startswith("SUBJECT:"):
            subject = line.replace("SUBJECT:", "").strip()
        elif line.startswith("BODY:"):
            in_body = True
        elif in_body:
            body_lines.append(line)

    body = "\n".join(body_lines).strip()
    if not body:
        body = response.strip()
        subject = "Following up - {}".format(name)

    body += CAN_SPAM_FOOTER

    return {
        "lead_name": name,
        "to_email": lead.get("email", ""),
        "from_email": GMAIL_ADDRESS,
        "subject_a": subject,
        "subject_b": subject,  # Same subject for follow-ups
        "body": body,
        "niche": niche,
        "city": city,
        "score": lead.get("score", 0),
        "industry": industry,
        "sequence_num": sequence_num,
        "generated_at": datetime.datetime.now().isoformat(),
    }

# ---------------------------------------------------------------------------
# Email validation
# ---------------------------------------------------------------------------

# MX record cache to avoid repeated DNS lookups within a single run
_mx_cache = {}

def check_mx_records(domain):
    """Check if a domain has valid MX records using dnspython.
    Returns (has_mx: bool, detail: str). Caches results per-run."""
    if domain in _mx_cache:
        return _mx_cache[domain]

    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, "MX")
        mx_hosts = [str(r.exchange).rstrip(".").lower() for r in answers]
        # Filter out null MX (RFC 7505 -- domain explicitly refuses email)
        mx_hosts = [h for h in mx_hosts if h and h != "."]
        if mx_hosts:
            result = (True, "mx:{}".format(mx_hosts[0]))
        else:
            result = (False, "null MX (domain refuses email)")
    except dns.resolver.NoAnswer:
        # No MX record at all -- fall back to A record check
        try:
            dns.resolver.resolve(domain, "A")
            result = (True, "no MX but has A record (implicit MX)")
        except Exception:
            result = (False, "no MX and no A record")
    except dns.resolver.NXDOMAIN:
        result = (False, "domain does not exist (NXDOMAIN)")
    except dns.resolver.NoNameservers:
        result = (False, "no nameservers for domain")
    except dns.resolver.LifetimeTimeout:
        result = (True, "MX lookup timed out (assuming valid)")
    except Exception as e:
        # dnspython not installed or other error -- fall back to socket
        try:
            import socket
            socket.getaddrinfo(domain, None)
            result = (True, "fallback socket check passed")
        except Exception:
            result = (False, "domain does not resolve")

    _mx_cache[domain] = result
    return result

def validate_email(email):
    """Email validation with format check + MX record verification.
    Uses dnspython for proper mail server detection. Catches dead domains
    before wasting Claude API tokens on drafting."""
    if not email or not isinstance(email, str):
        return False, "empty email"

    email = email.strip().lower()
    pattern = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "invalid format"

    domain = email.split("@")[1]
    fake_domains = {"example.com", "example.org", "test.com", "fake.com",
                    "noemail.com", "none.com", "na.com", "noreply.com",
                    "nowhere.com", "invalid.com", "mailinator.com"}
    if domain in fake_domains:
        return False, "fake/disposable domain"

    # Proper MX record check -- catches domains that resolve but accept no email
    has_mx, detail = check_mx_records(domain)
    if not has_mx:
        return False, "no mail server: {}".format(detail)

    return True, "valid ({})".format(detail)

# ---------------------------------------------------------------------------
# Gmail reply tracking via IMAP
# ---------------------------------------------------------------------------

def check_gmail_replies(contacted_leads):
    """
    Check Gmail inbox for replies to outreach emails.
    Uses IMAP to search for messages FROM any lead's email address.
    Returns list of dicts: [{"name": ..., "city": ..., "reply_subject": ..., "reply_date": ...}]
    """
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        print("[REPLY-TRACK] Skipping -- GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set")
        return []

    if not contacted_leads:
        print("[REPLY-TRACK] No contacted leads to check")
        return []

    # Build lookup: email -> lead info
    email_to_lead = {}
    for lead in contacted_leads:
        lead_email = lead.get("email", "").strip().lower()
        if lead_email:
            email_to_lead[lead_email] = {
                "name": lead.get("name", ""),
                "city": lead.get("city", ""),
            }

    if not email_to_lead:
        print("[REPLY-TRACK] No valid email addresses in contacted leads")
        return []

    replies_found = []

    try:
        print("[REPLY-TRACK] Connecting to Gmail IMAP...")
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select("INBOX", readonly=True)

        # Search for replies from each contacted lead's email
        for lead_email, lead_info in email_to_lead.items():
            try:
                # Search for emails FROM this lead address in the last 30 days
                since_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%d-%b-%Y")
                search_criteria = '(FROM "{}" SINCE {})'.format(lead_email, since_date)
                status, message_ids = mail.search(None, search_criteria)

                if status != "OK" or not message_ids[0]:
                    continue

                # Found replies from this lead
                ids = message_ids[0].split()
                # Just grab the most recent reply
                latest_id = ids[-1]
                status, msg_data = mail.fetch(latest_id, "(RFC822)")

                if status != "OK":
                    continue

                raw_email = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw_email)

                # Decode subject
                subject_header = msg.get("Subject", "(no subject)")
                decoded_parts = decode_header(subject_header)
                subject = ""
                for part, encoding in decoded_parts:
                    if isinstance(part, bytes):
                        subject += part.decode(encoding or "utf-8", errors="replace")
                    else:
                        subject += part

                reply_date = msg.get("Date", "")

                # Check if the reply is an unsubscribe request
                body_text = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True)
                            if payload:
                                body_text = payload.decode("utf-8", errors="replace")
                                break
                else:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        body_text = payload.decode("utf-8", errors="replace")

                is_unsub = any(kw in body_text.lower() for kw in [
                    "unsubscribe", "stop emailing", "remove me",
                    "opt out", "opt-out", "do not contact",
                    "take me off", "no more emails",
                ]) or any(kw in subject.lower() for kw in [
                    "unsubscribe", "stop", "remove",
                ])

                replies_found.append({
                    "name": lead_info["name"],
                    "city": lead_info["city"],
                    "email": lead_email,
                    "reply_subject": subject[:100],
                    "reply_date": reply_date,
                    "is_unsubscribe": is_unsub,
                })

                print("[REPLY-TRACK] Reply found from {} <{}>".format(
                    lead_info["name"], lead_email))

            except Exception as e:
                print("[REPLY-TRACK] Error checking {}: {}".format(lead_email, e))
                continue

        mail.logout()
        print("[REPLY-TRACK] Done -- {} replies found".format(len(replies_found)))

    except imaplib.IMAP4.error as e:
        print("[REPLY-TRACK] IMAP login failed: {} -- check GMAIL_APP_PASSWORD".format(e))
    except Exception as e:
        print("[REPLY-TRACK] Error: {}".format(e))

    return replies_found

# ---------------------------------------------------------------------------
# Email Deliverability Helpers
# ---------------------------------------------------------------------------

def get_daily_send_limit():
    """
    Calculate today's send limit based on warm-up schedule.
    Returns max emails allowed today.
    """
    if not EMAIL_WARMUP_START:
        return EMAIL_WARMUP_SCHEDULE.get(1, 5)  # Default to week 1 if no start date

    try:
        start_date = datetime.datetime.strptime(EMAIL_WARMUP_START, "%Y-%m-%d").date()
        days_active = (datetime.date.today() - start_date).days
        week_number = (days_active // 7) + 1

        if week_number > max(EMAIL_WARMUP_SCHEDULE.keys()):
            return EMAIL_MAX_PER_DAY_DEFAULT

        return EMAIL_WARMUP_SCHEDULE.get(week_number, EMAIL_MAX_PER_DAY_DEFAULT)
    except (ValueError, TypeError):
        return EMAIL_WARMUP_SCHEDULE.get(1, 5)

def is_send_window_open():
    """
    Check if current time is within the optimal send window.
    Returns True if it's a good day+hour to send, False otherwise.
    """
    try:
        tz = ZoneInfo(EMAIL_SEND_TIMEZONE)
        now = datetime.datetime.now(tz)

        # Check day of week (0=Monday)
        if now.weekday() not in EMAIL_SEND_DAYS:
            print("[DELIVER] Not a send day (today={}, allowed={})".format(
                now.strftime("%A"), EMAIL_SEND_DAYS))
            return False

        # Check hour
        start_hour, end_hour = EMAIL_SEND_HOURS
        if not (start_hour <= now.hour < end_hour):
            print("[DELIVER] Outside send window (now={}h, window={}-{}h CT)".format(
                now.hour, start_hour, end_hour))
            return False

        return True
    except Exception as e:
        print("[DELIVER] Send window check failed: {}, allowing send".format(e))
        return True  # Fail open

def detect_bounce(msg):
    """
    Check if an email message is a bounce notification.
    Returns dict with bounce info or None if not a bounce.
    """
    # Common bounce indicators
    from_addr = str(msg.get("From", "")).lower()
    subject = str(msg.get("Subject", "")).lower()

    bounce_senders = [
        "mailer-daemon", "postmaster", "mail-daemon",
        "noreply", "no-reply", "bounce",
    ]
    bounce_subjects = [
        "undeliverable", "delivery status", "mail delivery failed",
        "returned mail", "delivery failure", "undelivered",
        "message not delivered", "delivery problem",
        "permanent failure", "mailbox not found",
        "address rejected", "user unknown",
    ]

    is_bounce = (
        any(sender in from_addr for sender in bounce_senders) and
        any(subj in subject for subj in bounce_subjects)
    )

    if not is_bounce:
        return None

    # Try to extract the original recipient from bounce body
    body_text = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body_text = payload.decode("utf-8", errors="replace")
                    break
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body_text = payload.decode("utf-8", errors="replace")

    # Extract bounced email address from body
    import re as _re
    email_match = _re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", body_text[:2000])
    bounced_email = email_match.group(0) if email_match else ""

    return {
        "type": "bounce",
        "bounced_email": bounced_email,
        "subject": subject[:100],
        "date": str(msg.get("Date", "")),
    }

def check_spf_dkim_warning():
    """
    Print a warning at startup if SPF/DKIM might not be configured.
    This is advisory only -- we can't verify from the sender side.
    """
    print("[DELIVER] === DELIVERABILITY CHECKLIST ===")
    print("[DELIVER] Ensure your Gmail account has:")
    print("[DELIVER]   1. SPF record configured for your domain")
    print("[DELIVER]   2. DKIM signing enabled")
    print("[DELIVER]   3. DMARC policy set")
    print("[DELIVER]   4. Gmail App Password (not regular password)")
    if EMAIL_WARMUP_START:
        limit = get_daily_send_limit()
        print("[DELIVER] Warm-up active since {}. Today's limit: {} emails/day".format(
            EMAIL_WARMUP_START, limit))
    else:
        print("[DELIVER] WARNING: EMAIL_WARMUP_START not set. Defaulting to {} emails/day".format(
            EMAIL_WARMUP_SCHEDULE.get(1, 5)))
    print("[DELIVER] ================================")

# ---------------------------------------------------------------------------
# Gmail SMTP -- Send approved emails
# ---------------------------------------------------------------------------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------------------------------------------------------------------------
# Open / Click Tracking
# ---------------------------------------------------------------------------

def _lead_hash(email_addr):
    """Generate a short hash for tracking pixel identification."""
    return hashlib.sha256(email_addr.encode()).hexdigest()[:12]

def _inject_tracking_pixel(html_body, lead_email):
    """Inject a 1x1 tracking pixel into email HTML body.
    Returns (modified_body, pixel_url) or (original_body, None) if not configured."""
    if not TRACKING_PIXEL_BASE_URL:
        return html_body, None

    lid = _lead_hash(lead_email)
    pixel_url = "{}?lid={}".format(TRACKING_PIXEL_BASE_URL, lid)
    pixel_tag = '<img src="{}" width="1" height="1" alt="" style="display:none;" />'.format(pixel_url)

    # Append pixel at the end of the body
    tracked_body = html_body + "\n" + pixel_tag
    return tracked_body, pixel_url

def _wrap_links_for_tracking(html_body, lead_email):
    """Wrap http/https links in the email body for click tracking.
    Returns modified body with links going through the tracker redirect."""
    if not LINK_TRACKER_BASE_URL:
        return html_body

    lid = _lead_hash(lead_email)

    def replace_link(match):
        original_url = match.group(1)
        # Don't wrap unsubscribe links or tracking pixels
        if "unsubscribe" in original_url.lower() or TRACKING_PIXEL_BASE_URL in original_url:
            return match.group(0)
        from urllib.parse import quote
        tracked = "{}?url={}&lid={}".format(LINK_TRACKER_BASE_URL, quote(original_url), lid)
        return 'href="{}"'.format(tracked)

    import re as _re
    return _re.sub(r'href="(https?://[^"]+)"', replace_link, html_body)

def get_ab_winner(contacted_leads):
    """Analyze A/B subject line performance and return the winning variant.

    Returns dict with:
        winner: "A" or "B" or None (not enough data)
        stats: {variant: {sent, opens, clicks, replies, open_rate, engagement_score}}
        confidence: "high", "low", or "insufficient"
    """
    stats = {
        "A": {"sent": 0, "opens": 0, "clicks": 0, "replies": 0},
        "B": {"sent": 0, "opens": 0, "clicks": 0, "replies": 0},
    }

    for lead in contacted_leads:
        variant = lead.get("subject_variant_used", lead.get("template_variant", ""))
        if variant not in ("A", "B"):
            continue
        stats[variant]["sent"] += 1
        open_count = int(lead.get("open_count", 0) or 0)
        click_count = int(lead.get("click_count", 0) or 0)
        if open_count > 0:
            stats[variant]["opens"] += 1
        if click_count > 0:
            stats[variant]["clicks"] += 1
        if lead.get("reply_received", "").lower() in ("yes", "true", "1"):
            stats[variant]["replies"] += 1

    # Calculate rates
    for v in ("A", "B"):
        s = stats[v]
        s["open_rate"] = round(s["opens"] / s["sent"] * 100, 1) if s["sent"] > 0 else 0
        s["click_rate"] = round(s["clicks"] / s["sent"] * 100, 1) if s["sent"] > 0 else 0
        s["reply_rate"] = round(s["replies"] / s["sent"] * 100, 1) if s["sent"] > 0 else 0
        s["engagement_score"] = (
            s["opens"] * ENGAGEMENT_WEIGHTS["open"] +
            s["clicks"] * ENGAGEMENT_WEIGHTS["click"] +
            s["replies"] * ENGAGEMENT_WEIGHTS["reply"]
        )

    # Determine winner
    result = {"winner": None, "stats": stats, "confidence": "insufficient"}

    if stats["A"]["sent"] < AB_MIN_SENDS_FOR_WINNER or stats["B"]["sent"] < AB_MIN_SENDS_FOR_WINNER:
        return result

    score_a = stats["A"]["engagement_score"]
    score_b = stats["B"]["engagement_score"]

    if score_a == 0 and score_b == 0:
        return result

    max_score = max(score_a, score_b)
    min_score = min(score_a, score_b)
    margin = (max_score - min_score) / max_score if max_score > 0 else 0

    if margin >= AB_WIN_THRESHOLD:
        result["winner"] = "A" if score_a > score_b else "B"
        result["confidence"] = "high" if margin >= 0.3 else "low"
    else:
        result["confidence"] = "low"  # Too close to call

    return result

def select_subject_variant(draft, ab_result=None):
    """Choose which subject line variant to use for sending.
    If A/B test has a winner, always use winner. Otherwise alternate."""
    if ab_result and ab_result.get("winner"):
        winner = ab_result["winner"]
        return draft.get("subject_{}".format(winner.lower()), draft.get("subject_a", "")), winner

    # Default: alternate based on even/odd timestamp
    import time
    if int(time.time()) % 2 == 0:
        return draft.get("subject_a", ""), "A"
    else:
        return draft.get("subject_b", draft.get("subject_a", "")), "B"

def send_approved_email(draft):
    """
    Send a single approved email via Gmail SMTP.
    Uses GMAIL_ADDRESS + GMAIL_APP_PASSWORD from config.
    Returns dict with send result or None on failure.
    """
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        print("[SEND] Skipping -- GMAIL_ADDRESS or GMAIL_APP_PASSWORD not set")
        return None

    to_email = draft.get("to_email", "")
    if not to_email:
        print("[SEND] No recipient email in draft for {}".format(draft.get("lead_name", "?")))
        return None

    # Use A/B winner if available, otherwise alternate
    ab_result = draft.get("_ab_result", None)
    subject, subject_variant = select_subject_variant(draft, ab_result)
    if not subject:
        subject = "Quick question about your business"
    body = draft.get("body", "")

    if not body:
        print("[SEND] Empty body for {}".format(draft.get("lead_name", "?")))
        return None

    try:
        # Build MIME message
        msg = MIMEMultipart("alternative")
        msg["From"] = GMAIL_ADDRESS
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = GMAIL_ADDRESS

        # Plain text version
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # HTML version with tracking pixel + link tracking
        html_body = "<html><body><pre style='font-family: Arial, sans-serif; white-space: pre-wrap;'>{}</pre>".format(
            body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )
        html_body, pixel_url = _inject_tracking_pixel(html_body, to_email)
        html_body = _wrap_links_for_tracking(html_body + "</body></html>", to_email)
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        # Connect and send
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_ADDRESS, to_email, msg.as_string())
        server.quit()

        print("[SEND] Sent to {} <{}>".format(draft.get("lead_name", "?"), to_email))

        return {
            "name": draft.get("lead_name", ""),
            "email": to_email,
            "niche": draft.get("niche", ""),
            "city": draft.get("city", ""),
            "score": draft.get("score", ""),
            "sent_date": datetime.date.today().isoformat(),
            "subject": subject,
            "sequence_num": str(draft.get("sequence_num", 1)),
            "template_variant": subject_variant,
            "reply_received": "",
            "reply_date": "",
            "subject_variant_used": subject_variant,
            "open_count": "0",
            "first_open_date": "",
            "click_count": "0",
            "first_click_date": "",
            "engagement_score": "0",
        }

    except smtplib.SMTPAuthenticationError as e:
        print("[SEND] Auth failed -- check GMAIL_APP_PASSWORD: {}".format(e))
        return None
    except smtplib.SMTPRecipientsRefused as e:
        print("[SEND] Recipient refused {}: {}".format(to_email, e))
        return None
    except Exception as e:
        print("[SEND] Error sending to {}: {}".format(to_email, e))
        return None

def send_approved_emails(approved_drafts):
    """
    Send all approved email drafts via Gmail SMTP.
    Enforces daily send limits (warm-up schedule) and optimal send windows.
    Returns (sent_records, failed_count, skipped_count).
    """
    if not approved_drafts:
        print("[SEND] No approved drafts to send")
        return [], 0, 0

    # Check send window
    if not is_send_window_open():
        print("[SEND] Outside send window -- queuing {} drafts for next window".format(
            len(approved_drafts)))
        return [], 0, len(approved_drafts)

    # Enforce daily limit
    daily_limit = get_daily_send_limit()
    if len(approved_drafts) > daily_limit:
        print("[SEND] Rate limiting: {} drafts but limit is {}/day".format(
            len(approved_drafts), daily_limit))
        approved_drafts = approved_drafts[:daily_limit]

    # Run A/B analysis to determine winner (if enough data)
    try:
        contacted = get_leads_by_stage("contacted")
        ab_result = get_ab_winner(contacted)
        if ab_result["winner"]:
            print("[SEND] A/B winner: Variant {} (confidence: {})".format(
                ab_result["winner"], ab_result["confidence"]))
        # Inject A/B result into each draft so send_approved_email can use it
        for d in approved_drafts:
            d["_ab_result"] = ab_result
    except Exception as e:
        print("[SEND] A/B analysis skipped: {}".format(e))

    sent_records = []
    failed = 0

    for draft in approved_drafts:
        result = send_approved_email(draft)
        if result:
            sent_records.append(result)
        else:
            failed += 1

        # Brief pause between sends to avoid rate limits
        time.sleep(2)

    skipped = max(0, len(approved_drafts) - len(sent_records) - failed)
    print("[SEND] Sent {}/{} emails ({} failed, {} skipped)".format(
        len(sent_records), len(approved_drafts), failed, skipped))
    return sent_records, failed, skipped

# ---------------------------------------------------------------------------
# Main email bot pipeline
# ---------------------------------------------------------------------------

def run_email_bot():
    """
    Generate email drafts for qualified leads.
    Returns: (drafts_list, skipped_count)
    """
    # Get qualified leads from Sheets
    if sheets_connected():
        leads = get_leads_by_stage("qualified")
    else:
        # CSV fallback
        leads = _load_csv_leads()

    print("[EMAIL] Found {} qualified leads for drafting".format(len(leads)))

    drafts = []
    skipped = 0
    processed = 0

    for lead in leads:
        if processed >= MAX_DRAFTS_PER_RUN:
            print("[EMAIL] Hit max drafts per run ({})".format(MAX_DRAFTS_PER_RUN))
            break

        email = lead.get("email", "")
        name = lead.get("name", "Unknown")

        # Validate email
        is_valid, reason = validate_email(email)
        if not is_valid:
            print("[EMAIL] Skipping {} -- {} ({})".format(name, reason, email))
            skipped += 1
            continue

        # Scrape reviews for personalization
        reviews = scrape_google_reviews(name, lead.get("city", ""))

        # Generate initial draft with A/B subjects
        draft = generate_initial_draft(lead, reviews)
        if draft:
            drafts.append(draft)
            processed += 1

            # Update lead stage in Sheets
            if sheets_connected():
                update_lead_multiple_fields(
                    name, lead.get("city", ""),
                    {
                        "pipeline_stage": "draft_ready",
                        "subject_line_a": draft.get("subject_a", ""),
                        "subject_line_b": draft.get("subject_b", ""),
                    }
                )

            # Rate limit Claude calls
            time.sleep(1)
        else:
            skipped += 1

    print("[EMAIL] Generated {} drafts, skipped {}".format(len(drafts), skipped))
    return drafts, skipped

def run_followup_bot():
    """
    Generate follow-up drafts for leads that haven't replied.
    Returns: (followup_drafts_list, skipped_count)
    """
    if not sheets_connected():
        print("[EMAIL] Sheets not connected -- skipping follow-ups")
        return [], 0

    rules = FOLLOW_UP_RULES
    followup_drafts = []
    skipped = 0

    # Check for leads needing first follow-up (3+ days since contact)
    leads_f1 = get_leads_needing_followup(rules["first_follow_up_days"])
    leads_f1 = [l for l in leads_f1 if l.get("pipeline_stage") == "contacted"]

    # Check for leads needing second follow-up (7+ days since last follow-up)
    leads_f2 = get_leads_needing_followup(rules["second_follow_up_days"])
    leads_f2 = [l for l in leads_f2 if l.get("pipeline_stage") == "follow_up_1"]

    print("[EMAIL] Follow-up candidates: {} first, {} second".format(len(leads_f1), len(leads_f2)))

    for lead in leads_f1:
        email = lead.get("email", "")
        is_valid, _ = validate_email(email)
        if not is_valid:
            skipped += 1
            continue

        draft = generate_followup_draft(lead, sequence_num=2)
        if draft:
            followup_drafts.append(draft)
            time.sleep(1)
        else:
            skipped += 1

    for lead in leads_f2:
        email = lead.get("email", "")
        is_valid, _ = validate_email(email)
        if not is_valid:
            skipped += 1
            continue

        draft = generate_followup_draft(lead, sequence_num=3)
        if draft:
            followup_drafts.append(draft)
            time.sleep(1)
        else:
            skipped += 1

    print("[EMAIL] Generated {} follow-up drafts, skipped {}".format(
        len(followup_drafts), skipped))
    return followup_drafts, skipped

# ---------------------------------------------------------------------------
# CSV fallback
# ---------------------------------------------------------------------------

def _load_csv_leads():
    """Load qualified leads from CSV fallback."""
    csv_file = "leads.csv"
    if not os.path.exists(csv_file):
        return []

    leads = []
    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("status") == "qualified":
                    leads.append(row)
    except Exception as e:
        print("[EMAIL] CSV load error: {}".format(e))

    return leads
