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
import datetime
import requests

from config import (
    ANTHROPIC_API_KEY, ANTHROPIC_ENDPOINT, ANTHROPIC_MODEL,
    GMAIL_ADDRESS, MAX_DRAFTS_PER_RUN, CAN_SPAM_FOOTER,
    INDUSTRY_ANGLES, MIN_SCORE_FOR_DRAFT, FOLLOW_UP_RULES,
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

    prompt = """You are a friendly, professional web services consultant writing a cold outreach email to a local business.

BUSINESS INFO:
- Name: {name}
- Industry: {industry}
- City: {city}
- Website: {website}
- Rating: {rating} ({review_count} reviews)
- Industry pain points: {pain_points}{review_context}{website_context}{competition_context}

INSTRUCTIONS:
1. Write TWO different subject lines (labeled SUBJECT_A and SUBJECT_B). Make them different approaches:
   - Subject A: Direct/benefit-focused
   - Subject B: Question/curiosity-driven
2. Write ONE email body that:
   - Opens with a personalized observation about their business (use review quotes if available)
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

def validate_email(email):
    """Basic email validation with format + DNS check."""
    if not email or not isinstance(email, str):
        return False, "empty email"

    email = email.strip().lower()
    pattern = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "invalid format"

    domain = email.split("@")[1]
    fake_domains = {"example.com", "example.org", "test.com", "fake.com", "noemail.com", "none.com", "na.com"}
    if domain in fake_domains:
        return False, "fake domain"

    try:
        import socket
        socket.getaddrinfo(domain, None)
        return True, "valid"
    except socket.gaierror:
        return False, "domain does not resolve"
    except Exception:
        return True, "dns check skipped"


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
