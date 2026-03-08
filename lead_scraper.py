"""
lead_scraper.py
Google Maps lead scraper using Outscraper API.
Scores and filters leads for local service businesses in Saint Paul/Minneapolis MN.
"""

import os
import csv
import time
import datetime
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTSCRAPER_API_KEY = os.getenv("OUTSCRAPER_API_KEY", "")

TARGET_SEARCHES = [
    "plumber Saint Paul MN",
    "electrician Minneapolis MN",
    "auto repair Saint Paul MN",
    "landscaping Minneapolis MN",
    "cleaning service Saint Paul MN",
    "restaurant Minneapolis MN",
    "hair salon Saint Paul MN",
    "roofing contractor Minneapolis MN",
    "HVAC Saint Paul MN",
    "general contractor Minneapolis MN",
]

LEADS_FILE = "leads.csv"
CONTACTED_FILE = "contacted.csv"

LEADS_COLUMNS = [
    "name", "address", "phone", "website", "rating",
    "reviews", "email", "score", "reason", "niche",
    "city", "scraped_date", "status",
]

OUTSCRAPER_ENDPOINT = "https://api.app.outscraper.com/maps/search-v3"
OUTSCRAPER_FIELDS = "name,full_address,phone,site,rating,reviews,emails_and_contacts"


# ---------------------------------------------------------------------------
# Outscraper API call
# ---------------------------------------------------------------------------

def scrape_google_maps(query, limit=20):
    """
    Call the Outscraper Maps Search v3 API for a given query.
    Returns a flat list of business result dicts.
    Falls back to demo data if no API key is configured.
    """
    if not OUTSCRAPER_API_KEY:
        print("  [WARN] No OUTSCRAPER_API_KEY - using demo data for: " + query)
        return _demo_data(query)

    headers = {
        "X-API-KEY": OUTSCRAPER_API_KEY,
        "Content-Type": "application/json",
    }
    params = {
        "query": query,
        "limit": limit,
        "fields": OUTSCRAPER_FIELDS,
        "async": False,
    }

    try:
        response = requests.get(
            OUTSCRAPER_ENDPOINT,
            headers=headers,
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()

        # Outscraper returns {"data": [[...results...]]} or {"data": [...results...]}
        raw = data.get("data", [])
        if raw and isinstance(raw[0], list):
            raw = raw[0]  # unwrap nested list

        print("  [OK] {} results for: {}".format(len(raw), query))
        return raw

    except requests.exceptions.RequestException as exc:
        print("  [ERROR] Outscraper request failed for '{}': {}".format(query, exc))
        return []


# ---------------------------------------------------------------------------
# Demo / fallback data
# ---------------------------------------------------------------------------

def _demo_data(query):
    """
    Return a small set of realistic-looking demo records so the pipeline
    can be tested without an Outscraper API key.
    """
    niche = query.split(" ")[0]
    city = "Saint Paul" if "Saint Paul" in query else "Minneapolis"
    base = [
        {
            "name": niche.title() + " Pro " + city,
            "full_address": "123 Main St, " + city + ", MN 55101",
            "phone": "+16515550101",
            "site": "",
            "rating": 3.2,
            "reviews": 14,
            "emails_and_contacts": {"emails": ["owner@example.com"]},
        },
        {
            "name": "Quality " + niche.title() + " LLC",
            "full_address": "456 Oak Ave, " + city + ", MN 55104",
            "phone": "+16515550102",
            "site": "http://qualityexample.com",
            "rating": 4.8,
            "reviews": 312,
            "emails_and_contacts": {},
        },
        {
            "name": "Fast " + niche.title() + " Services",
            "full_address": "789 Elm Blvd, " + city + ", MN 55106",
            "phone": "+16515550103",
            "site": "",
            "rating": 2.9,
            "reviews": 7,
            "emails_and_contacts": {"emails": ["info@fastservice.example"]},
        },
        {
            "name": "Metro " + niche.title() + " Group",
            "full_address": "321 Cedar Rd, " + city + ", MN 55117",
            "phone": "+16515550104",
            "site": "",
            "rating": 4.1,
            "reviews": 38,
            "emails_and_contacts": {},
        },
        {
            "name": "Reliable " + niche.title() + " Co",
            "full_address": "654 Birch Ln, " + city + ", MN 55108",
            "phone": "+16515550105",
            "site": "http://reliableexample.com",
            "rating": 3.7,
            "reviews": 22,
            "emails_and_contacts": {"emails": ["contact@reliable.example"]},
        },
    ]
    return base


# ---------------------------------------------------------------------------
# Lead scoring
# ---------------------------------------------------------------------------

def score_lead(lead):
    """
    Score a raw Outscraper result from 0-100.

    Scoring rules:
      +25  No website listed
      +20  Rating < 3.5  (bad reputation - needs reputation management)
      +10  Rating < 4.2  (mediocre - still improvable)
      +10  Review count between 1 and 50 (small, reachable business)
      +15  Email address found

    Disqualification rules (returns score=0):
      - reviews > 200  (too established)
      - reviews == 0   (possibly closed/fake listing)

    Qualification threshold: score >= 60
    Returns (score: int, reason: str)
    """
    score = 0
    reasons = []

    rating = lead.get("rating") or 0
    reviews = lead.get("reviews") or 0
    website = (lead.get("website") or "").strip()
    email = (lead.get("email") or "").strip()

    # Disqualify first
    if reviews == 0:
        return 0, "disqualified: zero reviews"
    if reviews > 200:
        return 0, "disqualified: too many reviews (>200)"

    # Positive signals
    if not website:
        score += 25
        reasons.append("no website (+25)")

    if rating < 3.5:
        score += 20
        reasons.append("rating {} < 3.5 (+20)".format(rating))
    elif rating < 4.2:
        score += 10
        reasons.append("rating {} < 4.2 (+10)".format(rating))

    if 1 <= reviews <= 50:
        score += 10
        reasons.append("{} reviews in 1-50 range (+10)".format(reviews))

    if email:
        score += 15
        reasons.append("email found (+15)")

    reason_str = "; ".join(reasons) if reasons else "no qualifying signals"
    return score, reason_str


# ---------------------------------------------------------------------------
# Contacted list
# ---------------------------------------------------------------------------

def load_contacted():
    """
    Read contacted.csv and return a set of business names already contacted,
    to avoid duplicate outreach.
    """
    contacted = set()
    if not os.path.exists(CONTACTED_FILE):
        return contacted
    try:
        with open(CONTACTED_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("name") or "").strip().lower()
                if name:
                    contacted.add(name)
    except Exception as exc:
        print("[WARN] Could not read {}: {}".format(CONTACTED_FILE, exc))
    return contacted


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_email(raw_lead):
    """Pull the first email address out of the emails_and_contacts field."""
    contacts = raw_lead.get("emails_and_contacts") or {}
    if isinstance(contacts, dict):
        emails = contacts.get("emails") or []
        if emails:
            return emails[0]
    elif isinstance(contacts, list):
        for item in contacts:
            if isinstance(item, dict):
                emails = item.get("emails") or []
                if emails:
                    return emails[0]
    return ""


def _city_from_address(address):
    """Best-effort city extraction from a full address string."""
    if "Saint Paul" in address:
        return "Saint Paul"
    if "Minneapolis" in address:
        return "Minneapolis"
    return "MN"


def _niche_from_query(query):
    """Return the first word of the search query as the niche label."""
    return query.split(" ")[0].lower()


def _save_leads(leads):
    """Write (or append to) leads.csv."""
    file_exists = os.path.exists(LEADS_FILE)
    with open(LEADS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEADS_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(leads)


# ---------------------------------------------------------------------------
# Main scraper function
# ---------------------------------------------------------------------------

def run_scraper():
    """
    Full scrape pipeline:
      1. Loop through TARGET_SEARCHES
      2. Call Outscraper (or demo fallback)
      3. Score each result
      4. Filter: qualified score >= 60, not already contacted, not duplicate
      5. Save qualified leads to leads.csv
      6. Return the list of qualified lead dicts
    """
    contacted = load_contacted()
    today = datetime.date.today().isoformat()
    all_leads = []
    seen_names = set()  # dedupe within this run

    for query in TARGET_SEARCHES:
        print("\n[SCRAPE] " + query)
        results = scrape_google_maps(query)
        niche = _niche_from_query(query)

        for raw in results:
            name = (raw.get("name") or "").strip()
            if not name:
                continue

            name_key = name.lower()

            # Skip already contacted
            if name_key in contacted:
                print("  [SKIP] Already contacted: " + name)
                continue

            # Skip duplicates within this run
            if name_key in seen_names:
                continue
            seen_names.add(name_key)

            # Extract / normalize fields
            email = _extract_email(raw)
            website = (raw.get("site") or "").strip()
            address = (raw.get("full_address") or "").strip()
            city = _city_from_address(address)
            rating = raw.get("rating") or 0
            reviews = raw.get("reviews") or 0

            # Score (pass website and email explicitly)
            lead_for_scoring = dict(raw)
            lead_for_scoring["website"] = website
            lead_for_scoring["email"] = email
            score, reason = score_lead(lead_for_scoring)

            if score < 60:
                print("  [LOW]  score={:3d} - {} ({})".format(score, name, reason))
                continue

            lead = {
                "name": name,
                "address": address,
                "phone": (raw.get("phone") or "").strip(),
                "website": website,
                "rating": rating,
                "reviews": reviews,
                "email": email,
                "score": score,
                "reason": reason,
                "niche": niche,
                "city": city,
                "scraped_date": today,
                "status": "new",
            }
            all_leads.append(lead)
            print("  [LEAD] score={:3d} - {} | {}".format(score, name, email or "no email"))

        # Be polite to the API between queries
        time.sleep(1)

    # Sort by score descending
    all_leads.sort(key=lambda x: x["score"], reverse=True)

    # Persist to CSV
    if all_leads:
        _save_leads(all_leads)
        print("\n[DONE] {} qualified leads saved to {}".format(len(all_leads), LEADS_FILE))
    else:
        print("\n[DONE] No qualified leads found this run.")

    return all_leads


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("LEAD SCRAPER - Saint Paul / Minneapolis MN")
    print("=" * 60)

    leads = run_scraper()

    print("\n--- TOP 5 LEADS ---")
    for i, lead in enumerate(leads[:5], 1):
        print("{i}. {name}".format(i=i, name=lead["name"]))
        print("   Niche   : " + lead["niche"])
        print("   City    : " + lead["city"])
        print("   Score   : " + str(lead["score"]))
        print("   Rating  : {} ({} reviews)".format(lead["rating"], lead["reviews"]))
        print("   Website : " + (lead["website"] or "(none)"))
        print("   Email   : " + (lead["email"] or "(none)"))
        print("   Reason  : " + lead["reason"])
        print("")
