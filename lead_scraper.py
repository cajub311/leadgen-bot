"""
lead_scraper.py
Free lead scraper using Google Maps + Yelp via httpx + BeautifulSoup.
Scores and filters leads for local service businesses in Saint Paul/Minneapolis MN.
Zero API cost -- all scraping is done via public HTTP requests.
"""

import os
import re
import csv
import time
import json
import random
import datetime
import httpx
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urljoin

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

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
    "city", "scraped_date", "status", "source",
]

# Rotate user agents to avoid blocks
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Emails to ignore (generic / spam traps)
IGNORE_EMAIL_DOMAINS = {
    "example.com", "example.org", "test.com", "sentry.io",
    "wixpress.com", "squarespace.com", "godaddy.com",
    "googleapis.com", "googleusercontent.com", "gstatic.com",
    "w3.org", "schema.org", "wordpress.org", "jquery.com",
}

IGNORE_EMAIL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "postmaster", "mailer-daemon",
    "webmaster", "hostmaster", "abuse", "support@wix", "support@squarespace",
}


# ---------------------------------------------------------------------------
# HTTP client helper
# ---------------------------------------------------------------------------

def _get_client():
    """Create an httpx client with randomized user agent and timeouts."""
    return httpx.Client(
        headers={
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=30,
        follow_redirects=True,
    )


def _polite_sleep(min_sec=2, max_sec=5):
    """Random sleep to be polite and avoid rate limits."""
    time.sleep(random.uniform(min_sec, max_sec))


# ---------------------------------------------------------------------------
# Google Maps scraping (via Google Search local results)
# ---------------------------------------------------------------------------

def scrape_google_maps(query, max_results=20):
    """
    Scrape Google Search local/maps results for a business query.
    Uses the public Google Search page and parses the local pack results.
    Returns a list of business dicts.
    """
    results = []
    encoded_q = quote_plus(query)
    url = "https://www.google.com/search?q={}&num=20&tbm=lcl".format(encoded_q)

    try:
        with _get_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Google local results are in div elements with data-cid attributes
            # Parse multiple possible structures
            listings = soup.select("div.rllt__details, div[data-cid]")

            if not listings:
                # Fallback: try to find any business-like divs
                listings = soup.find_all("div", class_=re.compile(r"(VkpGBb|rllt)"))

            for item in listings[:max_results]:
                biz = _parse_google_listing(item)
                if biz and biz.get("name"):
                    results.append(biz)

            # If structured parsing fails, try regex extraction from raw HTML
            if not results:
                results = _extract_google_fallback(resp.text, query)

    except Exception as exc:
        print("  [ERROR] Google scrape failed for '{}': {}".format(query, exc))

    print("  [GOOGLE] {} results for: {}".format(len(results), query))
    return results


def _parse_google_listing(item):
    """Parse a single Google local listing element."""
    biz = {
        "name": "",
        "full_address": "",
        "phone": "",
        "site": "",
        "rating": 0,
        "reviews": 0,
    }

    # Name
    name_el = item.select_one("span.OSrXXb, div.dbg0pd, span[role='heading']")
    if name_el:
        biz["name"] = name_el.get_text(strip=True)

    # Rating
    rating_el = item.select_one("span.yi40Hd, span.BTtC6e, span[role='img']")
    if rating_el:
        rating_text = rating_el.get_text(strip=True)
        try:
            biz["rating"] = float(re.search(r"[\d.]+", rating_text).group())
        except (AttributeError, ValueError):
            pass

    # Reviews count
    reviews_el = item.select_one("span.HypWnf, span.RDApEe")
    if reviews_el:
        reviews_text = reviews_el.get_text(strip=True)
        try:
            biz["reviews"] = int(re.search(r"[\d,]+", reviews_text.replace(",", "")).group())
        except (AttributeError, ValueError):
            pass

    # Address / location text
    addr_el = item.select_one("span.rllt__details div:nth-of-type(2), div.pJ3Ci")
    if addr_el:
        biz["full_address"] = addr_el.get_text(strip=True)

    # Website link
    link_el = item.select_one("a[href*='http']")
    if link_el:
        href = link_el.get("href", "")
        if "google.com" not in href:
            biz["site"] = href

    return biz


def _extract_google_fallback(html, query):
    """
    Fallback parser using regex to extract business data from Google HTML
    when structured selectors fail (Google changes layout frequently).
    """
    results = []

    # Look for JSON-LD structured data
    ld_blocks = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
    for block in ld_blocks:
        try:
            data = json.loads(block)
            if isinstance(data, list):
                for item in data:
                    biz = _parse_jsonld(item)
                    if biz:
                        results.append(biz)
            elif isinstance(data, dict):
                biz = _parse_jsonld(data)
                if biz:
                    results.append(biz)
        except json.JSONDecodeError:
            pass

    return results


def _parse_jsonld(data):
    """Try to extract business info from a JSON-LD object."""
    biz_types = {"LocalBusiness", "Restaurant", "Store", "Organization",
                 "AutoRepair", "Plumber", "Electrician", "HairSalon",
                 "HomeAndConstructionBusiness", "ProfessionalService"}

    schema_type = data.get("@type", "")
    if schema_type not in biz_types:
        return None

    name = data.get("name", "").strip()
    if not name:
        return None

    address = data.get("address", {})
    if isinstance(address, dict):
        addr_str = "{}, {}, {} {}".format(
            address.get("streetAddress", ""),
            address.get("addressLocality", ""),
            address.get("addressRegion", ""),
            address.get("postalCode", ""),
        ).strip(", ")
    else:
        addr_str = str(address)

    rating_obj = data.get("aggregateRating", {})
    rating = 0
    reviews = 0
    if isinstance(rating_obj, dict):
        try:
            rating = float(rating_obj.get("ratingValue", 0))
        except (ValueError, TypeError):
            pass
        try:
            reviews = int(rating_obj.get("reviewCount", 0))
        except (ValueError, TypeError):
            pass

    return {
        "name": name,
        "full_address": addr_str,
        "phone": data.get("telephone", ""),
        "site": data.get("url", ""),
        "rating": rating,
        "reviews": reviews,
    }


# ---------------------------------------------------------------------------
# Yelp scraping
# ---------------------------------------------------------------------------

def scrape_yelp(query, max_results=20):
    """
    Scrape Yelp search results for a business query.
    Returns a list of business dicts in the same format as Google results.
    """
    results = []
    niche = query.split(" ")[0]
    city = "Saint Paul" if "Saint Paul" in query else "Minneapolis"
    location = quote_plus("{}, MN".format(city))
    search_term = quote_plus(niche)
    url = "https://www.yelp.com/search?find_desc={}&find_loc={}".format(search_term, location)

    try:
        with _get_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Yelp uses JSON-LD and also has structured search result cards
            # Try JSON embedded in script tags first
            scripts = soup.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict) and data.get("@type") == "ItemList":
                        for item in data.get("itemListElement", [])[:max_results]:
                            biz = item.get("item", {})
                            if biz.get("name"):
                                addr = biz.get("address", {})
                                rating_obj = biz.get("aggregateRating", {})
                                results.append({
                                    "name": biz.get("name", ""),
                                    "full_address": "{}, {}".format(
                                        addr.get("streetAddress", ""),
                                        addr.get("addressLocality", city),
                                    ),
                                    "phone": biz.get("telephone", ""),
                                    "site": biz.get("url", ""),
                                    "rating": float(rating_obj.get("ratingValue", 0) or 0),
                                    "reviews": int(rating_obj.get("reviewCount", 0) or 0),
                                })
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass

            # Fallback: parse HTML result cards
            if not results:
                cards = soup.select("div[data-testid='serp-ia-card'], li.border-color--default__09f24__NPAKY")
                for card in cards[:max_results]:
                    biz = _parse_yelp_card(card, city)
                    if biz and biz.get("name"):
                        results.append(biz)

    except Exception as exc:
        print("  [ERROR] Yelp scrape failed for '{}': {}".format(query, exc))

    print("  [YELP]   {} results for: {}".format(len(results), query))
    return results


def _parse_yelp_card(card, city):
    """Parse a Yelp search result card element."""
    biz = {
        "name": "",
        "full_address": "",
        "phone": "",
        "site": "",
        "rating": 0,
        "reviews": 0,
    }

    # Name
    name_el = card.select_one("a.css-19v1rkv, h3 a, a[href*='/biz/']")
    if name_el:
        biz["name"] = name_el.get_text(strip=True)
        href = name_el.get("href", "")
        if href.startswith("/biz/"):
            biz["site"] = "https://www.yelp.com" + href

    # Rating
    rating_el = card.select_one("div[aria-label*='star rating'], span[class*='star']")
    if rating_el:
        label = rating_el.get("aria-label", "")
        try:
            biz["rating"] = float(re.search(r"[\d.]+", label).group())
        except (AttributeError, ValueError):
            pass

    # Reviews
    review_el = card.select_one("span.reviewCount, span.css-chan6m")
    if review_el:
        try:
            biz["reviews"] = int(re.search(r"\d+", review_el.get_text()).group())
        except (AttributeError, ValueError):
            pass

    # Address
    addr_el = card.select_one("address, span.raw__09f24__T4Ezm")
    if addr_el:
        biz["full_address"] = addr_el.get_text(strip=True)
    else:
        biz["full_address"] = city + ", MN"

    return biz


# ---------------------------------------------------------------------------
# Email discovery from website
# ---------------------------------------------------------------------------

def discover_email(website_url):
    """
    Visit a business website and try to find an email address.
    Checks the homepage and common contact pages.
    Returns the first valid email found, or empty string.
    """
    if not website_url:
        return ""

    # Clean up URL
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    # Remove yelp.com URLs (they won't have the business email)
    if "yelp.com" in website_url:
        return ""

    pages_to_check = [
        website_url,
        urljoin(website_url, "/contact"),
        urljoin(website_url, "/contact-us"),
        urljoin(website_url, "/about"),
    ]

    found_emails = set()

    try:
        with _get_client() as client:
            for page_url in pages_to_check:
                try:
                    resp = client.get(page_url)
                    if resp.status_code != 200:
                        continue

                    # Extract emails from page text
                    emails = EMAIL_REGEX.findall(resp.text)
                    for email in emails:
                        email_lower = email.lower()
                        domain = email_lower.split("@")[1] if "@" in email_lower else ""
                        prefix = email_lower.split("@")[0] if "@" in email_lower else ""

                        # Skip junk emails
                        if domain in IGNORE_EMAIL_DOMAINS:
                            continue
                        if any(email_lower.startswith(p) for p in IGNORE_EMAIL_PREFIXES):
                            continue
                        if len(email) > 60:  # suspicious long email
                            continue

                        found_emails.add(email_lower)

                    if found_emails:
                        break  # Found emails, no need to check more pages

                    _polite_sleep(1, 2)

                except Exception:
                    continue  # Page failed, try next

    except Exception as exc:
        print("    [EMAIL] Could not fetch {}: {}".format(website_url, exc))

    if found_emails:
        # Prefer info@, contact@, owner@ addresses
        preferred_prefixes = ["info", "contact", "owner", "hello", "admin"]
        for prefix in preferred_prefixes:
            for email in found_emails:
                if email.startswith(prefix + "@"):
                    return email
        return sorted(found_emails)[0]  # Return first alphabetically

    return ""


# ---------------------------------------------------------------------------
# Lead scoring
# ---------------------------------------------------------------------------

def score_lead(lead):
    """
    Score a lead from 0-100.

    Scoring rules:
      +25  No website listed (or website is just a Yelp page)
      +20  Rating < 3.5  (bad reputation - needs reputation management)
      +10  Rating < 4.2  (mediocre - still improvable)
      +10  Review count between 1 and 50 (small, reachable business)
      +15  Email address found
      +10  Phone number found (reachable)

    Disqualification rules (returns score=0):
      - reviews > 200  (too established)
      - reviews == 0   (possibly closed/fake listing)

    Qualification threshold: score >= 50
    Returns (score: int, reason: str)
    """
    score = 0
    reasons = []

    rating = lead.get("rating") or 0
    reviews = lead.get("reviews") or 0
    website = (lead.get("website") or "").strip()
    email = (lead.get("email") or "").strip()
    phone = (lead.get("phone") or "").strip()

    # Website is "no website" if empty or just a Yelp listing URL
    has_real_website = bool(website) and "yelp.com" not in website

    # Disqualify first
    if reviews == 0:
        return 0, "disqualified: zero reviews"
    if reviews > 200:
        return 0, "disqualified: too many reviews (>200)"

    # Positive signals
    if not has_real_website:
        score += 25
        reasons.append("no website (+25)")

    if rating and rating < 3.5:
        score += 20
        reasons.append("rating {} < 3.5 (+20)".format(rating))
    elif rating and rating < 4.2:
        score += 10
        reasons.append("rating {} < 4.2 (+10)".format(rating))

    if 1 <= reviews <= 50:
        score += 10
        reasons.append("{} reviews in 1-50 range (+10)".format(reviews))

    if email:
        score += 15
        reasons.append("email found (+15)")

    if phone:
        score += 10
        reasons.append("phone found (+10)")

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
      2. Scrape Google + Yelp for each query (two sources, free)
      3. Discover emails from business websites
      4. Score each result
      5. Filter: qualified score >= 50, not already contacted, not duplicate
      6. Save qualified leads to leads.csv
      7. Return the list of qualified lead dicts
    """
    contacted = load_contacted()
    today = datetime.date.today().isoformat()
    all_leads = []
    seen_names = set()  # dedupe within this run

    for query in TARGET_SEARCHES:
        print("\n[SCRAPE] " + query)

        # Scrape both sources
        google_results = scrape_google_maps(query)
        _polite_sleep(3, 6)  # Pause between sources
        yelp_results = scrape_yelp(query)

        niche = _niche_from_query(query)

        # Combine results, tag source
        combined = []
        for r in google_results:
            r["_source"] = "google"
            combined.append(r)
        for r in yelp_results:
            r["_source"] = "yelp"
            combined.append(r)

        for raw in combined:
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
            website = (raw.get("site") or "").strip()
            address = (raw.get("full_address") or "").strip()
            city = _city_from_address(address)
            rating = raw.get("rating") or 0
            reviews = raw.get("reviews") or 0
            phone = (raw.get("phone") or "").strip()
            source = raw.get("_source", "unknown")

            # Try to discover email from website
            email = ""
            if website and "yelp.com" not in website:
                print("    [EMAIL] Checking {} ...".format(website[:60]))
                email = discover_email(website)
                if email:
                    print("    [EMAIL] Found: {}".format(email))
                _polite_sleep(1, 3)

            # Score the lead
            lead_for_scoring = {
                "website": website if "yelp.com" not in website else "",
                "email": email,
                "phone": phone,
                "rating": rating,
                "reviews": reviews,
            }
            score, reason = score_lead(lead_for_scoring)

            if score < 50:
                print("  [LOW]  score={:3d} - {} ({})".format(score, name, reason))
                continue

            lead = {
                "name": name,
                "address": address,
                "phone": phone,
                "website": website if "yelp.com" not in website else "",
                "rating": rating,
                "reviews": reviews,
                "email": email,
                "score": score,
                "reason": reason,
                "niche": niche,
                "city": city,
                "scraped_date": today,
                "status": "new",
                "source": source,
            }
            all_leads.append(lead)
            print("  [LEAD] score={:3d} - {} | {} | src:{}".format(
                score, name, email or "no email", source
            ))

        # Be polite between search queries
        _polite_sleep(4, 8)

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
    print("LEAD SCRAPER v2 - Free | Google Maps + Yelp")
    print("Saint Paul / Minneapolis MN")
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
        print("   Phone   : " + (lead["phone"] or "(none)"))
        print("   Source  : " + lead["source"])
        print("   Reason  : " + lead["reason"])
        print("")
