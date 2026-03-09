"""
lead_scraper.py  -- LeadGen Bot v3
Free lead scraper using Google Maps + Yelp + Facebook Pages via httpx + BeautifulSoup.
Scores and filters leads with website quality analysis, competition density, and review sentiment.
Reads search queries from Google Sheets Config tab. Deduplicates against existing leads.
Zero API cost -- all scraping via public HTTP requests.
"""

import os
import re
import csv
import time
import json
import random
import socket
import datetime
import httpx
from bs4 import BeautifulSoup
from urllib.parse import quote_plus, urljoin, urlparse

from config import (
    USER_AGENTS, IGNORE_EMAIL_DOMAINS, IGNORE_EMAIL_PREFIXES,
    CONTACT_PATHS, SCORING_WEIGHTS, LEADS_COLUMNS,
    MAX_QUERIES_PER_RUN, MAX_RESULTS_PER_QUERY,
    SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX,
    MAX_RETRIES, RETRY_BACKOFF_BASE,
    MIN_SCORE_FOR_DRAFT, FALLBACK_SEARCHES,
)
from sheets_client import (
    is_connected as sheets_connected,
    get_search_queries, get_existing_lead_keys, append_leads,
)

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Error categories for Telegram reporting
SCRAPE_ERRORS = {
    "no_leads_found": [],
    "scraping_blocked": [],
    "network_error": [],
    "parse_error": [],
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_client():
    """Create httpx client with randomized user agent."""
    return httpx.Client(
        headers={
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=30,
        follow_redirects=True,
    )


def _polite_sleep():
    """Random sleep to avoid rate limits."""
    time.sleep(random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX))


def _fetch_with_retry(client, url, context=""):
    """Fetch URL with exponential backoff on 429/503."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.get(url)
            if resp.status_code in (429, 503):
                wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                print("[SCRAPER] Rate limited on {} ({}), waiting {}s...".format(
                    context, resp.status_code, wait))
                SCRAPE_ERRORS["scraping_blocked"].append({
                    "url": url, "status": resp.status_code, "context": context
                })
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp
            else:
                print("[SCRAPER] HTTP {} for {}".format(resp.status_code, context))
                return None
        except httpx.TimeoutException:
            wait = RETRY_BACKOFF_BASE * (2 ** attempt)
            print("[SCRAPER] Timeout on {}, retry in {}s".format(context, wait))
            SCRAPE_ERRORS["network_error"].append({"url": url, "context": context, "error": "timeout"})
            time.sleep(wait)
        except Exception as e:
            SCRAPE_ERRORS["network_error"].append({"url": url, "context": context, "error": str(e)})
            print("[SCRAPER] Error fetching {}: {}".format(context, e))
            return None

    print("[SCRAPER] Max retries exceeded for {}".format(context))
    return None


# ---------------------------------------------------------------------------
# Email discovery (deep)
# ---------------------------------------------------------------------------

def _filter_email(email):
    """Check if email should be ignored."""
    if not email:
        return False
    email = email.lower().strip()
    domain = email.split("@")[-1]
    if domain in IGNORE_EMAIL_DOMAINS:
        return False
    for prefix in IGNORE_EMAIL_PREFIXES:
        if email.startswith(prefix):
            return False
    return True


def discover_emails_deep(client, website_url):
    """Scrape website homepage + contact/about pages for emails."""
    if not website_url:
        return []

    found_emails = set()

    # Normalize URL
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    pages_to_check = [website_url]
    parsed = urlparse(website_url)
    base = "{}://{}".format(parsed.scheme, parsed.netloc)
    for path in CONTACT_PATHS:
        pages_to_check.append(base + path)

    for page_url in pages_to_check:
        try:
            resp = client.get(page_url, timeout=15)
            if resp.status_code == 200:
                emails = EMAIL_REGEX.findall(resp.text)
                for e in emails:
                    if _filter_email(e):
                        found_emails.add(e.lower())
            _polite_sleep()
        except Exception:
            continue

    return list(found_emails)


# ---------------------------------------------------------------------------
# Website quality analysis
# ---------------------------------------------------------------------------

def analyze_website(client, website_url):
    """Check website for SSL, mobile-friendliness, and blog presence."""
    result = {
        "website_ssl": "unknown",
        "website_mobile": "unknown",
        "website_blog": "unknown",
    }

    if not website_url:
        return result

    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    try:
        resp = client.get(website_url, timeout=15)
        if resp.status_code != 200:
            return result

        # SSL check
        result["website_ssl"] = "yes" if website_url.startswith("https") or str(resp.url).startswith("https") else "no"

        html = resp.text.lower()
        soup = BeautifulSoup(html, "html.parser")

        # Mobile check -- look for viewport meta tag
        viewport = soup.find("meta", attrs={"name": "viewport"})
        result["website_mobile"] = "yes" if viewport else "no"

        # Blog check -- look for /blog links or blog-related elements
        has_blog = False
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if "/blog" in href or "/news" in href or "/articles" in href:
                has_blog = True
                break
        result["website_blog"] = "yes" if has_blog else "no"

    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# Competition density
# ---------------------------------------------------------------------------

def estimate_competition(client, niche, city):
    """Estimate how many competitors exist for this niche in this city."""
    query = "{} {}".format(niche, city)
    url = "https://www.google.com/search?q={}&num=10".format(quote_plus(query))

    try:
        resp = _fetch_with_retry(client, url, "competition:{}".format(query))
        if not resp:
            return "unknown"

        soup = BeautifulSoup(resp.text, "html.parser")
        # Count result entries as rough proxy
        results = soup.find_all("div", class_="g")
        count = len(results)

        if count >= 8:
            return "high"
        elif count >= 4:
            return "medium"
        else:
            return "low"
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Lead scoring v3
# ---------------------------------------------------------------------------

def score_lead(lead):
    """Score a lead based on multiple quality signals. Returns (score, reasons)."""
    score = 0
    reasons = []
    w = SCORING_WEIGHTS

    # Basic contact info
    if lead.get("website"):
        score += w["has_website"]
        reasons.append("+{}:has_website".format(w["has_website"]))
    if lead.get("email"):
        score += w["has_email"]
        reasons.append("+{}:has_email".format(w["has_email"]))
    if lead.get("phone"):
        score += w["has_phone"]
        reasons.append("+{}:has_phone".format(w["has_phone"]))

    # Rating signals
    try:
        rating = float(lead.get("rating", 0))
        if rating >= 4.0:
            score += w["high_rating"]
            reasons.append("+{}:high_rating({})".format(w["high_rating"], rating))
        elif rating > 0 and rating < 3.5:
            score += w["low_rating"]
            reasons.append("+{}:low_rating_needs_help({})".format(w["low_rating"], rating))
    except (ValueError, TypeError):
        pass

    # Review count signals
    try:
        reviews = int(lead.get("reviews", 0))
        if 0 < reviews < 20:
            score += w["few_reviews"]
            reasons.append("+{}:few_reviews({})".format(w["few_reviews"], reviews))
        elif reviews > 200:
            score += w["many_reviews"]
            reasons.append("{}:many_reviews({})".format(w["many_reviews"], reviews))
    except (ValueError, TypeError):
        pass

    # Website quality signals
    if lead.get("website_ssl") == "no":
        score += w["website_no_ssl"]
        reasons.append("+{}:no_ssl".format(w["website_no_ssl"]))
    if lead.get("website_mobile") == "no":
        score += w["website_not_mobile"]
        reasons.append("+{}:not_mobile".format(w["website_not_mobile"]))
    if lead.get("website_blog") == "no":
        score += w["website_no_blog"]
        reasons.append("+{}:no_blog".format(w["website_no_blog"]))

    # Competition density
    comp = lead.get("competition_density", "unknown")
    if comp == "high":
        score += w["high_competition"]
        reasons.append("+{}:high_competition".format(w["high_competition"]))
    elif comp == "low":
        score += w["low_competition"]
        reasons.append("{}:low_competition".format(w["low_competition"]))

    return max(0, score), "; ".join(reasons)


# ---------------------------------------------------------------------------
# Google Maps scraping
# ---------------------------------------------------------------------------

def scrape_google_maps(client, query, max_results=20):
    """Scrape Google Search local results for businesses."""
    url = "https://www.google.com/search?q={}&num={}&tbm=lcl".format(
        quote_plus(query), max_results
    )
    resp = _fetch_with_retry(client, url, "google:{}".format(query))
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    businesses = []

    # Parse local pack results
    for div in soup.find_all("div", class_="VkpGBb"):
        try:
            name_el = div.find("div", class_="dbg0pd")
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                continue

            # Address
            addr_el = div.find("span", class_="rllt__details")
            address = ""
            if addr_el:
                spans = addr_el.find_all("span")
                address = spans[-1].get_text(strip=True) if spans else ""

            # Phone
            phone = ""
            phone_match = re.search(r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', div.get_text())
            if phone_match:
                phone = phone_match.group()

            # Rating
            rating = ""
            rating_el = div.find("span", class_="yi40Hd")
            if rating_el:
                rating = rating_el.get_text(strip=True)

            # Reviews count
            reviews = ""
            reviews_match = re.search(r'\((\d+)\)', div.get_text())
            if reviews_match:
                reviews = reviews_match.group(1)

            # Website (from link)
            website = ""
            for link in div.find_all("a", href=True):
                href = link["href"]
                if "google.com" not in href and href.startswith("http"):
                    website = href
                    break

            businesses.append({
                "name": name,
                "address": address,
                "phone": phone,
                "website": website,
                "rating": rating,
                "reviews": reviews,
                "source": "google",
            })
        except Exception as e:
            SCRAPE_ERRORS["parse_error"].append({"source": "google", "error": str(e)})
            continue

    return businesses[:max_results]


# ---------------------------------------------------------------------------
# Yelp scraping
# ---------------------------------------------------------------------------

def scrape_yelp(client, query, max_results=20):
    """Scrape Yelp search results for businesses."""
    url = "https://www.yelp.com/search?find_desc={}&find_loc={}".format(
        quote_plus(query.split(" ")[0]),
        quote_plus(" ".join(query.split(" ")[1:]))
    )
    resp = _fetch_with_retry(client, url, "yelp:{}".format(query))
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    businesses = []

    for container in soup.find_all("div", {"data-testid": "serp-ia-card"}):
        try:
            name_el = container.find("a", class_="css-19v1rkv")
            if not name_el:
                name_el = container.find("a", {"name": True})
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                continue

            # Remove leading number (e.g., "1. Joe's Plumbing")
            name = re.sub(r'^\d+\.\s*', '', name)

            website = ""
            if name_el and name_el.get("href"):
                biz_url = "https://www.yelp.com" + name_el["href"] if name_el["href"].startswith("/") else name_el["href"]
                # We could follow this to get the actual website but that's slow
                website = biz_url

            address = ""
            addr_el = container.find("address")
            if addr_el:
                address = addr_el.get_text(strip=True)

            phone = ""
            phone_el = container.find("p", string=re.compile(r'\(\d{3}\)'))
            if phone_el:
                phone = phone_el.get_text(strip=True)

            rating = ""
            rating_el = container.find("div", {"aria-label": re.compile(r"star rating")})
            if rating_el:
                rating_match = re.search(r'([\d.]+)', rating_el.get("aria-label", ""))
                if rating_match:
                    rating = rating_match.group(1)

            reviews = ""
            reviews_el = container.find("span", string=re.compile(r'\d+ review'))
            if reviews_el:
                reviews_match = re.search(r'(\d+)', reviews_el.get_text())
                if reviews_match:
                    reviews = reviews_match.group(1)

            businesses.append({
                "name": name,
                "address": address,
                "phone": phone,
                "website": website,
                "rating": rating,
                "reviews": reviews,
                "source": "yelp",
            })
        except Exception as e:
            SCRAPE_ERRORS["parse_error"].append({"source": "yelp", "error": str(e)})
            continue

    return businesses[:max_results]


# ---------------------------------------------------------------------------
# Facebook Pages scraping
# ---------------------------------------------------------------------------

def scrape_facebook_pages(client, query, max_results=10):
    """Search Google for Facebook business pages."""
    search_query = "site:facebook.com {} business page".format(query)
    url = "https://www.google.com/search?q={}&num={}".format(
        quote_plus(search_query), max_results
    )
    resp = _fetch_with_retry(client, url, "facebook:{}".format(query))
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    businesses = []

    for div in soup.find_all("div", class_="g"):
        try:
            link = div.find("a", href=True)
            if not link or "facebook.com" not in link["href"]:
                continue

            title = link.get_text(strip=True)
            if not title or "Facebook" not in title:
                continue

            # Clean business name from Facebook title
            name = re.sub(r'\s*[-|]?\s*Facebook.*$', '', title, flags=re.IGNORECASE).strip()
            name = re.sub(r'\s*[-|]?\s*\d+\s*photos?.*$', '', name, flags=re.IGNORECASE).strip()
            if not name or len(name) < 3:
                continue

            # Get snippet for potential phone/address
            snippet = ""
            snippet_el = div.find("div", class_="VwiC3b")
            if snippet_el:
                snippet = snippet_el.get_text()

            phone = ""
            phone_match = re.search(r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', snippet)
            if phone_match:
                phone = phone_match.group()

            businesses.append({
                "name": name,
                "address": "",
                "phone": phone,
                "website": link["href"],
                "rating": "",
                "reviews": "",
                "source": "facebook",
            })
        except Exception as e:
            SCRAPE_ERRORS["parse_error"].append({"source": "facebook", "error": str(e)})
            continue

    return businesses[:max_results]


# ---------------------------------------------------------------------------
# Main scraper pipeline
# ---------------------------------------------------------------------------

def run_scraper():
    """
    Execute the full scraping pipeline:
    1. Load search queries from Google Sheets (or fallback)
    2. Pick random subset of queries for this run
    3. Scrape Google Maps + Yelp + Facebook for each query
    4. Deep email discovery on business websites
    5. Website quality analysis
    6. Competition density estimation
    7. Score and deduplicate leads
    8. Save to Google Sheets (or CSV fallback)
    Returns: (qualified_leads_list, error_summary_dict)
    """
    global SCRAPE_ERRORS
    SCRAPE_ERRORS = {
        "no_leads_found": [],
        "scraping_blocked": [],
        "network_error": [],
        "parse_error": [],
    }

    # Load queries from Sheets or fallback
    all_queries = get_search_queries() if sheets_connected() else FALLBACK_SEARCHES
    
    # Pick random subset for this run
    if len(all_queries) > MAX_QUERIES_PER_RUN:
        queries = random.sample(all_queries, MAX_QUERIES_PER_RUN)
    else:
        queries = all_queries

    print("[SCRAPER] Running {} queries this session".format(len(queries)))
    for q in queries:
        print("  - {}".format(q))

    # Get existing leads for dedup
    existing_keys = get_existing_lead_keys() if sheets_connected() else set()
    print("[SCRAPER] {} existing leads for dedup".format(len(existing_keys)))

    all_leads = []
    client = _get_client()

    for query in queries:
        print("\n[SCRAPER] Processing: {}".format(query))

        # Extract niche and city from query
        parts = query.rsplit(" ", 2)
        if len(parts) >= 3:
            niche = " ".join(parts[:-2])
            city = " ".join(parts[-2:])
        else:
            niche = query
            city = "Minneapolis MN"

        # Scrape all three sources
        google_results = scrape_google_maps(client, query, MAX_RESULTS_PER_QUERY)
        _polite_sleep()
        yelp_results = scrape_yelp(client, query, MAX_RESULTS_PER_QUERY)
        _polite_sleep()
        fb_results = scrape_facebook_pages(client, query, min(10, MAX_RESULTS_PER_QUERY))
        _polite_sleep()

        combined = google_results + yelp_results + fb_results
        print("[SCRAPER] Found {} raw results for '{}'".format(len(combined), query))

        if not combined:
            SCRAPE_ERRORS["no_leads_found"].append(query)
            continue

        # Estimate competition for this niche/city combo
        competition = estimate_competition(client, niche, city)
        _polite_sleep()

        # Process each lead
        seen_names = set()
        for biz in combined:
            name = biz.get("name", "").strip()
            name_lower = name.lower()

            # Skip duplicates within this run
            if name_lower in seen_names:
                continue
            seen_names.add(name_lower)

            # Skip if already in Google Sheets
            if (name_lower, city.lower()) in existing_keys:
                print("[SCRAPER] Skipping duplicate: {}".format(name))
                continue

            # Deep email discovery
            emails = discover_emails_deep(client, biz.get("website", ""))
            email = emails[0] if emails else ""

            # Website quality
            site_quality = analyze_website(client, biz.get("website", ""))

            # Build lead record
            lead = {
                "name": name,
                "address": biz.get("address", ""),
                "phone": biz.get("phone", ""),
                "website": biz.get("website", ""),
                "rating": biz.get("rating", ""),
                "reviews": biz.get("reviews", ""),
                "email": email,
                "niche": niche,
                "city": city,
                "scraped_date": datetime.date.today().isoformat(),
                "source": biz.get("source", "unknown"),
                "pipeline_stage": "new",
                "follow_up_count": "0",
                "last_contact": "",
                "reply_date": "",
                "website_ssl": site_quality.get("website_ssl", "unknown"),
                "website_mobile": site_quality.get("website_mobile", "unknown"),
                "website_blog": site_quality.get("website_blog", "unknown"),
                "competition_density": competition,
                "subject_line_a": "",
                "subject_line_b": "",
            }

            # Score the lead
            score, reasons = score_lead(lead)
            lead["score"] = str(score)
            lead["reason"] = reasons

            # Set status based on score
            if score >= MIN_SCORE_FOR_DRAFT:
                lead["status"] = "qualified"
                lead["pipeline_stage"] = "qualified"
            else:
                lead["status"] = "low_score"
                lead["pipeline_stage"] = "new"

            all_leads.append(lead)
            # Add to existing keys to prevent dupes across queries
            existing_keys.add((name_lower, city.lower()))

    client.close()

    # Save to Google Sheets
    if all_leads and sheets_connected():
        append_leads(all_leads)
    elif all_leads:
        # CSV fallback
        _save_csv_fallback(all_leads)

    qualified = [l for l in all_leads if l.get("status") == "qualified"]
    print("\n[SCRAPER] DONE: {} total leads, {} qualified".format(
        len(all_leads), len(qualified)))

    return qualified, SCRAPE_ERRORS


def _save_csv_fallback(leads):
    """Save leads to CSV as fallback when Sheets is unavailable."""
    csv_file = "leads.csv"
    file_exists = os.path.exists(csv_file)
    try:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LEADS_COLUMNS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerows(leads)
        print("[SCRAPER] Saved {} leads to CSV fallback".format(len(leads)))
    except Exception as e:
        print("[SCRAPER] CSV save failed: {}".format(e))


def get_error_summary():
    """Get formatted error summary for Telegram reporting."""
    summary = []
    for category, errors in SCRAPE_ERRORS.items():
        if errors:
            summary.append("{}: {}".format(category, len(errors)))
    return " | ".join(summary) if summary else "No errors"
