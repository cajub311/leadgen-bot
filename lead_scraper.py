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
    SERPAPI_KEY,
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
# Scrape source health tracking
# ---------------------------------------------------------------------------

SCRAPE_HEALTH = {}

CAPTCHA_INDICATORS = [
    "unusual traffic", "are you a robot", "captcha", "verify you are human",
    "blocked", "access denied", "please complete the security check",
    "automated access", "bot detection", "rate limit exceeded",
]


def _update_health(source, success):
    """Track success rate per scrape source."""
    if source not in SCRAPE_HEALTH:
        SCRAPE_HEALTH[source] = {"attempts": 0, "successes": 0}
    SCRAPE_HEALTH[source]["attempts"] += 1
    if success:
        SCRAPE_HEALTH[source]["successes"] += 1


def _is_source_healthy(source, min_rate=0.5):
    """Check if a source is healthy (>50% success rate). New sources are always healthy."""
    if source not in SCRAPE_HEALTH:
        return True
    stats = SCRAPE_HEALTH[source]
    if stats["attempts"] < 3:
        return True  # Not enough data yet
    return (stats["successes"] / stats["attempts"]) >= min_rate


def _detect_captcha(response_text):
    """Check if response contains captcha/block indicators."""
    text_lower = response_text[:5000].lower()
    return any(indicator in text_lower for indicator in CAPTCHA_INDICATORS)


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
    """Fetch URL with exponential backoff, captcha detection, and UA rotation."""
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
            if resp.status_code == 403:
                # Rotate User-Agent and retry
                new_ua = random.choice(USER_AGENTS)
                client.headers["User-Agent"] = new_ua
                print("[SCRAPER] 403 on {}, rotating UA and retrying...".format(context))
                SCRAPE_ERRORS["scraping_blocked"].append({
                    "url": url, "status": 403, "context": context
                })
                time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                continue
            if resp.status_code == 200:
                # Check for captcha in response body
                if _detect_captcha(resp.text):
                    new_ua = random.choice(USER_AGENTS)
                    client.headers["User-Agent"] = new_ua
                    print("[SCRAPER] Captcha detected on {}, rotating UA...".format(context))
                    SCRAPE_ERRORS["scraping_blocked"].append({
                        "url": url, "status": "captcha", "context": context
                    })
                    time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt) + random.uniform(2, 5))
                    continue
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
# Lead Enrichment: PageSpeed Insights
# ---------------------------------------------------------------------------

def check_pagespeed(website_url, api_key=None):
    """Check Google PageSpeed Insights score for a website.
    Uses free API (no key required for basic usage, but key increases quota).
    Returns dict with mobile_score, desktop_score, and key metrics.
    """
    if not website_url or not website_url.startswith("http"):
        return {"mobile_score": None, "desktop_score": None, "error": "invalid_url"}

    try:
        import httpx

        base = "https://www.googleapis.com/pagespeedonline/v5/runPagespeedtest"
        results = {}

        for strategy in ("mobile", "desktop"):
            params = {
                "url": website_url,
                "strategy": strategy,
                "category": "performance",
            }
            if api_key:
                params["key"] = api_key

            resp = httpx.get(base, params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                lh = data.get("lighthouseResult", {})
                cats = lh.get("categories", {})
                perf = cats.get("performance", {})
                score = perf.get("score")
                if score is not None:
                    score = int(score * 100)

                # Key metrics
                audits = lh.get("audits", {})
                fcp = audits.get("first-contentful-paint", {}).get("displayValue", "")
                lcp = audits.get("largest-contentful-paint", {}).get("displayValue", "")
                cls_val = audits.get("cumulative-layout-shift", {}).get("displayValue", "")

                results[strategy] = {
                    "score": score,
                    "fcp": fcp,
                    "lcp": lcp,
                    "cls": cls_val,
                }
            else:
                results[strategy] = {"score": None, "error": "api_{}".format(resp.status_code)}

        return {
            "mobile_score": results.get("mobile", {}).get("score"),
            "desktop_score": results.get("desktop", {}).get("score"),
            "mobile_fcp": results.get("mobile", {}).get("fcp", ""),
            "mobile_lcp": results.get("mobile", {}).get("lcp", ""),
            "desktop_score_raw": results.get("desktop", {}),
            "mobile_score_raw": results.get("mobile", {}),
        }

    except Exception as e:
        print("[ENRICH] PageSpeed error for {}: {}".format(website_url, e))
        return {"mobile_score": None, "desktop_score": None, "error": str(e)}


# ---------------------------------------------------------------------------
# Lead Enrichment: BBB Rating Lookup
# ---------------------------------------------------------------------------

def check_bbb_rating(business_name, city="Saint Paul", state="MN"):
    """Look up BBB (Better Business Bureau) rating via web scraping.
    Returns dict with rating (A+ to F), accredited (bool), and profile_url.
    """
    try:
        import httpx
        from urllib.parse import quote_plus

        search_query = "{} {} {}".format(business_name, city, state)
        search_url = "https://www.bbb.org/search?find_text={}&find_loc={}%2C+{}".format(
            quote_plus(business_name), quote_plus(city), quote_plus(state)
        )

        client = httpx.Client(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=15,
            follow_redirects=True,
        )

        resp = client.get(search_url)
        client.close()

        if resp.status_code != 200:
            return {"rating": None, "accredited": False, "error": "http_{}".format(resp.status_code)}

        text = resp.text

        # Look for rating pattern in BBB search results
        import re
        # BBB shows ratings like "A+", "A", "B+", etc.
        rating_match = re.search(r'class="[^"]*rating[^"]*"[^>]*>([A-F][+-]?)</span>', text, re.IGNORECASE)
        rating = rating_match.group(1) if rating_match else None

        # Check if accredited
        accredited = "BBB Accredited" in text or "accredited" in text.lower()

        # Try to find profile URL
        profile_match = re.search(r'href="(https://www\.bbb\.org/us/[^"]+)"', text)
        profile_url = profile_match.group(1) if profile_match else ""

        return {
            "rating": rating,
            "accredited": accredited,
            "profile_url": profile_url,
        }

    except Exception as e:
        print("[ENRICH] BBB error for {}: {}".format(business_name, e))
        return {"rating": None, "accredited": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Lead Enrichment: LinkedIn Company Page Detection
# ---------------------------------------------------------------------------

def detect_linkedin_page(business_name, website_url=None):
    """Detect if a business has a LinkedIn company page.
    First checks their website for LinkedIn links, then falls back to Google search.
    Returns dict with linkedin_url and has_linkedin (bool).
    """
    try:
        import httpx
        import re

        linkedin_url = None

        # Method 1: Check website source for LinkedIn links
        if website_url and website_url.startswith("http"):
            try:
                client = httpx.Client(
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                    },
                    timeout=10,
                    follow_redirects=True,
                )
                resp = client.get(website_url)
                client.close()

                if resp.status_code == 200:
                    li_match = re.search(
                        r'href="(https?://(?:www\.)?linkedin\.com/(?:company|in)/[^"?#]+)',
                        resp.text, re.IGNORECASE
                    )
                    if li_match:
                        linkedin_url = li_match.group(1)
            except Exception:
                pass  # Website check failed, try Google

        # Method 2: Google search fallback
        if not linkedin_url:
            try:
                from urllib.parse import quote_plus
                search_url = "https://www.google.com/search?q={}+site:linkedin.com/company".format(
                    quote_plus(business_name)
                )
                client = httpx.Client(
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    timeout=10,
                    follow_redirects=True,
                )
                resp = client.get(search_url)
                client.close()

                if resp.status_code == 200:
                    li_match = re.search(
                        r'(https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9_-]+)',
                        resp.text, re.IGNORECASE
                    )
                    if li_match:
                        linkedin_url = li_match.group(1)
            except Exception:
                pass

        return {
            "linkedin_url": linkedin_url or "",
            "has_linkedin": bool(linkedin_url),
        }

    except Exception as e:
        print("[ENRICH] LinkedIn error for {}: {}".format(business_name, e))
        return {"linkedin_url": "", "has_linkedin": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Lead Enrichment: Combined Pipeline
# ---------------------------------------------------------------------------

def enrich_lead(lead, pagespeed_api_key=None):
    """Run all enrichment checks on a lead and return combined results.
    Called from the main scraping pipeline after basic lead data is collected.
    """
    results = {}
    name = lead.get("name", "")
    website = lead.get("website", "")
    city = lead.get("city", "Saint Paul")

    # PageSpeed (only if website exists)
    if website:
        ps = check_pagespeed(website, api_key=pagespeed_api_key)
        results["pagespeed_mobile"] = str(ps.get("mobile_score", "")) if ps.get("mobile_score") is not None else ""
        results["pagespeed_desktop"] = str(ps.get("desktop_score", "")) if ps.get("desktop_score") is not None else ""
    else:
        results["pagespeed_mobile"] = ""
        results["pagespeed_desktop"] = ""

    # BBB Rating
    if name:
        bbb = check_bbb_rating(name, city=city.replace(" MN", ""), state="MN")
        results["bbb_rating"] = bbb.get("rating", "") or ""
        results["bbb_accredited"] = "yes" if bbb.get("accredited") else "no"
    else:
        results["bbb_rating"] = ""
        results["bbb_accredited"] = ""

    # LinkedIn
    if name:
        li = detect_linkedin_page(name, website_url=website)
        results["linkedin_url"] = li.get("linkedin_url", "")
    else:
        results["linkedin_url"] = ""

    print("[ENRICH] {} -- PageSpeed: {}/{}, BBB: {}, LinkedIn: {}".format(
        name,
        results["pagespeed_mobile"], results["pagespeed_desktop"],
        results["bbb_rating"] or "N/A",
        "yes" if results["linkedin_url"] else "no",
    ))

    return results


# ---------------------------------------------------------------------------
# Website content scraping (RAG-lite for email personalization)
# ---------------------------------------------------------------------------

# Pages to scrape for business context (ordered by value)
_CONTENT_PATHS = ["/about", "/about-us", "/services", "/our-services",
                  "/what-we-do", "/our-story", "/why-us", "/our-team"]


def scrape_website_content(client, website_url, max_chars=2000):
    """Scrape business website for About/Services text to feed Claude.
    Returns a condensed text summary of what the business does, their
    services, team, and differentiators. Used for hyper-personalized emails.
    
    Args:
        client: httpx.Client instance
        website_url: Business homepage URL
        max_chars: Max characters to return (keeps Claude token cost low)
    
    Returns:
        dict with 'homepage_text', 'about_text', 'services_list', 'tagline'
    """
    result = {
        "homepage_text": "",
        "about_text": "",
        "services_list": "",
        "tagline": "",
    }

    if not website_url:
        return result

    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    base_url = website_url.rstrip("/")

    def _clean_text(soup_element):
        """Extract visible text, strip nav/footer/script noise."""
        # Remove elements that add noise
        for tag in soup_element.find_all(["script", "style", "nav", "footer",
                                           "header", "iframe", "noscript"]):
            tag.decompose()
        text = soup_element.get_text(separator=" ", strip=True)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _extract_tagline(soup):
        """Try to find the business tagline/slogan."""
        # Check meta description first
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return meta["content"].strip()[:200]
        # Check h1
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)[:200]
        # Check og:description
        og = soup.find("meta", attrs={"property": "og:description"})
        if og and og.get("content"):
            return og["content"].strip()[:200]
        return ""

    def _extract_services(soup):
        """Try to extract a services list from the page."""
        services = []
        # Look for lists within sections that mention "services"
        for section in soup.find_all(["section", "div"]):
            heading = section.find(["h1", "h2", "h3"])
            if heading and any(kw in heading.get_text().lower()
                              for kw in ["service", "what we", "our work", "solution"]):
                for li in section.find_all("li"):
                    svc = li.get_text(strip=True)
                    if 3 < len(svc) < 100:
                        services.append(svc)
                if not services:
                    # Fall back to h3/h4 within the section
                    for h in section.find_all(["h3", "h4"]):
                        svc = h.get_text(strip=True)
                        if 3 < len(svc) < 100:
                            services.append(svc)
                if services:
                    break
        return services[:10]  # Cap at 10 services

    try:
        # --- Scrape homepage ---
        resp = client.get(base_url, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            result["tagline"] = _extract_tagline(soup)
            homepage_services = _extract_services(soup)
            if homepage_services:
                result["services_list"] = "; ".join(homepage_services)
            # Get main content area text (truncated)
            main = soup.find("main") or soup.find("body")
            if main:
                result["homepage_text"] = _clean_text(main)[:max_chars]

        # --- Scrape about/services pages ---
        about_texts = []
        for path in _CONTENT_PATHS:
            if len(" ".join(about_texts)) > max_chars:
                break  # Already have enough content
            try:
                page_url = base_url + path
                resp = client.get(page_url, timeout=10)
                if resp.status_code == 200:
                    page_soup = BeautifulSoup(resp.text, "html.parser")
                    main = page_soup.find("main") or page_soup.find("body")
                    if main:
                        text = _clean_text(main)
                        if len(text) > 50:  # Skip near-empty pages
                            about_texts.append(text[:max_chars // 2])
                            print("[SCRAPER] Scraped content from {}".format(page_url))
                    # Also grab services if we didn't find them on homepage
                    if not result["services_list"]:
                        page_services = _extract_services(page_soup)
                        if page_services:
                            result["services_list"] = "; ".join(page_services)
                _polite_sleep()  # Be polite between subpage requests
            except Exception:
                continue

        if about_texts:
            result["about_text"] = " | ".join(about_texts)[:max_chars]

    except Exception as e:
        print("[SCRAPER] Website content scrape failed for {}: {}".format(website_url, e))

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
# SerpAPI fallback (optional, needs SERPAPI_KEY env var)
# ---------------------------------------------------------------------------

def scrape_serpapi(query, max_results=10):
    """
    Search Google Maps via SerpAPI as fallback when HTML scraping fails.
    Free tier: 100 searches/month. Returns list of business dicts.
    """
    if not SERPAPI_KEY:
        print("[SERPAPI] No API key configured, skipping")
        return []

    if not _is_source_healthy("serpapi"):
        print("[SERPAPI] Source unhealthy, skipping")
        return []

    try:
        url = "https://serpapi.com/search.json"
        params = {
            "engine": "google_maps",
            "q": query,
            "type": "search",
            "api_key": SERPAPI_KEY,
        }
        resp = httpx.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print("[SERPAPI] HTTP {} for '{}'".format(resp.status_code, query))
            _update_health("serpapi", False)
            return []

        data = resp.json()
        results = data.get("local_results", [])[:max_results]
        leads = []

        for r in results:
            website = r.get("website", "")
            leads.append({
                "name": r.get("title", ""),
                "address": r.get("address", ""),
                "phone": r.get("phone", ""),
                "website": website,
                "rating": str(r.get("rating", "")),
                "reviews": str(r.get("reviews", "")),
                "source": "serpapi",
            })

        print("[SERPAPI] Found {} results for '{}'".format(len(leads), query))
        _update_health("serpapi", True)
        return leads

    except Exception as e:
        print("[SERPAPI] Error: {}".format(e))
        _update_health("serpapi", False)
        SCRAPE_ERRORS["network_error"].append({"url": "serpapi", "context": query, "error": str(e)})
        return []


# ---------------------------------------------------------------------------
# Bing Places fallback (free, no API key needed)
# ---------------------------------------------------------------------------

def scrape_bing_places(client, query, max_results=10):
    """
    Scrape Bing Maps/Local for business listings.
    Zero cost, no API key. Returns list of business dicts.
    """
    if not _is_source_healthy("bing"):
        print("[BING] Source unhealthy, skipping")
        return []

    search_url = "https://www.bing.com/maps?q={}".format(quote_plus(query))
    resp = _fetch_with_retry(client, search_url, context="bing:{}".format(query))

    if not resp:
        _update_health("bing", False)
        return []

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        leads = []

        # Bing local results are in taskPaneSidebar or listing cards
        listings = soup.select(".b_sideBleed .b_factrow, .bm_details_overlay, [data-tag='TextItem']")

        if not listings:
            # Try alternative selectors for Bing local packs
            listings = soup.select(".b_localResult, .bm_component")

        if not listings:
            # Final fallback: search results with address-like content
            listings = soup.select(".b_algo")

        for item in listings[:max_results]:
            name_el = item.select_one("h2, .lc_content h2, a.tilk, .b_entityTitle")
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                continue

            addr_el = item.select_one(".b_address, .lc_content .b_factrow, .b_addrLine")
            address = addr_el.get_text(strip=True) if addr_el else ""

            phone_el = item.select_one(".b_phone, [aria-label*='phone']")
            phone = phone_el.get_text(strip=True) if phone_el else ""

            link_el = item.select_one("a[href*='http']")
            website = ""
            if link_el:
                href = link_el.get("href", "")
                if "bing.com" not in href and "microsoft.com" not in href:
                    website = href

            leads.append({
                "name": name,
                "address": address,
                "phone": phone,
                "website": website,
                "rating": "",
                "reviews": "",
                "source": "bing",
            })

        print("[BING] Found {} results for '{}'".format(len(leads), query))
        _update_health("bing", len(leads) > 0)
        return leads

    except Exception as e:
        print("[BING] Parse error: {}".format(e))
        _update_health("bing", False)
        SCRAPE_ERRORS["parse_error"].append({"url": search_url, "context": "bing", "error": str(e)})
        return []


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

    # Mix in seasonal niche searches for the current month
    from config import get_seasonal_searches
    seasonal = get_seasonal_searches()
    if seasonal:
        # Add seasonal queries that aren't already in the main list
        existing_lower = {q.lower() for q in all_queries}
        new_seasonal = [s for s in seasonal if s.lower() not in existing_lower]
        if new_seasonal:
            all_queries = all_queries + new_seasonal[:4]  # Add up to 4 seasonal
            print("[SCRAPER] Added {} seasonal niche searches".format(min(len(new_seasonal), 4)))

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

        # Scrape primary sources with health checks
        google_results = []
        if _is_source_healthy("google_maps"):
            google_results = scrape_google_maps(client, query, MAX_RESULTS_PER_QUERY)
            _update_health("google_maps", len(google_results) > 0)
            _polite_sleep()

        # If Google Maps returned nothing, try SerpAPI fallback
        serpapi_results = []
        if not google_results and SERPAPI_KEY:
            print("[SCRAPER] Google Maps returned 0, trying SerpAPI fallback...")
            serpapi_results = scrape_serpapi(query, MAX_RESULTS_PER_QUERY)
            _polite_sleep()

        yelp_results = []
        if _is_source_healthy("yelp"):
            yelp_results = scrape_yelp(client, query, MAX_RESULTS_PER_QUERY)
            _update_health("yelp", len(yelp_results) > 0)
            _polite_sleep()

        fb_results = []
        if _is_source_healthy("facebook"):
            fb_results = scrape_facebook_pages(client, query, min(10, MAX_RESULTS_PER_QUERY))
            _update_health("facebook", len(fb_results) > 0)
            _polite_sleep()

        # Bing Places as additional fallback
        bing_results = []
        if not google_results and not serpapi_results and _is_source_healthy("bing"):
            print("[SCRAPER] Primary sources dry, trying Bing Places...")
            bing_results = scrape_bing_places(client, query, MAX_RESULTS_PER_QUERY)
            _polite_sleep()

        combined = google_results + serpapi_results + yelp_results + fb_results + bing_results
        print("[SCRAPER] Found {} raw results for '{}' (G:{} S:{} Y:{} F:{} B:{})".format(
            len(combined), query,
            len(google_results), len(serpapi_results),
            len(yelp_results), len(fb_results), len(bing_results)))

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

            # Website content scraping (RAG-lite for email personalization)
            site_content = scrape_website_content(client, biz.get("website", ""))

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
                # RAG-lite content for Claude email personalization
                "tagline": site_content.get("tagline", ""),
                "services_list": site_content.get("services_list", ""),
                "about_text": site_content.get("about_text", ""),
                "homepage_text": site_content.get("homepage_text", ""),
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

            # Enrich qualified leads with PageSpeed, BBB, LinkedIn data
            if lead["status"] == "qualified":
                try:
                    enrichment = enrich_lead(lead)
                    lead["pagespeed_mobile"] = str(enrichment.get("pagespeed_mobile", ""))
                    lead["pagespeed_desktop"] = str(enrichment.get("pagespeed_desktop", ""))
                    lead["bbb_rating"] = enrichment.get("bbb_rating", "")
                    lead["bbb_accredited"] = str(enrichment.get("bbb_accredited", ""))
                    lead["linkedin_url"] = enrichment.get("linkedin_url", "")
                    # Boost score for leads with enrichment signals
                    if enrichment.get("pagespeed_mobile") and int(enrichment["pagespeed_mobile"]) < 50:
                        score += 10  # Poor mobile speed = bigger opportunity
                        lead["score"] = str(score)
                        lead["reason"] += "; poor mobile PageSpeed (<50)"
                    if enrichment.get("bbb_rating") in ("A+", "A", "A-"):
                        score += 5  # Reputable business = better client
                        lead["score"] = str(score)
                        lead["reason"] += "; BBB A-rated"
                    print("[ENRICH] {} -- PS:{} BBB:{} LI:{}".format(
                        name,
                        enrichment.get("pagespeed_mobile", "N/A"),
                        enrichment.get("bbb_rating", "N/A"),
                        "yes" if enrichment.get("linkedin_url") else "no",
                    ))
                except Exception as e:
                    print("[ENRICH] Failed for {}: {}".format(name, e))

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


def get_health_summary():
    """Get source health summary for monitoring."""
    if not SCRAPE_HEALTH:
        return "No health data yet"
    parts = []
    for source, stats in SCRAPE_HEALTH.items():
        rate = (stats["successes"] / stats["attempts"] * 100) if stats["attempts"] > 0 else 0
        status = "OK" if _is_source_healthy(source) else "DEGRADED"
        parts.append("{}: {:.0f}% ({}/{}) [{}]".format(
            source, rate, stats["successes"], stats["attempts"], status))
    return " | ".join(parts)
