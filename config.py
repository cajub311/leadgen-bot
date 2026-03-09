"""
config.py
Centralized configuration for LeadGen Bot v3.
All environment variables, scoring weights, timing rules, and defaults.
"""

import os
import json

# ---------------------------------------------------------------------------
# Environment Variables
# ---------------------------------------------------------------------------

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

# Google Sheets service account credentials (JSON string from secret)
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")


def get_google_credentials_dict():
    """Parse the GOOGLE_SHEETS_CREDENTIALS env var into a dict."""
    if not GOOGLE_SHEETS_CREDENTIALS_JSON:
        return None
    try:
        return json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
    except json.JSONDecodeError:
        print("[CONFIG] ERROR: GOOGLE_SHEETS_CREDENTIALS is not valid JSON")
        return None


# ---------------------------------------------------------------------------
# Anthropic / Claude
# ---------------------------------------------------------------------------

ANTHROPIC_ENDPOINT = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-3-5-haiku-latest"
MAX_DRAFTS_PER_RUN = 25

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
TELEGRAM_MESSAGE_LIMIT = 4000

# ---------------------------------------------------------------------------
# Google Sheets Tab Names
# ---------------------------------------------------------------------------

SHEET_TAB_LEADS = "Sheet1"
SHEET_TAB_CONTACTED = "Contacted"
SHEET_TAB_CONFIG = "Config"

# ---------------------------------------------------------------------------
# Lead Scoring Weights
# ---------------------------------------------------------------------------

SCORING_WEIGHTS = {
    "has_website": 10,
    "has_email": 15,
    "has_phone": 5,
    "high_rating": 10,
    "low_rating": 5,
    "few_reviews": 10,
    "many_reviews": -5,
    "website_no_ssl": 15,
    "website_not_mobile": 10,
    "website_no_blog": 5,
    "high_competition": 10,
    "low_competition": -5,
}

# ---------------------------------------------------------------------------
# Follow-up Timing Rules
# ---------------------------------------------------------------------------

FOLLOW_UP_RULES = {
    "first_follow_up_days": 3,
    "second_follow_up_days": 7,
    "max_follow_ups": 3,
    "give_up_days": 21,
}

# ---------------------------------------------------------------------------
# Scraping Configuration
# ---------------------------------------------------------------------------

FALLBACK_SEARCHES = [
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

FALLBACK_CITIES = [
    "Saint Paul MN",
    "Minneapolis MN",
]

MAX_QUERIES_PER_RUN = 6
MAX_RESULTS_PER_QUERY = 20
SCRAPE_DELAY_MIN = 2
SCRAPE_DELAY_MAX = 5
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

IGNORE_EMAIL_DOMAINS = {
    "example.com", "example.org", "test.com", "sentry.io",
    "wixpress.com", "squarespace.com", "godaddy.com",
    "googleapis.com", "googleusercontent.com", "gstatic.com",
    "w3.org", "schema.org", "wordpress.org", "jquery.com",
    "facebook.com", "twitter.com", "instagram.com", "linkedin.com",
    "youtube.com", "pinterest.com", "tiktok.com",
}

IGNORE_EMAIL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "postmaster", "mailer-daemon",
    "webmaster", "hostmaster", "abuse", "support@wix", "support@squarespace",
    "info@example", "admin@wordpress",
}

CONTACT_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/connect"]

# ---------------------------------------------------------------------------
# CAN-SPAM Compliance
# ---------------------------------------------------------------------------

CAN_SPAM_FOOTER = (
    "\n\n---\n"
    "Twin Cities Web Co | Saint Paul, MN 55104\n"
    "You're receiving this because your business was found in a public directory.\n"
    "To stop future emails, reply with 'unsubscribe' and we'll remove you immediately.\n"
    "This is a one-time outreach -- we do not send follow-ups without your permission."
)

# ---------------------------------------------------------------------------
# Industry-Specific Email Templates
# ---------------------------------------------------------------------------

INDUSTRY_ANGLES = {
    "restaurant": {
        "pain_points": ["online ordering", "Google Maps visibility", "review management", "menu updates"],
        "hook": "I noticed {name} has great food but your online presence might be leaving customers on the table.",
    },
    "hair salon": {
        "pain_points": ["online booking", "Instagram portfolio", "Google reviews", "appointment reminders"],
        "hook": "Salons that show up first on Google get 3x more bookings -- I'd love to help {name} get there.",
    },
    "plumber": {
        "pain_points": ["emergency search ranking", "Google Local Services", "review generation", "lead capture"],
        "hook": "When a pipe bursts at 2am, homeowners Google 'plumber near me' -- is {name} showing up first?",
    },
    "electrician": {
        "pain_points": ["emergency search ranking", "Google Local Services", "review generation", "lead capture"],
        "hook": "Electricians who rank in Google's top 3 local results get 70% of the calls -- let's get {name} there.",
    },
    "auto repair": {
        "pain_points": ["online appointment scheduling", "review management", "price transparency", "Google Maps"],
        "hook": "Car owners check reviews before choosing a shop -- {name} deserves to shine online as much as in the garage.",
    },
    "landscaping": {
        "pain_points": ["before/after portfolio", "seasonal promotions", "Google Maps", "lead capture forms"],
        "hook": "Your work speaks for itself, but a strong online portfolio could bring {name} twice the spring bookings.",
    },
    "cleaning service": {
        "pain_points": ["trust signals", "online booking", "review generation", "recurring client management"],
        "hook": "Trust is everything in cleaning -- a professional web presence helps {name} win clients before you even walk in.",
    },
    "roofing contractor": {
        "pain_points": ["storm damage leads", "before/after gallery", "financing info", "Google Local Services"],
        "hook": "After every storm, homeowners search 'roofer near me' -- is {name} the first result they see?",
    },
    "hvac": {
        "pain_points": ["emergency service ranking", "seasonal tune-up promotions", "review generation", "online scheduling"],
        "hook": "When the furnace dies in January, Twin Cities homeowners need you fast -- let's make sure they find {name} first.",
    },
    "general contractor": {
        "pain_points": ["project portfolio", "client testimonials", "permit/license display", "lead capture"],
        "hook": "Homeowners want to see your past work before hiring -- a strong portfolio site could double {name}'s project leads.",
    },
    "dentist": {
        "pain_points": ["patient booking", "insurance info clarity", "Google reviews", "new patient specials"],
        "hook": "New patients choose dentists based on Google reviews and easy booking -- let's optimize both for {name}.",
    },
    "chiropractor": {
        "pain_points": ["patient education content", "online booking", "Google Maps", "review generation"],
        "hook": "People searching for pain relief pick the chiropractor they find first and trust most -- that should be {name}.",
    },
    "default": {
        "pain_points": ["Google visibility", "online reviews", "website modernization", "lead capture"],
        "hook": "I came across {name} and noticed a few quick wins that could bring in more customers from Google.",
    },
}

# ---------------------------------------------------------------------------
# Pipeline Stages
# ---------------------------------------------------------------------------

PIPELINE_STAGES = [
    "new", "qualified", "draft_ready", "approved", "contacted",
    "follow_up_1", "follow_up_2", "replied", "meeting", "closed",
    "unsubscribed", "dead",
]

MIN_SCORE_FOR_DRAFT = 20

# ---------------------------------------------------------------------------
# Leads & Contacted Columns
# ---------------------------------------------------------------------------

LEADS_COLUMNS = [
    "name", "address", "phone", "website", "rating",
    "reviews", "email", "score", "reason", "niche",
    "city", "scraped_date", "status", "source",
    "pipeline_stage", "follow_up_count", "last_contact",
    "reply_date", "website_ssl", "website_mobile",
    "website_blog", "competition_density",
    "subject_line_a", "subject_line_b",
]

CONTACTED_COLUMNS = [
    "name", "email", "niche", "city", "score",
    "sent_date", "subject", "sequence_num",
    "template_variant", "reply_received", "reply_date",
]
