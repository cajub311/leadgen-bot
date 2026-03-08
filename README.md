# Lead Gen Bot v2

Free, automated lead generation and outreach pipeline for local service businesses
in the Saint Paul / Minneapolis, MN area. Runs on GitHub Actions every weekday morning.

**v2 changes:** Zero paid APIs, CAN-SPAM compliant, Telegram approval flow (no auto-blast).

---

## 1. What This Bot Does

1. **Scrapes Google Maps + Yelp** for free using httpx + BeautifulSoup across 10 local
   business niches (plumber, electrician, auto repair, landscaping, cleaning service,
   restaurant, hair salon, roofing contractor, HVAC, general contractor).
2. **Discovers emails** by visiting business websites and extracting contact addresses.
3. **Scores every result** from 0-100 based on signals: no website, low rating,
   low review count, email found, phone found. Leads scoring 50+ are kept.
4. **Deduplicates** against `contacted.csv` so no business is contacted twice.
5. **Validates email addresses** (format + DNS check) before drafting.
6. **Generates personalised email drafts** using Claude (claude-3-5-haiku-latest)
   via the Anthropic API. Drafts adapt based on website status, star rating, and niche.
7. **Appends CAN-SPAM footer** with physical address and unsubscribe mechanism.
8. **Sends lead cards to Telegram** with business details and draft previews.
   You review and approve before any email is sent.
9. **Sends pipeline stats** summary to Telegram after each run.

**No emails are sent automatically.** Every email requires your Telegram approval.

---

## 2. Cost

| Service        | Cost      | Notes                                       |
|----------------|-----------|---------------------------------------------|
| Google scraping| **$0**    | Free -- httpx + BeautifulSoup, no API key   |
| Yelp scraping  | **$0**    | Free -- httpx + BeautifulSoup, no API key   |
| Anthropic API  | ~$5/month | Claude 3.5 Haiku, ~$0.005 per email draft   |
| GitHub Actions | **$0**    | 2,000 min/month free (public repo)          |
| Telegram Bot   | **$0**    | Free, unlimited                             |
| Gmail SMTP     | **$0**    | Only sends approved emails (low volume)     |

**Total: ~$5/month** (Anthropic only, everything else is free).

---

## 3. Setup

### Secrets Required (only 4)

| Secret Name          | Description                                    |
|----------------------|------------------------------------------------|
| `ANTHROPIC_API_KEY`  | Anthropic API key for Claude email generation  |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather             |
| `TELEGRAM_CHAT_ID`   | Your Telegram chat ID (numeric)                |
| `GMAIL_ADDRESS`      | Your Gmail address for sending approved emails |

### Quick Start

1. Fork or clone this repo.
2. Add the 4 secrets in **Settings > Secrets and variables > Actions**.
3. Run the workflow manually from **Actions > Lead Gen Bot v2 > Run workflow**.
4. Check your Telegram for lead cards and approve/skip.

---

## 4. How It Works

```
Scrape (Google + Yelp)
    |
    v
Score & Filter (50+ threshold)
    |
    v
Discover Emails (website scraping)
    |
    v
Validate Emails (format + DNS)
    |
    v
Generate AI Drafts (Claude 3.5 Haiku)
    |
    v
Append CAN-SPAM Footer
    |
    v
Send Lead Cards to Telegram
    |
    v
YOU APPROVE/SKIP in Telegram
    |
    v
Approved emails sent via Gmail
```

---

## 5. Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ANTHROPIC_API_KEY="your_key"
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"
export GMAIL_ADDRESS="you@gmail.com"

# Run the full pipeline
python main.py

# Or run individual components:
python lead_scraper.py      # Scrape and score leads only
python email_bot.py          # Generate drafts for existing leads
python telegram_notify.py    # Test Telegram notifications
```

---

## 6. File Structure

```
leadgen-bot/
  main.py              # Pipeline orchestrator (5-step flow)
  lead_scraper.py      # Google Maps + Yelp scraper + email discovery + scorer
  email_bot.py         # AI draft generator (CAN-SPAM compliant, no auto-send)
  telegram_notify.py   # Lead cards, draft previews, pipeline stats
  requirements.txt     # Python deps: requests, httpx, beautifulsoup4
  leads.csv            # Generated: all qualified leads
  contacted.csv        # Generated: outreach history
  drafts.json          # Generated: pending email drafts
  .github/
    workflows/
      leadgen.yml      # GitHub Actions schedule (weekdays 8am CT)
```

> Add `leads.csv`, `contacted.csv`, and `drafts.json` to `.gitignore`.

---

## 7. CAN-SPAM Compliance

Every email includes:
- Physical mailing address (Twin Cities Web Co, Saint Paul, MN 55104)
- Clear identification as commercial email
- Unsubscribe mechanism (reply with "unsubscribe")
- Accurate sender information and subject lines
- One-time outreach notice (no follow-ups without permission)

---

## 8. Expected Results

| Metric                  | Conservative | Optimistic |
|-------------------------|-------------|------------|
| Leads scraped per week  | 30          | 150        |
| Drafts generated/week   | 15          | 75         |
| Approved emails/week    | 10          | 50         |
| Reply rate              | 1-2%        | 3-5%       |
| Monthly revenue (est.)  | $300        | $1,500+    |
