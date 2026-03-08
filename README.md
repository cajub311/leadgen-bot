# Lead Gen Bot

An automated lead generation and cold outreach pipeline for local service businesses
in the Saint Paul / Minneapolis, MN area. Runs on GitHub Actions every weekday morning.

---

## 1. What This Bot Does

1. **Scrapes Google Maps** via the Outscraper API, searching 10 local business niches
   (plumber, electrician, auto repair, landscaping, cleaning service, restaurant,
   hair salon, roofing contractor, HVAC, general contractor).
2. **Scores every result** from 0-100 based on four signals: no website, low rating,
   low review count, and email address available. Only leads scoring 60+ are kept.
3. **Deduplicates** against a `contacted.csv` history file so no business is emailed twice.
4. **Saves qualified leads** to `leads.csv` with full metadata (name, address, phone,
   website, rating, reviews, email, score, niche, city, date, status).
5. **Generates a personalised cold email** for each lead using Claude (claude-3-haiku)
   via the Anthropic API. The email angle adapts based on whether the business has a
   website, their star rating, and their niche.
6. **Sends the email** via Gmail SMTP with a 30-second delay between sends.
7. **Notifies you on Telegram** with a scrape summary and email run results after
   each pipeline run.

---

## 2. Free Tier Limits

| Service       | Free Allowance                          | Notes                                      |
|---------------|-----------------------------------------|--------------------------------------------|
| Outscraper    | 25 searches / month free                | Upgrade for more at outscraper.com         |
| Gmail SMTP    | 500 emails / day free                   | Requires App Password (not your main pw)   |
| Anthropic API | Pay-per-use, approx. $0.01 per email    | No free tier; haiku model is cheapest      |
| GitHub Actions| 2,000 minutes / month free (public repo)| More than enough for daily weekday runs    |
| Telegram Bot  | Free, unlimited messages                | No cost at any volume                      |

**Estimated monthly cost running daily weekdays (20 runs/month):**
- Outscraper: $0 (within free tier at 10 searches/run)
- Anthropic: ~$10-15 (50 emails/day x 20 days x $0.01)
- Gmail: $0
- Total: roughly $10-15/month

---

## 3. Setup Steps

### Step 1 - Get an Outscraper API Key
1. Go to [outscraper.com](https://outscraper.com) and create a free account.
2. Navigate to **Profile > API Key** and copy your key.
3. Add it as a GitHub secret named `OUTSCRAPER_API_KEY` (see Step 5).

### Step 2 - Enable a Gmail App Password
1. Go to your Google Account at [myaccount.google.com](https://myaccount.google.com).
2. Navigate to **Security > 2-Step Verification** and enable it if not already on.
3. Go to **Security > App Passwords**.
4. Create a new app password (name it "Lead Gen Bot").
5. Copy the 16-character password shown. You will not see it again.
6. Add your Gmail address as `GMAIL_ADDRESS` and the app password as
   `GMAIL_APP_PASSWORD` in GitHub secrets.

### Step 3 - Get an Anthropic API Key
1. Go to [console.anthropic.com](https://console.anthropic.com) and sign up.
2. Navigate to **API Keys** and create a new key.
3. Add it as a GitHub secret named `ANTHROPIC_API_KEY`.
4. Add a small credit balance ($5-10 is enough to start).

### Step 4 - Get a Telegram Bot Token
1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` and follow the prompts to name your bot.
3. Copy the bot token provided (format: `123456789:ABCdef...`).
4. Start a conversation with your new bot, then visit:
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
   to find your `chat_id` in the response JSON.
5. Add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` as GitHub secrets.

### Step 5 - Add GitHub Repository Secrets
1. Go to your GitHub repo > **Settings > Secrets and variables > Actions**.
2. Click **New repository secret** for each of the six values below.

---

## 4. GitHub Secrets Required

| Secret Name          | Description                                         |
|----------------------|-----------------------------------------------------|
| `OUTSCRAPER_API_KEY` | Outscraper API key for Google Maps scraping         |
| `GMAIL_ADDRESS`      | Your full Gmail address (e.g. you@gmail.com)        |
| `GMAIL_APP_PASSWORD` | 16-character Gmail App Password (not your login pw) |
| `ANTHROPIC_API_KEY`  | Anthropic API key for Claude email generation       |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather                  |
| `TELEGRAM_CHAT_ID`   | Your Telegram chat ID (numeric)                     |

---

## 5. Running Locally

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Export environment variables
export OUTSCRAPER_API_KEY="your_key_here"
export GMAIL_ADDRESS="you@gmail.com"
export GMAIL_APP_PASSWORD="your_app_password"
export ANTHROPIC_API_KEY="your_anthropic_key"
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# 3. Run the full pipeline
python main.py

# Or run individual components:
python lead_scraper.py   # Scrape and score leads only
python email_bot.py      # Send emails to leads already in leads.csv
```

**No API keys?** The scraper automatically falls back to demo data so you can
test the scoring and CSV logic without any credentials.

---

## 6. Expected Results

| Metric                  | Conservative Estimate   | Optimistic Estimate     |
|-------------------------|-------------------------|-------------------------|
| Leads scraped per week  | 50                      | 200                     |
| Emails sent per week    | 50 (capped by default)  | 200 (raise the cap)     |
| Reply rate              | 1%                      | 3%                      |
| Replies per week        | 0-1                     | 3-6                     |
| Close rate on replies   | 20%                     | 33%                     |
| Avg. client value       | $300 one-time           | $500+ recurring/month   |
| Monthly revenue (est.)  | $300                    | $1,500+                 |

**Tips to improve results:**
- Personalise the email prompt in `email_bot.py` with your real name and offer.
- Follow up manually on any replies within 24 hours.
- Raise `MAX_EMAILS_PER_DAY` in `email_bot.py` once you confirm deliverability.
- Monitor your Gmail Sent folder and spam complaints weekly.
- Rotate niches seasonally (e.g. more landscaping in spring, HVAC in summer).

---

## File Structure

```
leadgen-bot/
  main.py              # Pipeline orchestrator
  lead_scraper.py      # Google Maps scraper + lead scorer
  email_bot.py         # AI email generator + Gmail sender
  telegram_notify.py   # Telegram notification helpers
  requirements.txt     # Python dependencies
  leads.csv            # Generated: all qualified leads (gitignored)
  contacted.csv        # Generated: outreach history (gitignored)
  .github/
    workflows/
      leadgen.yml      # GitHub Actions schedule (weekdays 8am CT)
```

> **Privacy note:** Add `leads.csv` and `contacted.csv` to your `.gitignore` to avoid
> committing personal business contact data to a public repository.
