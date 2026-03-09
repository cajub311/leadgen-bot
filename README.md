# LeadGen Bot v5 -- Twin Cities Web Co

Automated local business lead generation, AI-powered cold email outreach, and CRM pipeline management for web services in the Twin Cities metro area.

## What It Does

1. **Scrapes** local business directories (Google Maps, Yelp, Facebook, Bing Places) with fallback chains
2. **Scores** leads based on website quality, reviews, competition density, and 20+ signals
3. **Enriches** qualified leads with PageSpeed Insights, BBB ratings, and LinkedIn company detection
4. **Drafts** personalized cold emails using Claude AI with A/B subject lines and industry-specific templates
5. **Sends** via Gmail SMTP with warm-up scheduling, send windows, and deliverability best practices
6. **Tracks** opens (pixel), clicks (link wrapping), replies (IMAP), bounces, and unsubscribes
7. **Manages** the full pipeline in Google Sheets with batch updates and health checks
8. **Reports** weekly engagement analytics, A/B test results, conversion funnel, and ROI estimates via Telegram
9. **Rotates** seasonal niches automatically (snow removal in winter, landscaping in spring, etc.)

## Architecture

```
main.py              -- 10-step pipeline orchestrator
lead_scraper.py      -- Multi-source scraping, scoring, enrichment (1,345 lines)
email_bot.py         -- AI drafting, SMTP sending, tracking, A/B testing (1,173 lines)
telegram_notify.py   -- Rich notifications, approval buttons, dashboards (783 lines)
sheets_client.py     -- Google Sheets CRM with batch ops (602 lines)
config.py            -- All configuration, scoring, seasonal calendar (376 lines)
site/index.html      -- Landing page with Tailwind CSS + Schema.org (309 lines)
```

**Total: ~5,000 lines of Python + HTML**

## Pipeline (10 Steps)

| Step | What | Details |
|------|------|--------|
| 1 | Scrape | Google Maps + Yelp + Facebook + Bing with health tracking |
| 2 | Notify | Telegram summary of leads found |
| 3 | Draft | Claude AI generates emails with A/B subjects |
| 4 | Review | Rich lead cards sent to Telegram with Approve/Edit/Skip buttons |
| 5 | Follow-up | Auto-generates follow-up sequences for stale leads |
| 5b | Reply Check | IMAP scan for replies, bounces, unsubscribes |
| 6 | Follow-up Cards | Sends follow-up drafts to Telegram |
| 7 | Approval | Polls Telegram for button responses |
| 7b | Send | Gmail SMTP with warm-up limits and send windows |
| 8 | Stats | Pipeline summary + funnel scorecard |
| 9 | Engagement | Weekly A/B results + open/click/reply analytics (Sundays) |
| 10 | Dashboard | Full pipeline dashboard with ROI estimates (Sundays) |

## Features by Phase

### Phase 1: Email Send Pipeline
- Gmail SMTP with TLS
- HTML + plain text multipart
- Per-lead send confirmation via Telegram
- Unsubscribe detection in replies

### Phase 2: Scraping Hardening
- SerpAPI fallback when HTML scraping hits captchas
- Bing Places as secondary source
- Source health tracking with automatic failover
- Captcha/block detection and backoff

### Phase 3: Sheets Performance
- Batch cell updates (1 API call vs 50)
- Sheet health check at startup (creates missing tabs/columns)
- Metrics tab for deliverability tracking

### Phase 4: Email Deliverability
- 7-week warm-up ramp (5 -> 50 emails/day)
- Optimal send windows (Tue-Thu 9-11am CT)
- Bounce detection from IMAP
- SPF/DKIM/DMARC checklist at startup
- MX record validation (dnspython)

### Phase 5: Open/Click Tracking
- 1x1 tracking pixel injection
- Link wrapping for click tracking
- A/B subject line testing with auto-winner selection
- Engagement scoring (opens=1, clicks=3, replies=10, meetings=25)

### Phase 6: Landing Page
- Tailwind CSS dark theme
- Schema.org LocalBusiness markup
- 6 sections: hero, services, process, portfolio, testimonials, contact
- Mobile-responsive with scroll animations
- GitHub Pages ready

### Phase 7: Geographic Expansion
- 18 Twin Cities metro cities
- 22 diverse search queries
- 12-month seasonal niche calendar
- 16 industry-specific email templates

### Phase 8: Lead Enrichment
- PageSpeed Insights API (mobile + desktop scores)
- BBB rating + accreditation lookup
- LinkedIn company page detection
- Score boosting for poor PageSpeed (<50) and BBB A-rated businesses

### Phase 9: Reporting Dashboard
- Conversion funnel visualization
- Top niches by reply rate
- Top cities by volume
- Enrichment statistics
- Pipeline velocity (avg days to reply)
- ROI estimates ($500 avg deal value)
- Week-over-week comparison

## Setup

### Required Secrets (GitHub Actions)

```
ANTHROPIC_API_KEY        -- Claude AI for email drafting
GMAIL_ADDRESS            -- Your Gmail address
GMAIL_APP_PASSWORD       -- Gmail App Password (not regular password)
TELEGRAM_BOT_TOKEN       -- Telegram bot token
TELEGRAM_CHAT_ID         -- Your Telegram chat ID
GOOGLE_SHEET_ID          -- Google Sheets spreadsheet ID
GOOGLE_SHEETS_CREDENTIALS -- Service account JSON (stringified)
SERPAPI_KEY              -- Optional: SerpAPI fallback
TRACKING_PIXEL_BASE_URL  -- Optional: hosted tracking pixel endpoint
LINK_TRACKER_BASE_URL    -- Optional: link redirect tracker endpoint
EMAIL_WARMUP_START       -- ISO date when sending started (e.g., 2026-03-09)
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run Locally

```bash
python main.py
```

## Google Sheets Structure

| Tab | Purpose |
|-----|--------|
| Sheet1 (Leads) | All discovered leads with scores, enrichment data, pipeline stage |
| Contacted | Sent email history with A/B variant, open/click/reply tracking |
| Config | Custom search queries, cities, niches (overrides fallbacks) |
| Metrics | Daily deliverability stats (sent, bounced, opened, clicked, replies) |

## CAN-SPAM Compliance

- Physical address in footer
- Clear unsubscribe mechanism (reply 'unsubscribe')
- Honest subject lines (AI-generated, not misleading)
- No auto-send -- all emails require Telegram approval
- Unsubscribe requests automatically honored

## License

MIT

---
*Built by Charles G / Twin Cities Web Co / Saint Paul, MN*
