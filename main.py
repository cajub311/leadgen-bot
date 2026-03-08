"""
main.py
Orchestrator for the Lead Gen Bot v2 pipeline.

Run order:
  1. Scrape Google Maps + Yelp for qualified leads  (lead_scraper.py)
  2. Notify via Telegram: leads found               (telegram_notify.py)
  3. Generate AI email drafts (NO auto-send)         (email_bot.py)
  4. Send draft cards to Telegram for approval       (telegram_notify.py)
  5. Send pipeline stats summary                     (telegram_notify.py)
  6. On any unhandled exception: send error alert

Usage:
  python main.py
"""

import sys
import traceback

from lead_scraper import run_scraper
from email_bot import run_email_bot
from telegram_notify import (
    notify_leads_found,
    notify_drafts_ready,
    notify_pipeline_stats,
    notify_error,
)


def main():
    """
    Execute the full lead generation pipeline.

    Steps:
      1. Run the scraper to collect and score new leads (Google + Yelp).
      2. Send a Telegram message summarising what was found.
      3. Run the email bot to generate AI-personalised drafts.
      4. Send individual lead cards to Telegram for approval.
      5. Send pipeline stats summary.

    NO emails are sent automatically. Drafts go to Telegram
    for Charles to review and approve.

    Any unhandled exception is caught, printed, and forwarded to
    Telegram as an error alert before the process exits with code 1.
    """
    try:
        # ------------------------------------------------------------------
        # Step 1: Scrape Google Maps + Yelp for new leads
        # ------------------------------------------------------------------
        print("\n" + "=" * 60)
        print("LEAD GEN BOT v2 -- PIPELINE START")
        print("Free scraping | AI drafts | Telegram approval")
        print("=" * 60)
        print("\n[STEP 1] Running lead scraper (Google Maps + Yelp)...")

        leads = run_scraper()

        print("\n[STEP 1 COMPLETE] Scraper finished: {} qualified leads".format(
            len(leads)
        ))

        # ------------------------------------------------------------------
        # Step 2: Notify Telegram of scrape results
        # ------------------------------------------------------------------
        print("\n[STEP 2] Sending Telegram notification: leads found...")

        notify_leads_found(leads)

        print("[STEP 2 COMPLETE] Telegram notified.")

        # ------------------------------------------------------------------
        # Step 3: Generate AI email drafts (NO auto-send)
        # ------------------------------------------------------------------
        print("\n[STEP 3] Generating email drafts (no auto-send)...")

        drafts, skipped = run_email_bot()

        print(
            "\n[STEP 3 COMPLETE] {} drafts generated, {} skipped".format(
                len(drafts), skipped
            )
        )

        # ------------------------------------------------------------------
        # Step 4: Send draft cards to Telegram for approval
        # ------------------------------------------------------------------
        print("\n[STEP 4] Sending lead cards to Telegram for review...")

        notify_drafts_ready(drafts, skipped)

        print("[STEP 4 COMPLETE] Lead cards sent to Telegram.")

        # ------------------------------------------------------------------
        # Step 5: Pipeline stats summary
        # ------------------------------------------------------------------
        print("\n[STEP 5] Sending pipeline stats...")

        notify_pipeline_stats(
            leads_count=len(leads),
            drafts_count=len(drafts),
            skipped_count=skipped,
        )

        print("[STEP 5 COMPLETE] Stats sent.")

        # ------------------------------------------------------------------
        # Done
        # ------------------------------------------------------------------
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("  Leads found  : {}".format(len(leads)))
        print("  Drafts ready : {}".format(len(drafts)))
        print("  Skipped      : {}".format(skipped))
        print("  Status       : Awaiting Telegram approval")
        print("=" * 60 + "\n")

    except Exception as exc:
        tb = traceback.format_exc()
        error_summary = "{}: {}\n\n{}".format(type(exc).__name__, exc, tb)

        print("\n[FATAL ERROR] Pipeline failed:")
        print(error_summary)

        try:
            notify_error(error_summary)
        except Exception as notify_exc:
            print("[ERROR] Could not send Telegram error alert: {}".format(
                notify_exc
            ))

        sys.exit(1)


if __name__ == "__main__":
    main()
