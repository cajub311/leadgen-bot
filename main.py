"""
main.py
Orchestrator for the Lead Gen Bot pipeline.

Run order:
  1. Scrape Google Maps for qualified leads  (lead_scraper.py)
  2. Notify via Telegram: leads found         (telegram_notify.py)
  3. Send personalized cold emails            (email_bot.py)
  4. Notify via Telegram: emails sent         (telegram_notify.py)
  5. On any unhandled exception: send error alert to Telegram

Usage:
  python main.py
"""

import sys
import traceback

from lead_scraper import run_scraper
from email_bot import run_email_bot
from telegram_notify import notify_leads_found, notify_emails_sent, notify_error


def main():
    """
    Execute the full lead generation and outreach pipeline.

    Steps:
      1. Run the scraper to collect and score new leads.
      2. Send a Telegram message summarising what was found.
      3. Run the email bot to send personalised cold emails.
      4. Send a Telegram message summarising the email run.

    Any unhandled exception is caught, printed, and forwarded to
    Telegram as an error alert before the process exits with code 1.
    """
    try:
        # ------------------------------------------------------------------
        # Step 1: Scrape Google Maps for new leads
        # ------------------------------------------------------------------
        print("\n" + "=" * 60)
        print("LEAD GEN BOT - PIPELINE START")
        print("=" * 60)
        print("\n[STEP 1] Running lead scraper...")

        leads = run_scraper()

        print("\n[STEP 1 COMPLETE] Scraper finished: {} qualified leads".format(len(leads)))

        # ------------------------------------------------------------------
        # Step 2: Notify Telegram of scrape results
        # ------------------------------------------------------------------
        print("\n[STEP 2] Sending Telegram notification: leads found...")

        notify_leads_found(leads)

        print("[STEP 2 COMPLETE] Telegram notified.")

        # ------------------------------------------------------------------
        # Step 3: Send personalised cold emails
        # ------------------------------------------------------------------
        print("\n[STEP 3] Running email bot...")

        sent_count, failed_count = run_email_bot()

        print(
            "\n[STEP 3 COMPLETE] Email bot finished: {} sent, {} failed".format(
                sent_count, failed_count
            )
        )

        # ------------------------------------------------------------------
        # Step 4: Notify Telegram of email run results
        # ------------------------------------------------------------------
        print("\n[STEP 4] Sending Telegram notification: emails sent...")

        notify_emails_sent(sent_count, failed_count)

        print("[STEP 4 COMPLETE] Telegram notified.")

        # ------------------------------------------------------------------
        # Done
        # ------------------------------------------------------------------
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("  Leads found : {}".format(len(leads)))
        print("  Emails sent : {}".format(sent_count))
        print("  Emails failed: {}".format(failed_count))
        print("=" * 60 + "\n")

    except Exception as exc:
        # Capture full traceback for logging
        tb = traceback.format_exc()
        error_summary = "{}: {}\n\n{}".format(type(exc).__name__, exc, tb)

        print("\n[FATAL ERROR] Pipeline failed:")
        print(error_summary)

        # Forward to Telegram so Charles gets alerted even when away
        try:
            notify_error(error_summary)
        except Exception as notify_exc:
            print("[ERROR] Could not send Telegram error alert: {}".format(notify_exc))

        sys.exit(1)


if __name__ == "__main__":
    main()
