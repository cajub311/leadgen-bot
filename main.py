"""
main.py  -- LeadGen Bot v3 Orchestrator

Run order:
  1. Scrape Google Maps + Yelp + Facebook for qualified leads
  2. Notify via Telegram: leads found summary
  3. Generate AI email drafts with A/B subjects (NO auto-send)
  4. Send rich lead cards to Telegram with inline approve/skip buttons
  5. Generate follow-up drafts for stale contacted leads
  6. Send follow-up cards to Telegram
  7. Poll Telegram for approve/skip button responses (short window)
  8. Send pipeline stats + funnel scorecard
  9. On any unhandled exception: send error alert

Usage:
  python main.py
"""

import sys
import traceback
import datetime

from lead_scraper import run_scraper, get_error_summary
from email_bot import run_email_bot, run_followup_bot
from telegram_notify import (
    notify_leads_found,
    notify_drafts_ready,
    notify_followups_ready,
    notify_pipeline_stats,
    notify_error,
    poll_callback_queries,
)
from sheets_client import (
    is_connected as sheets_connected,
    get_funnel_summary,
    update_lead_stage,
    update_lead_multiple_fields,
)


def handle_approvals(responses, drafts, followup_drafts):
    """
    Process callback query responses from Telegram inline buttons.
    Updates lead stages in Google Sheets based on approve/skip actions.
    """
    if not responses:
        print("[MAIN] No button responses received")
        return 0, 0

    approved = 0
    skipped = 0
    all_drafts = drafts + followup_drafts

    for action, index in responses:
        if action == "approve_all":
            # Approve all drafts
            for draft in all_drafts:
                name = draft.get("lead_name", "")
                city = draft.get("city", "")
                if sheets_connected() and name:
                    update_lead_multiple_fields(name, city, {
                        "pipeline_stage": "approved",
                        "last_contact": datetime.date.today().isoformat(),
                    })
                approved += 1
            print("[MAIN] Bulk approved all {} drafts".format(len(all_drafts)))
            break

        elif action == "skip_all":
            for draft in all_drafts:
                name = draft.get("lead_name", "")
                city = draft.get("city", "")
                if sheets_connected() and name:
                    update_lead_stage(name, city, "new")
                skipped += 1
            print("[MAIN] Bulk skipped all {} drafts".format(len(all_drafts)))
            break

        elif action == "approve":
            try:
                idx = int(index)
                if 0 <= idx < len(all_drafts):
                    draft = all_drafts[idx]
                    name = draft.get("lead_name", "")
                    city = draft.get("city", "")
                    if sheets_connected() and name:
                        seq = draft.get("sequence_num", 1)
                        if seq == 1:
                            stage = "approved"
                        elif seq == 2:
                            stage = "follow_up_1"
                        else:
                            stage = "follow_up_2"
                        update_lead_multiple_fields(name, city, {
                            "pipeline_stage": stage,
                            "last_contact": datetime.date.today().isoformat(),
                            "follow_up_count": str(seq),
                        })
                    approved += 1
                    print("[MAIN] Approved: {}".format(draft.get("lead_name", "?")))
            except (ValueError, IndexError):
                pass

        elif action == "skip":
            try:
                idx = int(index)
                if 0 <= idx < len(all_drafts):
                    draft = all_drafts[idx]
                    name = draft.get("lead_name", "")
                    city = draft.get("city", "")
                    if sheets_connected() and name:
                        update_lead_stage(name, city, "new")
                    skipped += 1
                    print("[MAIN] Skipped: {}".format(draft.get("lead_name", "?")))
            except (ValueError, IndexError):
                pass

    return approved, skipped


def main():
    """Execute the full v3 lead generation pipeline."""
    try:
        # ==================================================================
        print("\n" + "=" * 60)
        print("LEAD GEN BOT v3 -- PIPELINE START")
        print("Google Sheets CRM | AI Drafts | Telegram Approval")
        print("=" * 60)

        # Check Sheets connection
        if sheets_connected():
            print("\n[OK] Google Sheets connected")
        else:
            print("\n[WARN] Google Sheets NOT connected -- using CSV fallback")

        # ------------------------------------------------------------------
        # Step 1: Scrape for new leads
        # ------------------------------------------------------------------
        print("\n[STEP 1/8] Running lead scraper (Google + Yelp + Facebook)...")

        leads, scrape_errors = run_scraper()
        error_summary = get_error_summary()

        print("[STEP 1 COMPLETE] {} qualified leads found".format(len(leads)))

        # ------------------------------------------------------------------
        # Step 2: Notify Telegram of scrape results
        # ------------------------------------------------------------------
        print("\n[STEP 2/8] Sending Telegram notification: leads found...")

        notify_leads_found(leads, error_summary)

        print("[STEP 2 COMPLETE]")

        # ------------------------------------------------------------------
        # Step 3: Generate AI email drafts
        # ------------------------------------------------------------------
        print("\n[STEP 3/8] Generating email drafts (A/B subjects, no auto-send)...")

        drafts, draft_skipped = run_email_bot()

        print("[STEP 3 COMPLETE] {} drafts, {} skipped".format(len(drafts), draft_skipped))

        # ------------------------------------------------------------------
        # Step 4: Send lead cards to Telegram for approval
        # ------------------------------------------------------------------
        print("\n[STEP 4/8] Sending lead cards with inline buttons...")

        notify_drafts_ready(drafts, draft_skipped)

        print("[STEP 4 COMPLETE]")

        # ------------------------------------------------------------------
        # Step 5: Generate follow-up drafts for stale leads
        # ------------------------------------------------------------------
        print("\n[STEP 5/8] Checking for follow-up opportunities...")

        followup_drafts, followup_skipped = run_followup_bot()

        print("[STEP 5 COMPLETE] {} follow-ups, {} skipped".format(
            len(followup_drafts), followup_skipped))

        # ------------------------------------------------------------------
        # Step 6: Send follow-up cards to Telegram
        # ------------------------------------------------------------------
        print("\n[STEP 6/8] Sending follow-up cards...")

        notify_followups_ready(followup_drafts, followup_skipped)

        print("[STEP 6 COMPLETE]")

        # ------------------------------------------------------------------
        # Step 7: Poll for Telegram button responses (30s window)
        # ------------------------------------------------------------------
        print("\n[STEP 7/8] Polling Telegram for approve/skip responses (30s)...")

        responses = poll_callback_queries(timeout_seconds=30)
        approved, skipped = handle_approvals(responses, drafts, followup_drafts)

        print("[STEP 7 COMPLETE] {} approved, {} skipped".format(approved, skipped))

        # ------------------------------------------------------------------
        # Step 8: Pipeline stats + funnel scorecard
        # ------------------------------------------------------------------
        print("\n[STEP 8/8] Sending pipeline summary...")

        funnel = get_funnel_summary() if sheets_connected() else None

        notify_pipeline_stats(
            leads_count=len(leads),
            drafts_count=len(drafts),
            skipped_count=draft_skipped + followup_skipped,
            followups_count=len(followup_drafts),
            error_summary=error_summary,
            funnel=funnel,
        )

        print("[STEP 8 COMPLETE]")

        # ==================================================================
        print("\n" + "=" * 60)
        print("LEAD GEN BOT v3 -- PIPELINE COMPLETE")
        print("Leads: {} | Drafts: {} | Follow-ups: {} | Approved: {}".format(
            len(leads), len(drafts), len(followup_drafts), approved))
        print("=" * 60)

    except Exception as exc:
        error_msg = "Pipeline failed: {}".format(str(exc))
        tb = traceback.format_exc()
        print("\n[FATAL] {}".format(error_msg))
        print(tb)

        try:
            notify_error(error_msg, tb)
        except Exception:
            print("[FATAL] Could not send error alert to Telegram")

        sys.exit(1)


if __name__ == "__main__":
    main()
