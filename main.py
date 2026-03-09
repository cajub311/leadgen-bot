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

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from lead_scraper import run_scraper, get_error_summary
from email_bot import run_email_bot, run_followup_bot, check_gmail_replies, send_approved_emails
from telegram_notify import (
    notify_leads_found,
    notify_drafts_ready,
    notify_followups_ready,
    notify_pipeline_stats,
    notify_error,
    poll_callback_queries,
    send_telegram_message,
)
from sheets_client import (
    is_connected as sheets_connected,
    get_funnel_summary,
    get_leads_by_stage,
    update_lead_stage,
    update_lead_multiple_fields,
    append_contacted,
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
        print("LEAD GEN BOT v5 -- PIPELINE START")
        print("Google Sheets CRM | AI Drafts | Reply Tracking | Telegram Approval")
        print("Enrichment | Seasonal Niches | Weekly Dashboard | Engagement Tracking")
        print("=" * 60)

        # Check Sheets connection
        if sheets_connected():
            print("\n[OK] Google Sheets connected")
        else:
            print("\n[WARN] Google Sheets NOT connected -- using CSV fallback")

        # Deliverability check at startup
        from email_bot import check_spf_dkim_warning
        check_spf_dkim_warning()

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
        # Step 5b: Check Gmail for replies to outreach emails
        # ------------------------------------------------------------------
        print("\n[STEP 5b] Checking Gmail for replies to outreach emails...")

        # Get all leads in 'contacted' or 'follow_up_*' stages
        contacted_leads = []
        for stage in ["contacted", "approved", "follow_up_1", "follow_up_2"]:
            contacted_leads.extend(get_leads_by_stage(stage))

        replies = check_gmail_replies(contacted_leads)
        reply_count = 0

        # Count bounces from reply detection
        bounce_count = sum(1 for r in (replies or []) if r.get("is_bounce", False))

        unsub_count = 0
        if replies and sheets_connected():
            for reply in replies:
                if reply.get("is_bounce", False):
                    update_lead_multiple_fields(reply["name"], reply["city"], {
                        "pipeline_stage": "bounced",
                        "last_contact": datetime.date.today().isoformat(),
                        "notes": "BOUNCED: {}".format(reply.get("reply_subject", "")[:60]),
                    })
                    print("[STEP 5b] BOUNCED: {}".format(reply["name"]))
                elif reply.get("is_unsubscribe", False):
                    update_lead_multiple_fields(reply["name"], reply["city"], {
                        "pipeline_stage": "unsubscribed",
                        "last_contact": datetime.date.today().isoformat(),
                        "notes": "UNSUBSCRIBED: {}".format(reply["reply_subject"][:60]),
                    })
                    unsub_count += 1
                    print("[STEP 5b] UNSUBSCRIBED: {}".format(reply["name"]))
                else:
                    update_lead_multiple_fields(reply["name"], reply["city"], {
                        "pipeline_stage": "replied",
                        "last_contact": datetime.date.today().isoformat(),
                        "notes": "Reply received: {}".format(reply["reply_subject"][:60]),
                    })
                    reply_count += 1
                    print("[STEP 5b] Updated {} to 'replied'".format(reply["name"]))

            # Remove replied leads from follow-up drafts to avoid sending follow-ups
            replied_emails = {r["email"] for r in replies}
            before = len(followup_drafts)
            followup_drafts = [
                d for d in followup_drafts
                if d.get("to_email", "").lower() not in replied_emails
            ]
            removed = before - len(followup_drafts)
            if removed:
                print("[STEP 5b] Removed {} follow-ups for leads who already replied".format(removed))

        print("[STEP 5b COMPLETE] {} replies tracked".format(reply_count))

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
        # Step 7b: Send approved emails via Gmail SMTP
        # ------------------------------------------------------------------
        print("\n[STEP 7b] Sending approved emails via Gmail...")

        # Collect approved drafts
        approved_drafts = []
        all_drafts = drafts + followup_drafts
        for action, index in (responses or []):
            if action == "approve_all":
                approved_drafts = list(all_drafts)
                break
            elif action == "approve":
                try:
                    idx = int(index)
                    if 0 <= idx < len(all_drafts):
                        approved_drafts.append(all_drafts[idx])
                except (ValueError, IndexError):
                    pass

        sent_records, send_failed, send_skipped = send_approved_emails(approved_drafts)
        sent_count = len(sent_records)

        # Log to Contacted tab in Google Sheets
        if sent_records and sheets_connected():
            append_contacted(sent_records)
            # Update pipeline stage to 'contacted' for sent leads
            for record in sent_records:
                update_lead_multiple_fields(record["name"], record.get("city", ""), {
                    "pipeline_stage": "contacted",
                    "last_contact": record["sent_date"],
                })

        # Send confirmation to Telegram
        if sent_count > 0 or send_failed > 0 or send_skipped > 0:
            msg = "EMAIL SEND COMPLETE\n"
            msg += "Sent: {}\n".format(sent_count)
            if send_failed > 0:
                msg += "Failed: {}\n".format(send_failed)
            if send_skipped > 0:
                msg += "Skipped (rate limit/window): {}\n".format(send_skipped)
            if unsub_count > 0:
                msg += "Unsubscribed: {}".format(unsub_count)
            try:
                send_telegram_message(msg)
            except Exception as e:
                print("[STEP 7b] Telegram notification failed: {}".format(e))

        print("[STEP 7b COMPLETE] {} sent, {} failed".format(sent_count, send_failed))

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
            sent_count=sent_count,
            replies_count=reply_count,
            error_summary=error_summary,
            funnel=funnel,
        )

        print("[STEP 8 COMPLETE]")

        # ------------------------------------------------------------------
        # Step 9: Weekly Engagement Report (runs on Sundays)
        # ------------------------------------------------------------------
        try:
            now = datetime.datetime.now(ZoneInfo("America/Chicago"))
            if now.weekday() == 6:  # Sunday
                print("\n=== Step 9: Weekly Engagement Report ===")
                from email_bot import get_ab_winner
                from telegram_notify import notify_engagement_report

                contacted = get_leads_by_stage("contacted")
                if contacted:
                    total_sent = len(contacted)
                    total_opens = sum(1 for l in contacted if int(l.get("open_count", 0) or 0) > 0)
                    total_clicks = sum(1 for l in contacted if int(l.get("click_count", 0) or 0) > 0)
                    total_replies = sum(1 for l in contacted if l.get("reply_received", "").lower() in ("yes", "true", "1"))
                    total_meetings = sum(1 for l in contacted if l.get("pipeline_stage", "") == "meeting")

                    open_rate = (total_opens / total_sent * 100) if total_sent > 0 else 0
                    click_rate = (total_clicks / total_sent * 100) if total_sent > 0 else 0
                    reply_rate = (total_replies / total_sent * 100) if total_sent > 0 else 0

                    ab_result = get_ab_winner(contacted)

                    # Find top performing subjects
                    top_subjects = []
                    for l in contacted:
                        if int(l.get("open_count", 0) or 0) > 0:
                            top_subjects.append({
                                "subject": l.get("subject", "?"),
                                "open_rate": round(int(l.get("open_count", 0) or 0) / 1 * 100, 1),
                            })
                    top_subjects.sort(key=lambda x: x["open_rate"], reverse=True)

                    notify_engagement_report(
                        ab_result=ab_result,
                        total_sent=total_sent,
                        total_opens=total_opens,
                        total_clicks=total_clicks,
                        total_replies=total_replies,
                        total_meetings=total_meetings,
                        open_rate=open_rate,
                        click_rate=click_rate,
                        reply_rate=reply_rate,
                        top_subjects=top_subjects[:5],
                    )
                    print("[ENGAGE] Weekly report sent")
        except Exception as e:
            print("[ENGAGE] Weekly report error: {}".format(e))

        # ------------------------------------------------------------------
        # Step 10: Weekly Pipeline Dashboard (runs on Sundays)
        # ------------------------------------------------------------------
        try:
            if now.weekday() == 6:  # Sunday (reuse 'now' from Step 9)
                print("\n=== Step 10: Weekly Pipeline Dashboard ===")
                from telegram_notify import build_pipeline_data, notify_weekly_dashboard

                all_leads_data = get_leads_by_stage("all")
                contacted_data = get_leads_by_stage("contacted")

                if all_leads_data:
                    dashboard_data = build_pipeline_data(
                        all_leads=all_leads_data,
                        contacted_leads=contacted_data or [],
                    )
                    notify_weekly_dashboard(dashboard_data)
                    print("[DASHBOARD] Weekly pipeline dashboard sent")
                else:
                    print("[DASHBOARD] No leads data available for dashboard")
        except Exception as e:
            print("[DASHBOARD] Dashboard error: {}".format(e))

        # ==================================================================
        print("\n" + "=" * 60)
        print("LEAD GEN BOT v5 -- PIPELINE COMPLETE")
        print("Leads: {} | Drafts: {} | Follow-ups: {} | Replies: {} | Approved: {} | Sent: {} | Unsub: {}".format(
            len(leads), len(drafts), len(followup_drafts), reply_count, approved, sent_count, unsub_count))
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
