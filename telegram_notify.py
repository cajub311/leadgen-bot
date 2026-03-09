"""
telegram_notify.py  -- LeadGen Bot v3
Telegram notification and approval interface for lead generation bot.
Features: inline keyboard buttons (Approve/Edit/Skip), callback query handler,
rich lead cards with website quality + competition indicators, funnel scorecard.
"""

import os
import json
import time
import requests

from config import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    TELEGRAM_API_BASE, TELEGRAM_MESSAGE_LIMIT,
)


# ---------------------------------------------------------------------------
# Core send functions
# ---------------------------------------------------------------------------

def _get_api_url(method):
    """Build Telegram API URL."""
    return TELEGRAM_API_BASE.format(token=TELEGRAM_BOT_TOKEN) + "/" + method


def send_telegram(message, parse_mode="HTML", reply_markup=None):
    """Send a message to Telegram. Handles splitting for long messages."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[TELEGRAM] Missing BOT_TOKEN or CHAT_ID -- notification skipped")
        print("[TELEGRAM] Message would have been:")
        print(message[:500])
        return False

    if len(message) > TELEGRAM_MESSAGE_LIMIT:
        chunks = _split_message(message, TELEGRAM_MESSAGE_LIMIT)
        success = True
        for i, chunk in enumerate(chunks):
            # Only add reply_markup to last chunk
            markup = reply_markup if i == len(chunks) - 1 else None
            if not _send_single(chunk, parse_mode, markup):
                success = False
        return success
    else:
        return _send_single(message, parse_mode, reply_markup)


def _send_single(message, parse_mode="HTML", reply_markup=None):
    """Send a single message (under 4096 chars)."""
    url = _get_api_url("sendMessage")
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)

    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok"):
            return True
        else:
            print("[TELEGRAM] API error: {}".format(data.get("description", "unknown")))
            return False
    except requests.exceptions.RequestException as exc:
        print("[TELEGRAM] Request failed: {}".format(exc))
        return False


def _split_message(text, max_len=4000):
    """Split a long message into chunks at newline boundaries."""
    chunks = []
    while len(text) > max_len:
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


# ---------------------------------------------------------------------------
# Inline keyboard buttons
# ---------------------------------------------------------------------------

def _make_approve_keyboard(lead_name, draft_index=0):
    """Create inline keyboard with Approve/Edit/Skip buttons."""
    return {
        "inline_keyboard": [
            [
                {"text": "Approve (A)", "callback_data": "approve:{}".format(draft_index)},
                {"text": "Edit", "callback_data": "edit:{}".format(draft_index)},
                {"text": "Skip", "callback_data": "skip:{}".format(draft_index)},
            ],
            [
                {"text": "Approve All", "callback_data": "approve_all"},
                {"text": "Skip All", "callback_data": "skip_all"},
            ],
        ]
    }


def _quality_indicator(value, good="yes", label=""):
    """Return emoji-like indicator for quality signals."""
    if value == good:
        return "[OK] {}".format(label)
    elif value == "unknown":
        return "[??] {}".format(label)
    else:
        return "[!!] {}".format(label)


# ---------------------------------------------------------------------------
# Rich lead card notifications
# ---------------------------------------------------------------------------

def notify_lead_card(draft, index=0):
    """Send a detailed lead card with inline approve/edit/skip buttons."""
    name = draft.get("lead_name", "Unknown")
    email = draft.get("to_email", "")
    niche = draft.get("niche", "")
    city = draft.get("city", "")
    score = draft.get("score", 0)
    industry = draft.get("industry", "")
    sequence = draft.get("sequence_num", 1)

    # Website quality indicators
    ssl = draft.get("website_ssl", "unknown")
    mobile = draft.get("website_mobile", "unknown")
    competition = draft.get("competition", "unknown")

    ssl_ind = _quality_indicator(ssl, "yes", "SSL")
    mobile_ind = _quality_indicator(mobile, "yes", "Mobile")
    comp_label = "HIGH" if competition == "high" else "MED" if competition == "medium" else "LOW" if competition == "low" else "??"

    # Build card
    seq_label = ""
    if sequence == 2:
        seq_label = " [FOLLOW-UP #1]"
    elif sequence == 3:
        seq_label = " [BREAK-UP EMAIL]"

    subject_a = draft.get("subject_a", "")
    subject_b = draft.get("subject_b", "")

    card = (
        "<b>--- LEAD CARD #{idx}{seq} ---</b>\n"
        "\n"
        "<b>{name}</b>\n"
        "Industry: {industry} | {city}\n"
        "Email: <code>{email}</code>\n"
        "Score: <b>{score}</b>/100\n"
        "\n"
        "<b>Quality Signals:</b>\n"
        "  {ssl} | {mobile} | Competition: {comp}\n"
        "\n"
        "<b>Subject A:</b> {subj_a}\n"
        "<b>Subject B:</b> {subj_b}\n"
        "\n"
        "<b>Email Preview:</b>\n"
        "<i>{body_preview}</i>\n"
    ).format(
        idx=index + 1,
        seq=seq_label,
        name=name,
        industry=industry.title(),
        city=city,
        email=email,
        score=score,
        ssl=ssl_ind,
        mobile=mobile_ind,
        comp=comp_label,
        subj_a=subject_a,
        subj_b=subject_b,
        body_preview=_truncate(draft.get("body", ""), 300),
    )

    keyboard = _make_approve_keyboard(name, index)
    return send_telegram(card, reply_markup=keyboard)


def _truncate(text, max_len=300):
    """Truncate text and add ellipsis."""
    # Remove CAN-SPAM footer from preview
    if "---" in text:
        text = text.split("---")[0].strip()
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


# ---------------------------------------------------------------------------
# Batch notifications
# ---------------------------------------------------------------------------

def notify_leads_found(leads, error_summary=""):
    """Notify Telegram how many leads were found in this scrape run."""
    count = len(leads)

    if count == 0:
        msg = (
            "<b>LEAD GEN v3 -- Scrape Complete</b>\n\n"
            "No new qualified leads found this run.\n"
        )
        if error_summary:
            msg += "\nErrors: {}".format(error_summary)
        return send_telegram(msg)

    # Summarize by source
    sources = {}
    for lead in leads:
        src = lead.get("source", "unknown")
        sources[src] = sources.get(src, 0) + 1

    source_breakdown = " | ".join(["{}: {}".format(k, v) for k, v in sources.items()])

    # Top scored leads
    sorted_leads = sorted(leads, key=lambda x: int(x.get("score", 0)), reverse=True)
    top_leads = sorted_leads[:5]
    top_list = "\n".join([
        "  {} (score: {}, {})".format(l.get("name", "?"), l.get("score", 0), l.get("niche", ""))
        for l in top_leads
    ])

    msg = (
        "<b>LEAD GEN v3 -- Scrape Complete</b>\n\n"
        "<b>{count} new qualified leads found!</b>\n"
        "Sources: {sources}\n\n"
        "<b>Top 5 by score:</b>\n"
        "{top}\n"
    ).format(count=count, sources=source_breakdown, top=top_list)

    if error_summary:
        msg += "\nErrors: {}".format(error_summary)

    return send_telegram(msg)


def notify_drafts_ready(drafts, skipped):
    """Send individual lead cards for each draft."""
    if not drafts:
        msg = (
            "<b>EMAIL DRAFTS</b>\n\n"
            "No drafts generated this run. ({} skipped)\n"
        ).format(skipped)
        return send_telegram(msg)

    # Header message
    msg = (
        "<b>EMAIL DRAFTS -- {} Ready for Review</b>\n\n"
        "Review each card below and tap Approve, Edit, or Skip.\n"
        "{} leads skipped (invalid email or low score)\n"
    ).format(len(drafts), skipped)
    send_telegram(msg)

    # Send individual cards with inline buttons
    for i, draft in enumerate(drafts):
        notify_lead_card(draft, i)
        time.sleep(0.5)  # Rate limit

    return True


def notify_followups_ready(drafts, skipped):
    """Send follow-up draft cards."""
    if not drafts:
        return True

    msg = (
        "<b>FOLLOW-UP DRAFTS -- {} Ready</b>\n\n"
        "These leads haven\'t replied. Review follow-up drafts below.\n"
        "{} skipped\n"
    ).format(len(drafts), skipped)
    send_telegram(msg)

    for i, draft in enumerate(drafts):
        notify_lead_card(draft, i + 100)  # Offset index to distinguish from initial drafts
        time.sleep(0.5)

    return True


# ---------------------------------------------------------------------------
# Pipeline stats & funnel scorecard
# ---------------------------------------------------------------------------

def notify_pipeline_stats(leads_count=0, drafts_count=0, skipped_count=0,
                          followups_count=0, error_summary="", funnel=None):
    """Send pipeline run summary with funnel scorecard."""
    msg = (
        "<b>PIPELINE SUMMARY</b>\n"
        "========================\n"
        "Leads scraped: {leads}\n"
        "Drafts generated: {drafts}\n"
        "Follow-ups generated: {followups}\n"
        "Skipped: {skipped}\n"
    ).format(
        leads=leads_count,
        drafts=drafts_count,
        followups=followups_count,
        skipped=skipped_count,
    )

    if error_summary:
        msg += "Errors: {}\n".format(error_summary)

    # Add funnel scorecard if available
    if funnel:
        msg += (
            "\n<b>FUNNEL SCORECARD</b>\n"
            "========================\n"
            "Total leads in CRM: {total}\n"
            "  New:          {new}\n"
            "  Qualified:    {qualified}\n"
            "  Draft Ready:  {draft_ready}\n"
            "  Approved:     {approved}\n"
            "  Contacted:    {contacted}\n"
            "  Follow-up 1:  {f1}\n"
            "  Follow-up 2:  {f2}\n"
            "  Replied:      {replied}\n"
            "  Meeting:      {meeting}\n"
            "  Closed:       {closed}\n"
            "  Unsubscribed: {unsub}\n"
            "  Dead:         {dead}\n"
            "------------------------\n"
            "Emails sent:    {emails}\n"
        ).format(
            total=funnel.get("total_leads", 0),
            new=funnel.get("new", 0),
            qualified=funnel.get("qualified", 0),
            draft_ready=funnel.get("draft_ready", 0),
            approved=funnel.get("approved", 0),
            contacted=funnel.get("contacted", 0),
            f1=funnel.get("follow_up_1", 0),
            f2=funnel.get("follow_up_2", 0),
            replied=funnel.get("replied", 0),
            meeting=funnel.get("meeting", 0),
            closed=funnel.get("closed", 0),
            unsub=funnel.get("unsubscribed", 0),
            dead=funnel.get("dead", 0),
            emails=funnel.get("total_emails_sent", 0),
        )

    msg += "\n<i>LeadGen Bot v3 | Twin Cities Web Co</i>"
    return send_telegram(msg)


# ---------------------------------------------------------------------------
# Error alert
# ---------------------------------------------------------------------------

def notify_error(error_msg, traceback_str=""):
    """Send error alert to Telegram."""
    msg = (
        "<b>[ERROR] LeadGen Bot v3</b>\n\n"
        "{}\n"
    ).format(error_msg[:1000])

    if traceback_str:
        msg += "\n<pre>{}</pre>".format(traceback_str[:1500])

    return send_telegram(msg)


# ---------------------------------------------------------------------------
# Weekly Engagement Report
# ---------------------------------------------------------------------------

def notify_engagement_report(ab_result, total_sent, total_opens, total_clicks,
                              total_replies, total_meetings, open_rate, click_rate,
                              reply_rate, top_subjects=None):
    """Send weekly engagement analytics report to Telegram."""
    # A/B test status
    if ab_result and ab_result.get("winner"):
        ab_status = "Winner: Variant {} ({} confidence)".format(
            ab_result["winner"], ab_result.get("confidence", "?"))
        stats_a = ab_result["stats"]["A"]
        stats_b = ab_result["stats"]["B"]
        ab_detail = (
            "\n\nA/B BREAKDOWN:\n"
            "Variant A: {} sent | {:.1f}% open | {:.1f}% click | {:.1f}% reply\n"
            "Variant B: {} sent | {:.1f}% open | {:.1f}% click | {:.1f}% reply"
        ).format(
            stats_a["sent"], stats_a.get("open_rate", 0), stats_a.get("click_rate", 0), stats_a.get("reply_rate", 0),
            stats_b["sent"], stats_b.get("open_rate", 0), stats_b.get("click_rate", 0), stats_b.get("reply_rate", 0),
        )
    else:
        ab_status = "Not enough data yet"
        ab_detail = ""

    # Top performing subject lines
    top_section = ""
    if top_subjects:
        top_section = "\n\nTOP SUBJECT LINES:\n"
        for i, subj in enumerate(top_subjects[:5], 1):
            top_section += "{}. {} ({}% open)\n".format(i, subj["subject"][:50], subj["open_rate"])

    msg = (
        "<b>WEEKLY ENGAGEMENT REPORT</b>\n"
        "========================\n\n"
        "TOTALS:\n"
        "  Sent: {sent}\n"
        "  Opens: {opens} ({open_rate:.1f}%)\n"
        "  Clicks: {clicks} ({click_rate:.1f}%)\n"
        "  Replies: {replies} ({reply_rate:.1f}%)\n"
        "  Meetings: {meetings}\n\n"
        "A/B TEST: {ab_status}{ab_detail}{top_section}\n\n"
        "BENCHMARKS (cold email avg):\n"
        "  Open: 15-25% | Click: 2-5% | Reply: 1-5%"
    ).format(
        sent=total_sent,
        opens=total_opens,
        open_rate=open_rate,
        clicks=total_clicks,
        click_rate=click_rate,
        replies=total_replies,
        reply_rate=reply_rate,
        meetings=total_meetings,
        ab_status=ab_status,
        ab_detail=ab_detail,
        top_section=top_section,
    )

    send_telegram(msg)
    return True


# ---------------------------------------------------------------------------
# Weekly Pipeline Dashboard
# ---------------------------------------------------------------------------

def build_pipeline_data(all_leads, contacted_leads, week_leads=None, prev_week=None):
    """Aggregate pipeline data from lead lists for the dashboard.

    Args:
        all_leads: list of all leads (from Sheets)
        contacted_leads: list of contacted leads
        week_leads: list of leads discovered this week (optional)
        prev_week: dict with last week's stats for comparison (optional)

    Returns dict suitable for notify_weekly_dashboard().
    """
    from collections import Counter

    # Total pipeline stages
    stage_counts = Counter()
    for lead in all_leads:
        stage = lead.get("pipeline_stage", "discovered").lower()
        stage_counts[stage] += 1

    total_pipeline = {
        "discovered": len(all_leads),
        "qualified": sum(1 for l in all_leads if float(l.get("lead_score", 0) or 0) >= 50),
        "contacted": stage_counts.get("contacted", 0) + stage_counts.get("replied", 0) + stage_counts.get("meeting", 0) + stage_counts.get("closed", 0),
        "replied": stage_counts.get("replied", 0) + stage_counts.get("meeting", 0) + stage_counts.get("closed", 0),
        "meeting": stage_counts.get("meeting", 0) + stage_counts.get("closed", 0),
        "closed": stage_counts.get("closed", 0),
    }

    # This week counts
    week_leads = week_leads or []
    this_week_contacts = sum(1 for l in contacted_leads if l.get("contacted_this_week", False))
    this_week_replies = sum(1 for l in contacted_leads if l.get("replied_this_week", False))
    this_week_meetings = sum(1 for l in contacted_leads if l.get("meeting_this_week", False))

    # Top niches
    niche_data = {}
    for lead in all_leads:
        niche = lead.get("niche", lead.get("search_query", "other"))
        if niche not in niche_data:
            niche_data[niche] = {"leads": 0, "replies": 0}
        niche_data[niche]["leads"] += 1
        if lead.get("reply_received", "").lower() in ("yes", "true", "1"):
            niche_data[niche]["replies"] += 1

    top_niches = []
    for niche, data in niche_data.items():
        reply_rate = round(data["replies"] / data["leads"] * 100, 1) if data["leads"] > 0 else 0
        top_niches.append({"niche": niche, "leads": data["leads"], "replies": data["replies"], "reply_rate": reply_rate})
    top_niches.sort(key=lambda x: x["reply_rate"], reverse=True)

    # Top cities
    city_data = {}
    for lead in all_leads:
        city = lead.get("city", "Unknown")
        if city not in city_data:
            city_data[city] = {"leads": 0, "replies": 0}
        city_data[city]["leads"] += 1
        if lead.get("reply_received", "").lower() in ("yes", "true", "1"):
            city_data[city]["replies"] += 1

    top_cities = [{"city": c, **d} for c, d in city_data.items()]
    top_cities.sort(key=lambda x: x["leads"], reverse=True)

    # Enrichment stats
    ps_scores = [int(l.get("pagespeed_mobile", 0) or 0) for l in all_leads if l.get("pagespeed_mobile")]
    enrichment_stats = {
        "pagespeed_avg": round(sum(ps_scores) / len(ps_scores)) if ps_scores else "N/A",
        "bbb_rated": sum(1 for l in all_leads if l.get("bbb_rating")),
        "linkedin_found": sum(1 for l in all_leads if l.get("linkedin_url")),
    }

    # Week-over-week
    weekly_comparison = {}
    if prev_week:
        weekly_comparison = {
            "leads_delta": len(week_leads) - prev_week.get("leads", 0),
            "contacts_delta": this_week_contacts - prev_week.get("contacts", 0),
            "replies_delta": this_week_replies - prev_week.get("replies", 0),
        }

    # Pipeline velocity (avg days from contact to reply)
    import datetime
    velocities = []
    for lead in contacted_leads:
        contact_date = lead.get("contact_date", "")
        reply_date = lead.get("reply_date", "")
        if contact_date and reply_date:
            try:
                cd = datetime.datetime.strptime(contact_date[:10], "%Y-%m-%d")
                rd = datetime.datetime.strptime(reply_date[:10], "%Y-%m-%d")
                velocities.append((rd - cd).days)
            except (ValueError, TypeError):
                pass

    pipeline_velocity = round(sum(velocities) / len(velocities), 1) if velocities else 0

    # Revenue estimate ($500 avg deal value per meeting)
    avg_deal = 500
    estimated_revenue = total_pipeline["meeting"] * avg_deal * 0.5  # 50% close rate assumption

    return {
        "leads_discovered": len(week_leads),
        "leads_contacted": this_week_contacts,
        "replies_received": this_week_replies,
        "meetings_booked": this_week_meetings,
        "total_pipeline": total_pipeline,
        "top_niches": top_niches[:5],
        "top_cities": top_cities[:5],
        "enrichment_stats": enrichment_stats,
        "weekly_comparison": weekly_comparison,
        "estimated_revenue": estimated_revenue,
        "pipeline_velocity": pipeline_velocity,
    }


def notify_weekly_dashboard(pipeline_data):
    """Send comprehensive weekly pipeline dashboard to Telegram.

    Args:
        pipeline_data: dict with keys:
            leads_discovered: int -- total leads found this week
            leads_qualified: int -- leads that passed scoring
            leads_contacted: int -- emails sent this week
            replies_received: int -- replies this week
            meetings_booked: int -- meetings this week
            total_pipeline: dict -- {discovered: N, qualified: N, contacted: N, replied: N, meeting: N, closed: N}
            top_niches: list of dict -- [{niche, leads, replies, reply_rate}]
            top_cities: list of dict -- [{city, leads, replies}]
            enrichment_stats: dict -- {pagespeed_avg, bbb_rated, linkedin_found}
            weekly_comparison: dict -- {leads_delta, contacts_delta, replies_delta}
            estimated_revenue: float -- projected revenue from pipeline
            pipeline_velocity: float -- avg days from contact to reply
    """
    # Conversion funnel
    tp = pipeline_data.get("total_pipeline", {})
    discovered = tp.get("discovered", 0)
    qualified = tp.get("qualified", 0)
    contacted = tp.get("contacted", 0)
    replied = tp.get("replied", 0)
    meeting = tp.get("meeting", 0)
    closed = tp.get("closed", 0)

    # Calculate conversion rates
    qual_rate = round(qualified / discovered * 100, 1) if discovered > 0 else 0
    contact_rate = round(contacted / qualified * 100, 1) if qualified > 0 else 0
    reply_rate = round(replied / contacted * 100, 1) if contacted > 0 else 0
    meeting_rate = round(meeting / replied * 100, 1) if replied > 0 else 0
    close_rate = round(closed / meeting * 100, 1) if meeting > 0 else 0

    funnel = (
        "CONVERSION FUNNEL:\n"
        "  Discovered:  {discovered}\n"
        "  Qualified:   {qualified} ({qual_rate}%)\n"
        "  Contacted:   {contacted} ({contact_rate}%)\n"
        "  Replied:     {replied} ({reply_rate}%)\n"
        "  Meeting:     {meeting} ({meeting_rate}%)\n"
        "  Closed:      {closed} ({close_rate}%)"
    ).format(
        discovered=discovered, qualified=qualified, contacted=contacted,
        replied=replied, meeting=meeting, closed=closed,
        qual_rate=qual_rate, contact_rate=contact_rate, reply_rate=reply_rate,
        meeting_rate=meeting_rate, close_rate=close_rate,
    )

    # This week's activity
    wk = pipeline_data
    weekly_activity = (
        "\nTHIS WEEK:\n"
        "  New leads:    {leads}\n"
        "  Emails sent:  {contacts}\n"
        "  Replies:      {replies}\n"
        "  Meetings:     {meetings}"
    ).format(
        leads=wk.get("leads_discovered", 0),
        contacts=wk.get("leads_contacted", 0),
        replies=wk.get("replies_received", 0),
        meetings=wk.get("meetings_booked", 0),
    )

    # Week-over-week comparison
    comp = pipeline_data.get("weekly_comparison", {})
    def _delta_str(val):
        if val is None or val == 0:
            return "flat"
        return "+{}".format(val) if val > 0 else str(val)

    comparison = (
        "\nWEEK-OVER-WEEK:\n"
        "  Leads:    {leads}\n"
        "  Contacts: {contacts}\n"
        "  Replies:  {replies}"
    ).format(
        leads=_delta_str(comp.get("leads_delta")),
        contacts=_delta_str(comp.get("contacts_delta")),
        replies=_delta_str(comp.get("replies_delta")),
    )

    # Top niches by reply rate
    top_niches = pipeline_data.get("top_niches", [])
    niche_section = ""
    if top_niches:
        niche_section = "\nTOP NICHES (by reply rate):\n"
        for i, n in enumerate(top_niches[:5], 1):
            niche_section += "  {}. {} -- {} leads, {}% reply\n".format(
                i, n.get("niche", "?"), n.get("leads", 0), n.get("reply_rate", 0))

    # Top cities
    top_cities = pipeline_data.get("top_cities", [])
    city_section = ""
    if top_cities:
        city_section = "\nTOP CITIES:\n"
        for i, c in enumerate(top_cities[:5], 1):
            city_section += "  {}. {} -- {} leads, {} replies\n".format(
                i, c.get("city", "?"), c.get("leads", 0), c.get("replies", 0))

    # Enrichment stats
    enrich = pipeline_data.get("enrichment_stats", {})
    enrich_section = ""
    if enrich:
        enrich_section = (
            "\nENRICHMENT STATS:\n"
            "  Avg PageSpeed:  {ps}\n"
            "  BBB Rated:      {bbb}\n"
            "  LinkedIn Found: {li}"
        ).format(
            ps=enrich.get("pagespeed_avg", "N/A"),
            bbb=enrich.get("bbb_rated", "N/A"),
            li=enrich.get("linkedin_found", "N/A"),
        )

    # ROI estimate
    revenue = pipeline_data.get("estimated_revenue", 0)
    velocity = pipeline_data.get("pipeline_velocity", 0)
    roi_section = ""
    if revenue > 0 or velocity > 0:
        roi_section = (
            "\nROI ESTIMATES:\n"
            "  Pipeline value:     ${revenue:,.0f}\n"
            "  Avg days to reply:  {velocity:.1f}"
        ).format(revenue=revenue, velocity=velocity)

    # Assemble full dashboard
    msg = (
        "<b>WEEKLY PIPELINE DASHBOARD</b>\n"
        "================================\n\n"
        "{funnel}\n"
        "{weekly_activity}\n"
        "{comparison}\n"
        "{niche_section}"
        "{city_section}"
        "{enrich_section}"
        "{roi_section}"
    ).format(
        funnel=funnel,
        weekly_activity=weekly_activity,
        comparison=comparison,
        niche_section=niche_section,
        city_section=city_section,
        enrich_section=enrich_section,
        roi_section=roi_section,
    )

    send_telegram(msg)
    return True


# ---------------------------------------------------------------------------
# Callback query handler (for inline button responses)
# ---------------------------------------------------------------------------

def poll_callback_queries(timeout_seconds=30):
    """
    Poll for callback query responses from inline buttons.
    Returns list of (action, draft_index) tuples.
    Note: In GitHub Actions (cron), this runs for a short window.
    For real-time approval, a webhook-based approach would be better.
    """
    if not TELEGRAM_BOT_TOKEN:
        return []

    url = _get_api_url("getUpdates")
    responses = []
    offset = 0
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            params = {
                "timeout": 5,
                "offset": offset,
                "allowed_updates": ["callback_query"],
            }
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()

            if not data.get("ok"):
                break

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                callback = update.get("callback_query", {})
                cb_data = callback.get("data", "")

                if ":" in cb_data:
                    action, index = cb_data.split(":", 1)
                    responses.append((action, index))

                    # Acknowledge the callback
                    _answer_callback(callback.get("id", ""), action)

                elif cb_data in ("approve_all", "skip_all"):
                    responses.append((cb_data, "all"))
                    _answer_callback(callback.get("id", ""), cb_data)

            if not data.get("result"):
                time.sleep(2)

        except Exception as e:
            print("[TELEGRAM] Poll error: {}".format(e))
            break

    return responses


def _answer_callback(callback_query_id, action):
    """Acknowledge a callback query so the button stops spinning."""
    if not callback_query_id:
        return

    url = _get_api_url("answerCallbackQuery")
    text_map = {
        "approve": "Approved! Will send.",
        "edit": "Reply with your edits.",
        "skip": "Skipped.",
        "approve_all": "All approved!",
        "skip_all": "All skipped.",
    }

    try:
        requests.post(url, json={
            "callback_query_id": callback_query_id,
            "text": text_map.get(action, "Got it."),
        }, timeout=5)
    except Exception:
        pass
