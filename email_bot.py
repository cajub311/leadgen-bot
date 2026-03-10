"""
email_bot.py  -- LeadGen Bot v3
AI-powered cold email personalizer with CAN-SPAM compliance.
Features: industry-specific templates, A/B subject lines, 3-email drip sequences,
Google review personalization. Drafts via Claude -- does NOT auto-send.
"""

import os
import re
import json
import time
import csv
import datetime
import requests

import imaplib
import email as email_lib
from email.header import decode_header
import hashlib

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from config import (
    ANTHROPIC_API_KEY, ANTHROPIC_ENDPOINT, ANTHROPIC_MODEL,
    GMAIL_ADDRESS, GMAIL_APP_PASSWORD, MAX_DRAFTS_PER_RUN, CAN_SPAM_FOOTER,
    INDUSTRY_ANGLES, MIN_SCORE_FOR_DRAFT, FOLLOW_UP_RULES,
    EMAIL_WARMUP_SCHEDULE, EMAIL_MAX_PER_DAY_DEFAULT,
    EMAIL_SEND_DAYS, EMAIL_SEND_HOURS, EMAIL_SEND_TIMEZONE,
    EMAIL_WARMUP_START,
    TRACKING_PIXEL_BASE_URL, LINK_TRACKER_BASE_URL,
    AB_MIN_SENDS_FOR_WINNER, AB_WIN_THRESHOLD,
    ENGAGEMENT_WEIGHTS,
)
from sheets_client import (
    is_connected as sheets_connected,
    get_leads_by_stage, update_lead_multiple_fields,
    get_leads_needing_followup,
)