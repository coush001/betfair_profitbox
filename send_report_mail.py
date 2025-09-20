#!/root/betting/betenv/bin/python3
import os, subprocess, smtplib
from email.message import EmailMessage
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")

SNAPSHOT = str(BASE / "account_orders_report.py")
host = os.getenv("SMTP_HOST"); port = int(os.getenv("SMTP_PORT", "587"))
user = os.getenv("SMTP_USER");  pwd  = os.getenv("SMTP_PASS")
to   = os.getenv("MAIL_TO")

run = subprocess.run([SNAPSHOT], capture_output=True, text=True)
body = (run.stdout or "") + (f"\n[stderr]\n{run.stderr}" if run.stderr else "")
if not body.strip(): body = "(no output)"

msg = EmailMessage()
msg["From"] = user
msg["To"] = to
msg["Subject"] = f"Betfair trading report {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
msg.set_content(body)

with smtplib.SMTP(host, port) as s:
    s.starttls()
    s.login(user, pwd)
    s.send_message(msg)


markets = str(BASE / "next_24h_cricket_markets.py")
run = subprocess.run([markets], capture_output=True, text=True)
body = (run.stdout or "") + (f"\n[stderr]\n{run.stderr}" if run.stderr else "")
if not body.strip(): body = "(no output)"
msg = EmailMessage()
msg["From"] = user
msg["To"] = to
msg["Subject"] = f"Next 24hr Markets {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
msg.set_content(body)

with smtplib.SMTP(host, port) as s:
    s.starttls()
    s.login(user, pwd)
    s.send_message(msg)
