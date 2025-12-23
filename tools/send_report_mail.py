#!/root/betfair_profitbox/.venv/bin/python3
import os, sys, subprocess, smtplib, ssl, mimetypes
from email.message import EmailMessage
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# --- Config / Env ---
ENV_PATH = "/root/betfair_profitbox/.env"
load_dotenv(ENV_PATH)

def require(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        print(f"❌ Missing required env var: {name}", file=sys.stderr)
        sys.exit(2)
    return v.strip()

host = require("SMTP_HOST")
port = int(os.getenv("SMTP_PORT", "587"))
user = require("SMTP_USER")
pwd  = require("SMTP_PASS")
to   = require("MAIL_TO")
mail_from = os.getenv("MAIL_FROM", user).strip() or user
smtp_debug = int(os.getenv("SMTP_DEBUG", "0"))

print(f"ℹ️ Loaded .env from: {ENV_PATH}")
print(f"ℹ️ SMTP_HOST={host}  SMTP_PORT={port}")
print(f"ℹ️ SMTP_USER={user}  MAIL_FROM={mail_from}")
print(f"ℹ️ MAIL_TO={to}")
print(f"ℹ️ SMTP_DEBUG={smtp_debug}")

# Permissive TLS context to mirror your working behavior (no hostname/cert checks)
perm_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
perm_context.check_hostname = False
perm_context.verify_mode = ssl.CERT_NONE

# --- Helpers ---
def run_script(path: str) -> str:
    print(f"↪️ Running: {path}")
    run = subprocess.run([path], capture_output=True, text=True)
    print(f"   returncode={run.returncode}")
    body = (run.stdout or "") + (f"\n[stderr]\n{run.stderr}" if run.stderr else "")
    return body if body.strip() else "(no output)"

def attach_file(msg: EmailMessage, file_path: Path):
    if not file_path.is_file():
        return False
    mime_type, _ = mimetypes.guess_type(file_path)
    maintype, subtype = (mime_type.split("/", 1) if mime_type else ("application", "octet-stream"))
    with open(file_path, "rb") as f:
        msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=file_path.name)
    print(f"📎 Attached: {file_path}")
    return True

def send_email(subject: str, body: str, attachments: list[Path] | None = None):
    msg = EmailMessage()
    msg["From"] = mail_from
    recipients = [to, "mccoussens@gmail.com", "Matthew.westby@allenovery.com", "Matthew.westby@aoshearman.com"]
    msg['To'] = ", ".join(recipients)
    print(msg['To'])
    msg["Subject"] = subject
    msg.set_content(body)

    # Add attachments (if any)
    if attachments:
        for p in attachments:
            attach_file(msg, p)

    try:
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.set_debuglevel(smtp_debug)
            s.ehlo()
            s.starttls(context=perm_context)   # permissive TLS, like your working script
            s.ehlo()
            s.login(user, pwd)
            s.send_message(msg)
        print(f"✅ Sent: {subject}")
    except Exception as e:
        print(f"❌ Failed to send '{subject}': {e}")
        sys.exit(1)

# --- Main ---
now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

# 1) Trading report + optional PNG attachment
run_this = "/root/betfair_profitbox/jobs/eod_dump_trades.py"
body1 = run_script(run_this)
run_this = "/root/betfair_profitbox/jobs/eod_gen_trade_charts.py"
_null = run_script(run_this)

refresh_chart = "/root/betfair_profitbox/tools/pnl_chart.py"
body1 += run_script(refresh_chart)

chart_path = Path("/root/betfair_profitbox/store/account_stats/date_equity_pnl.png")
if chart_path.is_file():
    body1 += "\n\n📈 Attached: daily PnL chart (date_equity_pnl.png)"


TODAY = datetime.utcnow().strftime("%Y-%m-%d")
CSV_PATH = f"/root/betfair_profitbox/store/trade_csv/{TODAY}.csv"
OUT_DIR = "/root/betfair_profitbox/store/trade_chart"
OUT_IMG = Path(os.path.join(OUT_DIR, f"{TODAY}.png"))
if OUT_IMG.is_file():
    body1 += "\n📈 Attached: daily PnL chart" + f"{TODAY}.png"

send_email(
    subject=f"Betfair trading report {now_utc}",
    body=body1,
    attachments=[chart_path, OUT_IMG] if chart_path.is_file() else None
)

# 2) Next 24h markets (no attachment)
# MARKETS = "/root/betfair_profitbox/tools/next_24h_cricket_markets.py"
# body2 = run_script(MARKETS)
# send_email(
#     subject=f"Next 24hr Markets {now_utc}",
#     body=body2
# )
