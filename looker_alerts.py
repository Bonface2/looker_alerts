import os
import json
import smtplib
import configparser
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from email.mime.text import MIMEText

import looker_sdk
from looker_sdk import models40 as models
from dotenv import load_dotenv

# --- Load SMTP environment variables from custom .env ---
env_path = Path(__file__).parent / "looker_alerts.env"
load_dotenv(dotenv_path=env_path)

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")  # comma-separated allowed

if not SMTP_USER or not SMTP_PASS:
    raise RuntimeError("SMTP_USER or SMTP_PASS not loaded from .env")

# --- Looker SDK init and base_url from looker.ini ---
sdk = looker_sdk.init40("looker.ini")
config = configparser.ConfigParser()
config.read("looker.ini")
base_url = config["Looker"]["base_url"].rstrip("/")

# --- IDs to monitor (your provided lists) ---
DASHBOARD_IDS = [
    615, 5382, 5219, 4670, 4355, 1627, 832, 6215, 5007, 3574, 2673, 2068,
    1577, 1273, 1127, 721, 6797, 6496, 5631, 5599, 5555, 5549, 5065, 4999,
    4960, 4066, 3812, 3491, 3312, 2083, 1600, 5083, 5572
]
LOOK_IDS = [
    13281, 13283, 13358, 13360, 13815, 14245, 14300, 14317, 14319, 14320,
    14791, 14792, 14793, 14794, 14795, 14797, 14905, 14917, 14918, 14919,
    14926, 14928, 14929, 14931, 16005, 16366, 16562, 16566, 16696, 16817,
    17111, 18264, 18265, 18267, 18268, 18269, 18381, 18595, 19491, 19548,
    19554, 19681, 20272, 20535, 20642, 20651, 22170, 22211, 22393, 23834,
    24762, 25389, 25421, 25427, 25429, 25430, 25432, 25447, 25578, 25579,
    25622, 25651, 25659, 25739, 26006, 26180, 26181, 26363, 26488, 26824,
    26825, 26826, 26827, 26828, 2724, 3173, 3527, 3528, 3529, 3530, 3531,
    3535, 3536, 3537, 3538, 3539, 3540, 3541, 3542, 3690, 3691, 3692, 3693,
    3694, 3696, 3697, 5730, 8108, 8110, 8126, 8131, 8410, 8721, 8722, 8724,
    8725, 8727
]


# --- Helpers ---
def parse_time(ts: str):
    """Return timezone-aware datetime or None."""
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone()
        except Exception:
            continue
    return None


def collapse_errors(errors):
    """Collapse errors into 30-minute clusters and return representatives (oldest-first)."""
    errs = [e for e in errors if e.get("time")]
    errs.sort(key=lambda x: x["time"])
    clusters = []
    last_time = None
    for e in errs:
        t = e["time"]
        if last_time is None or (t - last_time) > timedelta(minutes=30):
            clusters.append(e)
            last_time = t
        else:
            # within 30m of last_time => same cluster => skip
            continue
    return clusters


def days_since_run(ts_raw):
    """Return '45 days ago', 'Never run', or 'Unknown' from a timestamp string."""
    if not ts_raw:
        return "Never run"
    dt = parse_time(ts_raw) if isinstance(ts_raw, str) else ts_raw
    if not dt:
        return "Unknown"
    delta = datetime.now(dt.tzinfo) - dt
    return f"{delta.days} days ago"


def is_unhealthy(clusters):
    """
    Unhealthy if more than 5 collapsed clusters in the past 7 days AND
    clusters involve more than one distinct user.
    """
    if len(clusters) <= 5:
        return False
    users = {c.get("user_id") for c in clusters if c.get("user_id")}
    return len(users) > 1


# --- Fetch set of dashboards/looks that had ANY run in last 30 days ---
def fetch_ids_ran_last_30_days():
    # dashboards
    q_dash = models.WriteQuery(
        model="system__activity",
        view="history",
        fields=["dashboard.id"],
        filters={"history.created_time": "30 days"},
        limit=5000,
    )
    dash_rows = json.loads(sdk.run_inline_query("json", q_dash))
    ran_dash_ids = {str(r["dashboard.id"]) for r in dash_rows if r.get("dashboard.id")}

    # looks
    q_look = models.WriteQuery(
        model="system__activity",
        view="history",
        fields=["look.id"],
        filters={"history.created_time": "30 days"},
        limit=5000,
    )
    look_rows = json.loads(sdk.run_inline_query("json", q_look))
    ran_look_ids = {str(r["look.id"]) for r in look_rows if r.get("look.id")}

    return ran_dash_ids, ran_look_ids


# --- Fetch recent errors (last 7 days) ---
def fetch_recent_errors():
    wq = models.WriteQuery(
        model="system__activity",
        view="history",
        fields=[
            "history.dashboard_id",
            "history.look_id",
            "query.id",
            "history.message",
            "user.name",
            "history.created_time",
        ],
        filters={"history.created_time": "7 days", "history.status": "error"},
        sorts=["history.created_time desc"],
        limit=5000,
    )


    raw = sdk.run_inline_query("json", wq)
    rows = json.loads(raw)

    dash_map = defaultdict(list)
    look_map = defaultdict(list)

    for row in rows:
        t = parse_time(row.get("history.created_time"))
        entry = {
            "time": t,
            "query_id": row.get("query.id"),
            "user_id": row.get("user.name"),
            "message": row.get("history.message", "Unknown error"),
        }
        d = row.get("history.dashboard_id")
        l = row.get("history.look_id")
        if d:
            dash_map[str(d)].append(entry)
        if l:
            look_map[str(l)].append(entry)

    return dash_map, look_map


# --- Fetch latest run (any time) for monitored ids if they DID NOT run in last 30 days ---
# We only call per-id SDK to get last_run_at when needed (small number of monitored ids).
def fetch_last_run_fallback(monitored_dash_ids, monitored_look_ids, ran_dash_set, ran_look_set):
    last_run_map = {}  # key: "dashboard:615" or "look:24762" -> timestamp string or None

    # Only inspect those that did NOT run in last 30d
    for did in monitored_dash_ids:
        did_s = str(did)
        if did_s in ran_dash_set:
            continue
        try:
            obj = sdk.dashboard(did)
            lr = getattr(obj, "last_run_at", None)
            last_run_map[f"dashboard:{did_s}"] = lr
        except Exception:
            last_run_map[f"dashboard:{did_s}"] = None

    for lid in monitored_look_ids:
        lid_s = str(lid)
        if lid_s in ran_look_set:
            continue
        try:
            obj = sdk.look(lid)
            lr = getattr(obj, "last_run_at", None)
            last_run_map[f"look:{lid_s}"] = lr
        except Exception:
            last_run_map[f"look:{lid_s}"] = None

    return last_run_map


# --- Build report ---
def build_report():
    ran_dash_set, ran_look_set = fetch_ids_ran_last_30_days()
    dash_map, look_map = fetch_recent_errors()

    # fallback last_run map only for those that did NOT run in last 30 days
    last_run_map = fetch_last_run_fallback(DASHBOARD_IDS, LOOK_IDS, ran_dash_set, ran_look_set)

    unhealthy_dashboards = {}
    never_ran_dashboards = []
    for did in DASHBOARD_IDS:
        did_s = str(did)
        clusters = collapse_errors(dash_map.get(did_s, []))
        if is_unhealthy(clusters):
            unhealthy_dashboards[did_s] = {"clusters": clusters[:-5]}
        else:
            # determine if DID had ANY run in last 30 days (fast check)
            if did_s not in ran_dash_set:
                # compute 'days since last run' from fallback (may be None)
                lr = days_since_run(last_run_map.get(f"dashboard:{did_s}"))
                # Only include in "not run in last 30 days" if >30 days or never run
                if lr == "Never run":
                    never_ran_dashboards.append((did_s, lr))
                elif lr.endswith("days ago"):
                    try:
                        days = int(lr.split()[0])
                        if days > 30:
                            never_ran_dashboards.append((did_s, lr))
                    except Exception:
                        pass

    unhealthy_looks = {}
    never_ran_looks = []
    for lid in LOOK_IDS:
        lid_s = str(lid)
        clusters = collapse_errors(look_map.get(lid_s, []))
        if is_unhealthy(clusters):
            unhealthy_looks[lid_s] = {"clusters": clusters[:5]}
        else:
            if lid_s not in ran_look_set:
                lr = days_since_run(last_run_map.get(f"look:{lid_s}"))
                if lr == "Never run":
                    never_ran_looks.append((lid_s, lr))
                elif lr.endswith("days ago"):
                    try:
                        days = int(lr.split()[0])
                        if days > 30:
                            never_ran_looks.append((lid_s, lr))
                    except Exception:
                        pass

    # counts among monitored IDs
    total_dashboards = len(DASHBOARD_IDS)
    total_looks = len(LOOK_IDS)
    healthy_dashboards = total_dashboards - len(unhealthy_dashboards)
    healthy_looks = total_looks - len(unhealthy_looks)

    # Build HTML email body (formatted for leaders)
    html_parts = []
    html_parts.append(f"<h1 style='font-size:22px;'><b>Looker Health Report - {datetime.now().date()}</b></h1>")
    html_parts.append("<hr/>")
    html_parts.append(f"<p><b>Dashboards Healthy:</b> {healthy_dashboards}/{total_dashboards} ({healthy_dashboards/total_dashboards*100:.1f}%)</p>")
    html_parts.append(f"<p><b>Looks Healthy:</b> {healthy_looks}/{total_looks} ({healthy_looks/total_looks*100:.1f}%)</p>")

    # Unhealthy dashboards (only these show errors)
    if unhealthy_dashboards:
        html_parts.append("<h2><b>Unhealthy Dashboards</b></h2>")
        for did, info in unhealthy_dashboards.items():
            html_parts.append(f"<p><a href='{base_url}/dashboards/{did}'><b>Dashboard {did}</b></a></p>")
            html_parts.append("<ul>")
            for c in info["clusters"]:
                t = c["time"].strftime("%Y-%m-%d %H:%M:%S") if c["time"] else "unknown"
                msg = c.get("message", "Unknown error")
                qid = c.get("query_id")
                user = c.get("user_id", "unknown")
                html_parts.append(f"<li>{t}: Query {qid} - {msg} (User {user})</li>")
            html_parts.append("</ul>")

    # Unhealthy looks
    if unhealthy_looks:
        html_parts.append("<h2><b>Unhealthy Looks</b></h2>")
        for lid, info in unhealthy_looks.items():
            html_parts.append(f"<p><a href='{base_url}/looks/{lid}'><b>Look {lid}</b></a></p>")
            html_parts.append("<ul>")
            for c in info["clusters"]:
                t = c["time"].strftime("%Y-%m-%d %H:%M:%S") if c["time"] else "unknown"
                msg = c.get("message", "Unknown error")
                qid = c.get("query_id")
                user = c.get("user_id", "unknown")
                html_parts.append(f"<li>{t}: Query {qid} - {msg} (User {user})</li>")
            html_parts.append("</ul>")

    # Dashboards not run in last 30 days (include last-run info)
    if never_ran_dashboards:
        html_parts.append("<h2><b>Dashboards not run in last 30 days</b></h2>")
        html_parts.append("<ul>")
        for did, lr in never_ran_dashboards:
            html_parts.append(f"<li><a href='{base_url}/dashboards/{did}'>Dashboard {did}</a></li>")
        html_parts.append("</ul>")
    else:
        html_parts.append("<h2><b>Dashboards not run in last 30 days</b></h2>")
        html_parts.append("<p>All monitored dashboards have run in the last 30 days.</p>")

    # Looks not run in last 30 days (include last-run info)
    if never_ran_looks:
        html_parts.append("<h2><b>Looks not run in last 30 days</b></h2>")
        html_parts.append("<ul>")
        for lid, lr in never_ran_looks:
            html_parts.append(f"<li><a href='{base_url}/looks/{lid}'>Look {lid}</a></li>")
        html_parts.append("</ul>")
    else:
         html_parts.append("<h2><b>Looks not run in last 30 days</b></h2>")
         html_parts.append("<p>All monitored looks have run in the last 30 days.</p>")

    # Footer
    html_parts.append("<hr/>")
    html_parts.append(f"<p>Report generated: {datetime.now().isoformat()}</p>")

    return "\n".join(html_parts)


def send_email(html_body: str):
    msg = MIMEText(html_body, "html")
    msg["Subject"] = f"Looker Health Report - {datetime.now().date()}"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO

    recipients = [r.strip() for r in EMAIL_TO.split(",") if r.strip()]

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(EMAIL_FROM, recipients, msg.as_string())


if __name__ == "__main__":
    body = build_report()
    print(body)
    send_email(body)

