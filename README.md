📊 Looker Health Report Alerts

This Python script generates and emails a daily health report for a defined set of Looker Dashboards and Looks.
It helps teams proactively identify:

Unhealthy dashboards/looks (frequent query errors).

Dashboards/looks not run in the last 30 days (possible candidates for cleanup/archival).

Overall health statistics for monitored assets.

The report is delivered as an HTML email with links to the affected dashboards and looks.

🚀 Features

Monitors a defined list of Looker dashboard IDs and look IDs.

Flags dashboards/looks as unhealthy if they show repeated errors across multiple users.

Highlights dashboards/looks that haven’t been run in the last 30 days.

Sends a clean, formatted HTML email report via SMTP.

Can be scheduled with cron for automatic daily runs.

⚙️ Requirements

Python 3.8+

A Looker account with access to the system__activity model.

SMTP server credentials (e.g., Gmail, Outlook, or an internal SMTP relay).

Install dependencies:

pip install looker-sdk python-dotenv

🔑 Configuration
1. Looker API credentials

Create a looker.ini file in the same folder:

[Looker]
base_url=https://your.looker.instance:19999
client_id=YOUR_CLIENT_ID
client_secret=YOUR_CLIENT_SECRET

2. SMTP settings

Create a .env file named looker_alerts.env in the same folder:

SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your.email@example.com
SMTP_PASS=your_app_password_or_smtp_password
EMAIL_FROM=your.email@example.com
EMAIL_TO=recipient1@example.com,recipient2@example.com

3. Monitored IDs

Update the DASHBOARD_IDS and LOOK_IDS lists in the script with the IDs you want monitored.

▶️ Usage

Run manually:

python looker_alerts.py


The report will be printed to the terminal and sent via email.

📅 Automation

To schedule the script daily at 8:00 AM, add a cron job:

crontab -e


Add:

0 8 * * * /usr/bin/python3 /path/to/looker_alerts.py >> /path/to/looker_alerts.log 2>&1


Check logs with:

cat /path/to/looker_alerts.log

📧 Example Email Report

Looker Health Report - 2025-09-25

Dashboards Healthy: 29/31 (93.5%)

Looks Healthy: 17/17 (100.0%)

Sections:

✅ Dashboards Healthy

✅ Looks Healthy

⚠️ Unhealthy Dashboards (with clustered error messages)

⚠️ Unhealthy Looks

📉 Dashboards not run in last 30 days

📉 Looks not run in last 30 days

🔒 Security Notes

Do not commit your .env or looker.ini files to git.

Add them to .gitignore:

looker_alerts.env:
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_email_password
EMAIL_FROM=your_email@gmail.com
EMAIL_TO=recipient1@example.com,recipient2@example.com


looker.ini
[Looker]
base_url=https://your-looker-instance.com
client_id=YOUR_CLIENT_ID
client_secret=YOUR_CLIENT_SECRET

Use environment variables or a secrets manager in production.

📜 License

MIT License – free to use, modify, and share.# looker_alerts
