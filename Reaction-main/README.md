# Reaction Reporter Bot

This project provides a Telegram bot that coordinates multiple Pyrogram session strings to submit reports against profiles, groups, channels, or stories. The bot ships with a premium, dark-themed chat UI with clear buttons and guardrails for every step.

## Features
- Guided flow that collects API ID, API Hash, and 1–500 Pyrogram session strings.
- Supports reporting up to 5 Telegram links at once (private groups, public groups/channels, or profiles/stories).
- Choose report type with inline buttons and provide a short reason message.
- Sends 500–7000 report attempts (default 5000) using saved sessions from MongoDB plus user-provided ones.
- Tracks successful/failed submissions, detects invalid sessions, and stops early if Telegram rejects the target.
- `/sessions` shows saved and currently loaded session counts.
- No developer/admin/owner info is displayed; the UI keeps buttons clear and non-overlapping.

## Requirements
- Python 3.10+
- Telegram Bot Token
- Telegram API ID and API Hash (https://my.telegram.org)
- MongoDB connection string (optional but recommended for session persistence). If
  `MONGO_URI` is set, the bot will use Motor/PyMongo for durable storage; leave it
  empty to fall back to in-memory mode.
- Dependencies from `requirements.txt`

## Deploy to Heroku
Use the one-click button after forking the repository (update `<your-username>` in the URL to match your GitHub handle):

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/Oxeigns/Reaction)

The `app.json` file configures the worker process and required config vars so Heroku can provision the app automatically.

## Environment variables
Set these before running the bot (or edit `config.py` to source them another way).
If any required value is missing or malformed, the bot will exit with a clear
error instead of crashing later with a Telegram RPC failure:

- `BOT_TOKEN`: Telegram bot token.
- `API_ID`: Default Telegram API ID (used if the user does not provide one).
- `API_HASH`: Default Telegram API Hash.
- `MONGO_URI`: MongoDB connection string (leave empty to use in-memory storage).

## Installation
```bash
# Clone
git clone <repo_url>
cd Reaction

# Install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running on a VPS (Linux/macOS)
```bash
export BOT_TOKEN=<your_bot_token>
export API_ID=<your_api_id>
export API_HASH=<your_api_hash>
export MONGO_URI="mongodb+srv://<user>:<pass>@<host>/<db>"
python main.py
```
The bot starts polling Telegram. Use `/report` to begin.

## Running on Termux
```bash
pkg install python git
cd ~
git clone <repo_url>
cd Reaction
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export BOT_TOKEN=<your_bot_token>
export API_ID=<your_api_id>
export API_HASH=<your_api_hash>
python main.py
```

## Running on Replit
1. Create a new Replit Python project and import this repository.
2. Add environment variables (`BOT_TOKEN`, `API_ID`, `API_HASH`, optionally `MONGO_URI`) in the Secrets manager.
3. In the shell, install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run `python main.py`. Ensure the Replit always-on/uptime solution is enabled if needed.

## Running on Heroku
1. Create a Heroku app and attach a MongoDB add-on (optional but recommended).
2. Set Config Vars: `BOT_TOKEN`, `API_ID`, `API_HASH`, and `MONGO_URI` (if available).
3. Deploy the repository (the included `Procfile` already defines the worker dyno: `worker: python main.py`).
4. Scale the worker dyno: `heroku ps:scale worker=1`.
5. Optional: set `ADMIN_IDS` (comma-separated Telegram user IDs) to allow trusted operators to issue `/restart`.

### Reliability tips for Heroku
- Keep only one worker dyno running to avoid duplicate schedulers and duplicate polling.
- Disable automatic deploys if you see continuous build/release loops after every commit; instead deploy only known-good revisions.
- If Telegram connectivity is flaky, the bot now retries with exponential backoff instead of crashing the dyno.
- Provide a `SOURCE_VERSION`, `HEROKU_RELEASE_VERSION`, or `GIT_REV` config var to surface the running version in `/uptime`.
- Prefer a Hobby dyno or larger if the free/basic tiers restart too frequently; add a simple Pingdom/Upptime health check against `/ping` to spot issues early.

## Bot commands
- `/start` – open the control panel.
- `/report` – begin a guided report (collects API credentials, sessions, target links, report type, reason, and count).
- `/addsessions` – store additional session strings.
- `/sessions` – view saved and currently loaded sessions.
- `/uptime` – show process uptime, server time, and build/version.
- `/ping` – measure Telegram round-trip latency with CPU/memory usage.
- `/restart` – admin-only graceful restart of the worker.
- `/help` – show usage instructions.
- `/cancel` – abort the current flow.

## Dark-themed chat mockup
Use the following layout as a quick reference for presenting the bot inside a dark-themed mobile chat UI (Telegram-style):

- **Chat header:** Standard bot header at the top.
- **User input:** A bubble with the text `/start` to kick things off.
- **Bot reply box:** A large, clearly bordered card beneath the user bubble with:
  - **Title:** An emoji-led heading such as `👋 Welcome! Bot Instructions`.
  - **Content:** A bullet list outlining reporting, session management, and API ID/Hash usage in a technical, well-spaced format.
  - **Call to action:** Finish with `Use /report to begin or the panel below.`
- **Buttons:** Stack three prominent rounded buttons under the reply box: `Start_Report_`, `Add_Sessions_`, and `View_Saved_`.

## Notes
- Minimum reports per link: 500; maximum: 7000; default: 5000.
- Minimum sessions: 1; maximum: 500. Invalid sessions are skipped automatically.
- The bot stops reporting if Telegram indicates the target is unavailable or deleted.
