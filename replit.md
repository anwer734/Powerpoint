# Telegram Group Manager

## Overview
A Flask web application for managing Telegram accounts and groups. Supports multiple users, auto-messaging, monitoring, and more.

## Tech Stack
- **Backend**: Python 3.11, Flask, Flask-SocketIO, Telethon
- **Frontend**: HTML/CSS/JS (single-page app) with Font Awesome icons
- **Protocol**: WebSocket via Socket.IO (polling transport)

## Running the App
The app runs on port 5000 via `python app.py`.

## Key Files
- `app.py` - Main Flask application (3300+ lines)
- `templates/index.html` - Frontend single-page application
- `static/` - Icons, PWA manifest, service worker
- `sessions/` - User session JSON files (gitignored)

## Environment Variables
- `SESSION_SECRET` - Flask secret key (default: "telegram_secret_2024")
- `GITHUB_TOKEN` - (Optional) GitHub token for session backup
- `GITHUB_REPO` - (Optional) GitHub repo for session storage (format: "user/repo")
- `PORT` - Server port (default: 5000)

## Admin Credentials
- Username: `admin`
- Password: `772997043anwer`

## Deployment (Render)
- `requirements.txt` - Python dependencies
- `Procfile` - Gunicorn start command with eventlet worker
- `render.yaml` - Render service configuration

## Bug Fixed
- Fixed `btoa()` crash in `parseJoinLinks()` function when group labels contained Arabic text. Changed to use index-based IDs instead.
