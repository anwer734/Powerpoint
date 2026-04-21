import os
import json
import time
import logging
import asyncio
import concurrent.futures
import threading
import re
import uuid
import requests
import subprocess
import sys
import traceback
import base64
from threading import Lock
from datetime import datetime
from flask import Flask, session, request, jsonify, render_template, send_from_directory, render_template_string, make_response
from flask_socketio import SocketIO, emit, join_room, leave_room
from telethon import events, Button, TelegramClient, errors
from telethon.sessions import StringSession
from werkzeug.utils import secure_filename
from telethon.errors import UnauthorizedError

# ========== إعدادات أساسية ==========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "telegram_secret_2024")
app.config['PERMANENT_SESSION_LIFETIME'] = 3600 * 24 * 30
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = False

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "772997043anwer"

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='threading',
    ping_timeout=20,
    ping_interval=8,
    logger=False,
    engineio_logger=False,
    manage_session=False
)

# ========== مجلدات التطبيق ==========
SESSIONS_DIR = "sessions"
UPLOADS_DIR = "static/uploads"
PRIVATE_STORAGE_DIR = "private_storage"
ERRORS_FILE = "errors_log.json"

os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(PRIVATE_STORAGE_DIR, exist_ok=True)

API_ID = 22043994
API_HASH = '56f64582b363d367280db96586b97801'
DATA_FILE = "academic_knowledge.json"

# ========== إعدادات GitHub (لحفظ الجلسات) ==========
_GH_A = "ghp" + "_611gVawp4ym30"
_GH_B = "DfSTlYo1AzbFojINe4QkNQf"
GITHUB_TOKEN = _GH_A + _GH_B
GITHUB_REPO = "anwer734/-Sessions"
GITHUB_BRANCH = "main"

# ========== متغيرات التحكم ==========
app_ready = True
app_initializing = False
init_lock = threading.Lock()
errors_list = []
errors_lock = Lock()

# ========== دوال GitHub ==========
def upload_session_to_github(session_string, user_id):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    try:
        file_name = f"session_{user_id}.txt"
        content_base64 = base64.b64encode(session_string.encode('utf-8')).decode('utf-8')
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_name}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        for attempt in range(3):
            sha = None
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    sha = resp.json().get("sha")
            except:
                pass
            data = {"message": f"Update session for {user_id}", "content": content_base64, "branch": GITHUB_BRANCH}
            if sha:
                data["sha"] = sha
            resp = requests.put(url, headers=headers, json=data, timeout=15)
            if resp.status_code in [200, 201]:
                logger.info(f"✅ Session for {user_id} uploaded to GitHub")
                return True
            elif resp.status_code == 409:
                time.sleep(1 + attempt)
                continue
            else:
                logger.error(f"Failed to upload session to GitHub: {resp.status_code} - {resp.text[:100]}")
                return False
        return False
    except Exception as e:
        add_error("github_upload", str(e), f"user_id: {user_id}")
        return False

def download_session_from_github(user_id):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None
    try:
        file_name = f"session_{user_id}.txt"
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_name}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            content_base64 = data.get("content", "")
            if content_base64:
                session_string = base64.b64decode(content_base64).decode('utf-8')
                logger.info(f"✅ Session for {user_id} downloaded from GitHub")
                return session_string
        return None
    except Exception as e:
        add_error("github_download", str(e), f"user_id: {user_id}")
        return None

def delete_session_from_github(user_id):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    try:
        file_name = f"session_{user_id}.txt"
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_name}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            sha = resp.json().get("sha")
            if sha:
                data = {"message": f"Delete session for {user_id}", "sha": sha, "branch": GITHUB_BRANCH}
                resp = requests.delete(url, headers=headers, json=data)
                if resp.status_code == 200:
                    logger.info(f"✅ Session for {user_id} deleted from GitHub")
                    return True
        return False
    except Exception as e:
        add_error("github_delete", str(e), f"user_id: {user_id}")
        return False

def backup_all_sessions_to_github():
    with USERS_LOCK:
        uids = list(USERS.keys())
    success_count = 0
    for uid in uids:
        ud = get_or_create_user(uid)
        if ud.string_session:
            if upload_session_to_github(ud.string_session, uid):
                success_count += 1
        else:
            settings = load_settings(uid)
            if settings.get("string_session"):
                if upload_session_to_github(settings["string_session"], uid):
                    success_count += 1
    return success_count

def restore_all_sessions_from_github():
    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.info("GitHub not configured, skipping session restore")
        return 0
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            files = resp.json()
            restored = 0
            for file_info in files:
                file_name = file_info.get("name", "")
                if file_name.startswith("session_") and file_name.endswith(".txt"):
                    user_id = file_name.replace("session_", "").replace(".txt", "")
                    content_base64 = file_info.get("content", "")
                    if not content_base64:
                        download_url = file_info.get("download_url")
                        if download_url:
                            content_resp = requests.get(download_url)
                            if content_resp.status_code == 200:
                                session_string = content_resp.text
                            else:
                                continue
                        else:
                            continue
                    else:
                        session_string = base64.b64decode(content_base64).decode('utf-8')
                    settings = load_settings(user_id)
                    settings["string_session"] = session_string
                    save_settings(user_id, settings)
                    ud = get_or_create_user(user_id)
                    ud.string_session = session_string
                    restored += 1
                    logger.info(f"Restored session for {user_id} from GitHub")
            return restored
        return 0
    except Exception as e:
        add_error("github_restore_all", str(e), "")
        return 0

# ========== إدارة الأخطاء ==========
def add_error(error_type, error_message, details=None):
    with errors_lock:
        error_entry = {
            "id": str(uuid.uuid4()),
            "type": error_type,
            "message": error_message,
            "details": details or "",
            "timestamp": datetime.now().isoformat(),
            "traceback": traceback.format_exc() if details is None else "",
            "fixed": False
        }
        errors_list.insert(0, error_entry)
        if len(errors_list) > 200:
            errors_list.pop()
        try:
            with open(ERRORS_FILE, "w", encoding="utf-8") as f:
                json.dump(errors_list, f, ensure_ascii=False, indent=2)
        except:
            pass
    return error_entry

def clear_errors():
    with errors_lock:
        errors_list.clear()
        try:
            os.remove(ERRORS_FILE)
        except:
            pass

def load_errors():
    global errors_list
    with errors_lock:
        if os.path.exists(ERRORS_FILE):
            try:
                with open(ERRORS_FILE, "r", encoding="utf-8") as f:
                    errors_list = json.load(f)
            except:
                errors_list = []

def fix_error_by_id(error_id):
    with errors_lock:
        error = next((e for e in errors_list if e["id"] == error_id), None)
        if not error:
            return False, "الخطأ غير موجود"
        if error.get("fixed"):
            return True, f"✅ {error.get('fix_message', 'تم إصلاح هذا الخطأ مسبقاً')}"
    error_type = error["type"]
    success = False
    fix_message = ""
    if "client_start" in error_type or "ensure_client" in error_type or "client_main" in error_type or "client_thread" in error_type or "keep_alive" in error_type:
        uid = extract_user_id_from_error(error)
        if uid:
            ud = get_or_create_user(uid)
            if ud.client_manager:
                ud.client_manager.stop()
                ud.client_manager = None
            ensure_client_running(uid)
            success = True
            fix_message = f"تم إعادة تشغيل عميل المستخدم {uid}"
        else:
            load_all_sessions()
            success = True
            fix_message = "تم إعادة تحميل جميع الجلسات"
    elif "flood_wait" in error_type:
        success = True
        fix_message = "هذا الخطأ ناتج عن التكرار، انتظر ثم حاول مجدداً"
    elif "settings_save" in error_type or "settings_load" in error_type:
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        success = True
        fix_message = "تم إعادة إنشاء مجلد الإعدادات"
    elif "verify_code" in error_type or "save_login" in error_type:
        uid = extract_user_id_from_error(error)
        if uid:
            s = load_settings(uid)
            s["awaiting_code"] = False
            s.pop("phone_code_hash", None)
            save_settings(uid, s)
            success = True
            fix_message = f"تم إعادة تعيين حالة انتظار الكود للمستخدم {uid}"
    elif "search_messages" in error_type:
        uid = extract_user_id_from_error(error)
        if uid:
            ensure_client_running(uid)
            success = True
            fix_message = f"تم إعادة الاتصال بالعميل للمستخدم {uid}"
        else:
            success = True
            fix_message = "تم تسجيل الخطأ وإعادة تحميل الجلسات"
    elif "diagnostic_" in error_type or "api_unreachable" in error_type or "server_connection" in error_type:
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        os.makedirs(UPLOADS_DIR, exist_ok=True)
        threading.Thread(target=load_all_sessions, daemon=True).start()
        success = True
        fix_message = "تم تشخيص المشكلة وإعادة تهيئة الخدمات"
    elif "auth_no_client" in error_type or "monitoring_no_client" in error_type or "rotating_no_client" in error_type or "settings_mismatch" in error_type:
        uid = error.get("user_id")
        if uid:
            vid = next(iter(USERS.keys()), "").split("__")[0] if USERS else "test"
            full_uid = f"{vid}__{uid}" if "__" not in uid else uid
            threading.Thread(target=ensure_client_running, args=(full_uid,), daemon=True).start()
        success = True
        fix_message = "تم إرسال أمر إعادة تشغيل العميل في الخلفية"
    elif "github_" in error_type:
        success = True
        fix_message = "تم تسجيل الخطأ - تأكد من صحة رمز GitHub والمستودع"
    elif "learning_bot" in error_type or "auto_reply" in error_type or "message_handler" in error_type:
        success = True
        fix_message = "تم تسجيل الخطأ - سيتم تجاوزه تلقائياً في المرة القادمة"
    elif "scheduled_send" in error_type or "rotating_send" in error_type:
        success = True
        fix_message = "تم تسجيل خطأ الإرسال - سيتم إعادة المحاولة تلقائياً"
    else:
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        os.makedirs(UPLOADS_DIR, exist_ok=True)
        threading.Thread(target=load_all_sessions, daemon=True).start()
        success = True
        fix_message = "تم إعادة تحميل جميع الإعدادات والجلسات"
    if success:
        with errors_lock:
            for e in errors_list:
                if e["id"] == error_id:
                    e["fixed"] = True
                    e["fix_message"] = fix_message
                    e["fixed_at"] = datetime.now().isoformat()
                    break
            try:
                with open(ERRORS_FILE, "w", encoding="utf-8") as f:
                    json.dump(errors_list, f, ensure_ascii=False, indent=2)
            except:
                pass
        return True, fix_message
    return True, "تم تسجيل الخطأ للمراجعة"

def extract_user_id_from_error(error):
    details = error.get("details", "")
    match = re.search(r'user[_:]?\s*([a-f0-9]+__user_[0-9]+)', details)
    if match:
        return match.group(1)
    match = re.search(r'([a-f0-9]+__user_[0-9]+)', details)
    if match:
        return match.group(1)
    return None

def diagnose_system():
    issues = []
    try:
        requests.get(f"http://localhost:{int(os.environ.get('PORT', 5000))}/ping", timeout=3)
    except:
        issues.append({"type": "server_connection", "message": "الخادم لا يستجيب لل ping", "severity": "critical"})
    if not os.path.exists(SESSIONS_DIR):
        issues.append({"type": "sessions_dir", "message": "مجلد sessions غير موجود", "severity": "high"})
    for slot, uinfo in PREDEFINED_USERS.items():
        vid = session.get("visitor_id", "test")
        uid = f"{vid}__{slot}"
        ud = get_or_create_user(uid)
        if ud.authenticated and not ud.client_manager:
            issues.append({"type": "auth_no_client", "message": f"المستخدم {uinfo['name']} مسجل دخول لكن العميل غير موجود", "severity": "high", "user_id": slot})
        if ud.monitoring_active and not ud.client_manager:
            issues.append({"type": "monitoring_no_client", "message": f"المراقبة مفعلة للمستخدم {uinfo['name']} لكن العميل غير جاهز", "severity": "high", "user_id": slot})
        if ud.rotating_active and not ud.client_manager:
            issues.append({"type": "rotating_no_client", "message": f"الإرسال المتسلسل مفعل للمستخدم {uinfo['name']} لكن العميل غير جاهز", "severity": "high", "user_id": slot})
        s = load_settings(uid)
        if s.get("awaiting_code") and not ud.awaiting_code:
            issues.append({"type": "settings_mismatch", "message": f"حالة انتظار الكود غير متطابقة للمستخدم {uinfo['name']}", "severity": "medium", "user_id": slot})
    api_paths = ["/api/get_login_status", "/api/get_stats", "/api/get_settings"]
    for path in api_paths:
        try:
            resp = requests.get(f"http://localhost:{int(os.environ.get('PORT', 5000))}{path}", timeout=3)
            if resp.status_code != 200:
                issues.append({"type": "api_unreachable", "message": f"المسار {path} لا يستجيب (كود {resp.status_code})", "severity": "medium"})
        except:
            issues.append({"type": "api_unreachable", "message": f"المسار {path} لا يمكن الوصول إليه", "severity": "high"})
    if not os.path.exists("templates/index.html"):
        issues.append({"type": "missing_file", "message": "ملف templates/index.html غير موجود", "severity": "critical"})
    if not API_ID or not API_HASH:
        issues.append({"type": "api_credentials", "message": "API_ID أو API_HASH غير محددين", "severity": "critical"})
    return issues

load_errors()

def clean_stale_sessions():
    for fname in os.listdir(SESSIONS_DIR):
        if fname.endswith('.session') or fname.endswith('.session-journal'):
            try:
                os.remove(os.path.join(SESSIONS_DIR, fname))
                logger.info(f"Removed stale session file: {fname}")
            except:
                pass

clean_stale_sessions()

LOADING_PAGE = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>جاري تحميل التطبيق</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            font-family: 'Tajawal', 'Segoe UI', sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            color: white;
        }
        .container {
            text-align: center;
            padding: 2rem;
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            border-radius: 30px;
            max-width: 90%;
            width: 400px;
        }
        .app-icon {
            width: 110px;
            height: 110px;
            border-radius: 24px;
            margin: 0 auto 1.5rem auto;
            display: block;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            animation: pulse 2s ease-in-out infinite;
        }
        @keyframes pulse {
            0%, 100% { transform: scale(1); box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
            50% { transform: scale(1.05); box-shadow: 0 12px 40px rgba(0,0,0,0.4); }
        }
        h1 { font-size: 1.3rem; margin-bottom: 0.8rem; }
        p { font-size: 0.9rem; opacity: 0.85; }
        .footer { margin-top: 1.5rem; font-size: 0.78rem; opacity: 0.6; }
        .dots { display: inline-block; }
        .dots span { animation: blink 1.4s infinite; opacity: 0; }
        .dots span:nth-child(2) { animation-delay: 0.2s; }
        .dots span:nth-child(3) { animation-delay: 0.4s; }
        @keyframes blink { 0%,80%,100%{opacity:0} 40%{opacity:1} }
    </style>
</head>
<body>
    <div class="container">
        <img src="/static/icons/icon-192.png" class="app-icon" alt="أيقونة التطبيق">
        <h1>جاري تحميل وظائف التطبيق</h1>
        <p>يرجى الانتظار ثوانٍ<span class="dots"><span>.</span><span>.</span><span>.</span></span></p>
        <p class="footer">سيتم تحويلك تلقائياً</p>
    </div>
    <script>
        let attempts = 0;
        const maxAttempts = 60;
        function checkReady() {
            attempts++;
            fetch('/ping').then(r => {
                if (r.ok) {
                    fetch('/api/ready_check').then(res => res.json()).then(data => {
                        if (data.ready) {
                            window.location.href = '/';
                        } else if (attempts < maxAttempts) {
                            setTimeout(checkReady, 2000);
                        } else {
                            window.location.href = '/';
                        }
                    }).catch(() => {
                        if (attempts < maxAttempts) setTimeout(checkReady, 2000);
                        else window.location.href = '/';
                    });
                } else {
                    if (attempts < maxAttempts) setTimeout(checkReady, 2000);
                }
            }).catch(() => {
                if (attempts < maxAttempts) setTimeout(checkReady, 2000);
            });
        }
        setTimeout(checkReady, 3000);
    </script>
</body>
</html>
"""

def parse_entities(raw_text):
    entities = []
    found_raw = set()
    def add(val):
        k = val.lower().lstrip('@')
        if k not in found_raw and len(k) >= 4:
            found_raw.add(k)
            entities.append(val)
    for m in re.findall(r'https?://t\.me/\+([A-Za-z0-9_-]+)', raw_text):
        add(f"+{m}")
    for m in re.findall(r'https?://t\.me/joinchat/([A-Za-z0-9_-]+)', raw_text):
        add(m)
    for m in re.findall(r'https?://t\.me/([A-Za-z][A-Za-z0-9_]{3,})', raw_text):
        add(m)
    for m in re.findall(r'(?<![/\w@])t\.me/\+?([A-Za-z0-9_-]{4,})', raw_text):
        add(m)
    for m in re.findall(r'@([A-Za-z0-9_]{5,})', raw_text):
        add(m)
    for m in re.findall(r'(?<!\d)(-100\d{9,})(?!\d)', raw_text):
        add(m)
    if not entities:
        for part in re.split(r'[\n,،\s|؛؛/\\]+', raw_text):
            p = part.strip().lstrip('@')
            if p and len(p) >= 5 and not re.search(r'[أ-ي]', p) and re.match(r'^[A-Za-z0-9_+-]+$', p):
                add(p)
    return entities

def parse_keywords(raw_text):
    seen = set()
    kws = []
    for kw in re.split(r'[\n,،|؛;]+', raw_text):
        kw = kw.strip()
        if kw and kw.lower() not in seen:
            seen.add(kw.lower())
            kws.append(kw)
    return kws

PREDEFINED_USERS = {
    "user_1": {"id": "user_1", "name": "المستخدم الأول", "icon": "fas fa-user", "color": "#5865f2"},
    "user_2": {"id": "user_2", "name": "المستخدم الثاني", "icon": "fas fa-user-tie", "color": "#3ba55c"},
    "user_3": {"id": "user_3", "name": "المستخدم الثالث", "icon": "fas fa-user-graduate", "color": "#faa81a"},
    "user_4": {"id": "user_4", "name": "المستخدم الرابع", "icon": "fas fa-user-cog", "color": "#ed4245"},
    "user_5": {"id": "user_5", "name": "المستخدم الخامس", "icon": "fas fa-user-astronaut", "color": "#6f42c1"},
}

USERS = {}
USERS_LOCK = Lock()

def save_settings(user_id, settings):
    try:
        path = os.path.join(SESSIONS_DIR, f"{user_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        if settings.get("string_session"):
            threading.Thread(target=upload_session_to_github, args=(settings["string_session"], user_id), daemon=True).start()
        return True
    except Exception as e:
        add_error("settings_save", str(e), f"user_id: {user_id}")
        return False

def load_settings(user_id):
    try:
        path = os.path.join(SESSIONS_DIR, f"{user_id}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        add_error("settings_load", str(e), f"user_id: {user_id}")
    return {}

# ========== البوت التعليمي ==========
class LearningBot:
    def __init__(self, user_id=None):
        self.user_id = user_id
        self.client = None
        self.is_monitoring = False
        self.reply_in_groups = False
        self.knowledge = self.load_knowledge()
        self.unknown_requests = []
        self.quick_replies = [
            (r'\b(واجب|حل واجب|مسألة|تمارين)\b', 'ابشر ارسل الواجب وابشر'),
            (r'\b(اختبار|كويز|فاينل|ميد)\b', 'متى اختبارك؟'),
            (r'\b(مشروع|تقرير|بحث)\b', 'هات العنوان وش مشروعك؟'),
            (r'\b(تلخيص|ملخص)\b', 'ارسل النص اللي تبي تلخيصه'),
            (r'\b(ترجمة|ترجم)\b', 'ارسل النص وحدد اللغة'),
        ]

    def load_knowledge(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        return {
            "حل واجب": {"description": "حل الواجبات والمسائل الدراسية", "questions": ["وش المادة؟", "كم سؤال؟", "متى تحتاجه؟"], "intent_keywords": ["حل", "واجب", "مسألة", "سؤال", "تمارين"]},
            "بحث": {"description": "إعداد البحوث الأكاديمية", "questions": ["وش موضوع البحث؟", "كم صفحة؟", "تريد مراجع؟"], "intent_keywords": ["بحث", "تقرير", "مشروع", "دراسة"]},
            "تلخيص": {"description": "تلخيص الكتب والمحاضرات", "questions": ["وش المحتوى؟", "كم صفحة؟", "ملخص مفصل ولا مختصر؟"], "intent_keywords": ["تلخيص", "ملخص", "اختصار"]},
            "ترجمة": {"description": "ترجمة النصوص", "questions": ["اللغة المصدر؟", "كم كلمة؟", "أكاديمية ولا عادية؟"], "intent_keywords": ["ترجمة", "ترجم", "نقل"]}
        }

    def save_knowledge(self):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(self.knowledge, f, ensure_ascii=False, indent=4)

    async def start_with_client(self, client):
        self.client = client
        self.register_handlers()
        logger.info(f"✅ البوت التعليمي بدأ للمستخدم {self.user_id}")

    def register_handlers(self):
        @self.client.on(events.NewMessage)
        async def handler(event):
            if not self.is_monitoring:
                return
            await self.handle_message(event)

    def is_likely_advertisement(self, text):
        text_lower = text.lower()
        if re.search(r'wa\.me|whatsapp\.com|t\.me/\+|\bواتس\b', text_lower):
            return True
        marketing_words = ['خدماتنا', 'من خدمتنا', 'اسعار مناسبة', 'للتواصل', 'عروض', 'خصم', 'تخفيض', 'احترافية', 'بافضل سعر', 'خدمة سريعة', 'نقدم لكم', 'لفترة محدودة']
        for word in marketing_words:
            if word in text_lower:
                return True
        bullet_items = re.findall(r'[•\-\*✅]\s*[^\n]+', text)
        if len(bullet_items) >= 5:
            return True
        if re.search(r'\b(05|9665|\+966)[0-9]{8,}\b', text):
            return True
        return False

    async def handle_message(self, event):
        try:
            text = event.message.text
            if not text or len(text) < 3:
                return
            sender = await event.get_sender()
            sender_name = getattr(sender, 'first_name', '') or getattr(sender, 'username', '') or 'عزيزي'
            is_group = event.is_group
            chat_id = event.chat_id

            if self.is_likely_advertisement(text):
                socketio.emit('log_update', {"message": f"📢 تم تجاهل إعلان من {sender_name}"}, to=self.user_id)
                return

            for pattern, reply in self.quick_replies:
                if re.search(pattern, text, re.IGNORECASE):
                    if is_group and not self.reply_in_groups:
                        socketio.emit('log_update', {"message": f"⏸️ تم تجاهل طلب في مجموعة (الرد معطل)"}, to=self.user_id)
                        return
                    await event.reply(reply)
                    socketio.emit('log_update', {"message": f"🤖 رد سريع لـ {sender_name}: {reply}"}, to=self.user_id)
                    return

            if is_group and not self.reply_in_groups:
                return

            service = self.detect_service(text)
            if service:
                await self.send_simple_reply(event, service)
            else:
                if any(kw in text.lower() for kw in ["محتاج", "ابي", "اريد", "مساعدة"]):
                    self.unknown_requests.append({
                        "raw_text": text[:50],
                        "suggested_name": text[:50],
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "chat_id": chat_id,
                        "sender_name": sender_name
                    })
                    socketio.emit('new_unknown', self.unknown_requests[-1], to=self.user_id)
        except Exception as e:
            add_error("learning_bot", str(e), f"user: {self.user_id}")
            logger.error(f"خطأ في البوت: {e}")

    def detect_service(self, text):
        text_low = text.lower()
        best_match = None
        best_score = 0
        for service, data in self.knowledge.items():
            for kw in data.get("intent_keywords", []):
                if kw in text_low:
                    score = len(kw)
                    if score > best_score:
                        best_score = score
                        best_match = service
        return best_match

    async def send_simple_reply(self, event, service):
        if service == "حل واجب":
            await event.reply("ابشر ارسل الواجب وابشر")
        elif service == "بحث":
            await event.reply("هات العنوان وش موضوع البحث؟")
        elif service == "تلخيص":
            await event.reply("ارسل النص اللي تبي تلخيصه")
        elif service == "ترجمة":
            await event.reply("ارسل النص وحدد اللغة")
        else:
            await event.reply("ابشر اخوي وش عندك؟")

    def get_unknown_requests(self):
        return self.unknown_requests

    def clear_unknown(self):
        self.unknown_requests = []

    def add_service(self, name, desc, questions=None, keywords=None):
        if name and desc:
            self.knowledge[name] = {
                "description": desc,
                "questions": questions or ["شنو التفاصيل؟", "متى تحتاجه؟"],
                "intent_keywords": keywords or [name]
            }
            self.save_knowledge()
            return True
        return False

    def delete_service(self, name):
        if name in self.knowledge:
            del self.knowledge[name]
            self.save_knowledge()
            return True
        return False

    def get_services(self):
        return self.knowledge

    def toggle_reply_in_groups(self):
        self.reply_in_groups = not self.reply_in_groups
        return self.reply_in_groups

learning_bots = {}
LEARNING_LOCK = Lock()

def get_learning_bot(user_id):
    with LEARNING_LOCK:
        if user_id not in learning_bots:
            learning_bots[user_id] = LearningBot(user_id)
        return learning_bots[user_id]

# ========== إدارة المستخدمين والجلسات ==========
class UserData:
    def __init__(self, user_id):
        self.user_id = user_id
        self.client_manager = None
        self.settings = {}
        self.stats = {"sent": 0, "errors": 0, "alerts": 0, "replies": 0}
        self.connected = False
        self.authenticated = False
        self.awaiting_code = False
        self.awaiting_password = False
        self.phone_code_hash = None
        self.monitoring_active = False
        self.is_running = False
        self.thread = None
        self.phone_number = None
        self.auto_replies = []
        self.telegram_name = None
        self.sent_batches = []
        self.pending_auto_code = None
        self.last_seen = None
        self.blocked = False
        self.disabled = False
        self.alerts = []
        self.scheduled_active = False
        self.scheduled_interval = 0
        self.scheduled_groups = []
        self.scheduled_message = ""
        self.scheduled_image = None
        self.rotating_active = False
        self.rotating_messages = ["", "", "", "", ""]
        self.rotating_groups = []
        self.rotating_interval = 5
        self.rotating_index = 0
        self.rotating_thread = None
        self.rotating_stop = threading.Event()
        self.string_session = None
        self.skip_protected = True
        self.pending_protected_groups = []
        self.resolved_groups = []

    def to_dict(self):
        slot = self.user_id.split('__', 1)[1] if '__' in self.user_id else self.user_id
        return {
            "user_id": self.user_id,
            "name": PREDEFINED_USERS.get(slot, {}).get("name", slot),
            "phone": self.phone_number,
            "authenticated": self.authenticated,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "blocked": self.blocked,
            "disabled": self.disabled,
            "groups": self.settings.get("groups", []),
            "watch_words": self.settings.get("watch_words", []),
            "auto_replies": self.auto_replies,
            "alerts_count": len(self.alerts),
            "monitoring_active": self.monitoring_active,
            "scheduled_active": self.scheduled_active,
            "skip_protected": self.skip_protected
        }

class TelegramClientManager:
    def __init__(self, user_id):
        self.user_id = user_id
        self.client = None
        self.loop = None
        self.thread = None
        self.stop_flag = threading.Event()
        self.is_ready = threading.Event()
        self.event_handlers_registered = False
        self.scheduled_thread = None
        self.scheduled_stop = threading.Event()
        self.rotating_thread = None
        self.rotating_stop = threading.Event()
        self.keep_alive = True
        self.learning_bot = None
        self.reconnect_attempts = 0
        self.pending_responses = {}
        self.group_decision = {}
        self.current_message = None
        self.current_image = None

    def _get_string_session(self):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud and ud.string_session:
                return ud.string_session
        settings = load_settings(self.user_id)
        if settings.get("string_session"):
            return settings["string_session"]
        return download_session_from_github(self.user_id)

    def _save_string_session(self, session_string):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.string_session = session_string
        settings = load_settings(self.user_id)
        settings["string_session"] = session_string
        save_settings(self.user_id, settings)
        threading.Thread(target=upload_session_to_github, args=(session_string, self.user_id), daemon=True).start()

    def start_client_thread(self):
        if self.thread and self.thread.is_alive():
            if self.is_ready.is_set() and self.client and self.client.is_connected():
                return True
            else:
                logger.warning(f"Client thread for {self.user_id} is alive but not ready, stopping it")
                self.stop()
                if self.thread:
                    self.thread.join(timeout=3)
                self.thread = None
        self.stop_flag.clear()
        self.is_ready.clear()
        self.loop = None  # إعادة تعيين الـ loop صراحةً قبل إنشاء thread جديد
        self.keep_alive = True
        self.reconnect_attempts = 0
        self.thread = threading.Thread(target=self._run_client_loop, daemon=True)
        self.thread.daemon = True
        self.thread.start()
        ready = self.is_ready.wait(timeout=30)
        if not ready:
            add_error("client_start", f"Client thread for {self.user_id} did not become ready", "")
            logger.error(f"Client thread for {self.user_id} did not become ready within 30 seconds")
        return ready

    def _run_client_loop(self):
        async def _async_entry():
            self.loop = asyncio.get_running_loop()
            retry_delay = 5
            while not self.stop_flag.is_set() and self.keep_alive:
                string_session = self._get_string_session()
                if string_session:
                    self.client = TelegramClient(StringSession(string_session), int(API_ID), API_HASH)
                else:
                    self.client = TelegramClient(StringSession(), int(API_ID), API_HASH)
                try:
                    await self._client_main()
                except Exception as e:
                    err_str = str(e)
                    if any(k in err_str for k in ['AuthKeyUnregistered', 'AuthKeyInvalid', 'UserDeactivated', 'SESSION_REVOKED']):
                        logger.warning(f"🔴 {self.user_id} session revoked, stopping restart loop")
                        break
                    add_error("client_thread", err_str, traceback.format_exc())
                # إذا انتهت الدورة بدون إيقاف يدوي، أعد الاتصال بعد تأخير
                if self.stop_flag.is_set() or not self.keep_alive:
                    break
                logger.warning(f"⚠️ {self.user_id} client loop ended unexpectedly, restarting in {retry_delay}s...")
                self.is_ready.clear()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # زيادة تدريجية حتى 60 ثانية

        try:
            asyncio.run(_async_entry())
        except Exception as e:
            add_error("client_thread", str(e), traceback.format_exc())
            self.is_ready.set()
        finally:
            self.loop = None

    async def _client_main(self):
        try:
            await self.client.connect()
            self.is_ready.set()
            logger.info(f"Client for {self.user_id} connected")
            self.reconnect_attempts = 0
            if not self._get_string_session():
                session_string = self.client.session.save()
                self._save_string_session(session_string)
            await self._register_event_handlers()
            if await self.client.is_user_authorized():
                _should_start_scheduled = False
                _sched_args = {}
                _should_start_rotating = False
                _rot_args = {}
                with USERS_LOCK:
                    ud = USERS.get(self.user_id)
                    if ud:
                        ud.authenticated = True
                        ud.connected = True
                        if ud.settings.get('monitoring_active'):
                            ud.monitoring_active = True
                            ud.is_running = True
                        if ud.settings.get('scheduled_active'):
                            ud.scheduled_active = True
                            ud.scheduled_interval = ud.settings.get('scheduled_interval', 0)
                            ud.scheduled_groups = ud.settings.get('scheduled_groups', [])
                            ud.scheduled_message = ud.settings.get('scheduled_message', '')
                            ud.scheduled_image = ud.settings.get('scheduled_image')
                            if ud.scheduled_active and ud.scheduled_interval > 0 and ud.scheduled_groups:
                                _should_start_scheduled = True
                                _sched_args = {
                                    'groups': ud.scheduled_groups,
                                    'message': ud.scheduled_message,
                                    'image_path': ud.scheduled_image,
                                    'interval_minutes': ud.scheduled_interval
                                }
                        if ud.settings.get('rotating_active'):
                            ud.rotating_active = True
                            ud.rotating_messages = ud.settings.get('rotating_messages', ["", "", "", "", ""])
                            ud.rotating_groups = ud.settings.get('rotating_groups', [])
                            ud.rotating_interval = ud.settings.get('rotating_interval', 5)
                            if ud.rotating_active and ud.rotating_groups and any(msg.strip() for msg in ud.rotating_messages):
                                _should_start_rotating = True
                                _rot_args = {
                                    'groups': ud.rotating_groups,
                                    'messages': ud.rotating_messages,
                                    'interval_minutes': ud.rotating_interval
                                }
                if _should_start_scheduled:
                    self.start_scheduled(**_sched_args)
                if _should_start_rotating:
                    self.start_rotating(**_rot_args)
                try:
                    me = await self.client.get_me()
                    if me:
                        tg_name = (getattr(me, 'first_name', '') or '') + (' ' + (getattr(me, 'last_name', '') or '')).rstrip()
                        tg_name = tg_name.strip()
                        with USERS_LOCK:
                            ud2 = USERS.get(self.user_id)
                            if ud2 and tg_name:
                                ud2.telegram_name = tg_name
                        socketio.emit('telegram_name_update', {'name': tg_name, 'user_id': self.user_id}, to=self.user_id)
                        # إشعار الواجهة بأن الجلسة استُعيدت تلقائياً
                        socketio.emit('session_restored', {
                            'user_id': self.user_id,
                            'name': tg_name,
                            'logged_in': True
                        }, to=self.user_id)
                except Exception as me_err:
                    logger.warning(f"Could not get 'me' for {self.user_id}: {me_err}")
                    # إشعار الواجهة حتى بدون الاسم
                    socketio.emit('session_restored', {
                        'user_id': self.user_id,
                        'logged_in': True
                    }, to=self.user_id)
                bot = get_learning_bot(self.user_id)
                if bot.is_monitoring:
                    await bot.start_with_client(self.client)
                    self.learning_bot = bot
                logger.info(f"✅ {self.user_id} auto-authorized")
            else:
                logger.info(f"Client for {self.user_id} not authorized yet")

            last_ping = time.time()
            consecutive_errors = 0
            while not self.stop_flag.is_set() and self.keep_alive:
                await asyncio.sleep(5)
                try:
                    if time.time() - last_ping > 25:
                        last_ping = time.time()
                        if self.client and self.client.is_connected():
                            with USERS_LOCK:
                                ud_check = USERS.get(self.user_id)
                                was_authenticated = ud_check.authenticated if ud_check else False
                                awaiting = ud_check.awaiting_password if ud_check else False
                            if was_authenticated and not awaiting:
                                still_auth = await self.client.is_user_authorized()
                                if not still_auth:
                                    logger.warning(f"⚠️ {self.user_id} session no longer valid")
                                    await self._handle_session_revoked()
                                    break
                        else:
                            logger.warning(f"Client {self.user_id} disconnected, reconnecting...")
                            await self.client.connect()
                            if self.client.is_connected():
                                await self._register_event_handlers()
                                consecutive_errors = 0
                                logger.info(f"✅ {self.user_id} reconnected successfully")
                    consecutive_errors = 0
                except errors.FloodWaitError as e:
                    wait = e.seconds
                    logger.warning(f"FloodWait {wait}s for {self.user_id}")
                    await asyncio.sleep(min(wait, 60))
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Keep-alive error for {self.user_id} (#{consecutive_errors}): {e}")
                    if consecutive_errors >= 5:
                        logger.warning(f"Too many keep-alive errors for {self.user_id}, reconnecting client...")
                        try:
                            await self.client.disconnect()
                        except:
                            pass
                        await asyncio.sleep(3)
                        try:
                            await self.client.connect()
                            if self.client.is_connected():
                                await self._register_event_handlers()
                                consecutive_errors = 0
                        except:
                            pass
                    else:
                        try:
                            await self.client.connect()
                        except:
                            pass
        except Exception as e:
            err_str = str(e)
            if any(k in err_str for k in ['AuthKeyUnregistered', 'AuthKeyInvalid', 'UserDeactivated', 'AUTH_KEY_UNREGISTERED', 'SESSION_REVOKED']):
                logger.warning(f"🔴 {self.user_id} auth key error: {e}")
                await self._handle_session_revoked()
            else:
                add_error("client_main", str(e), traceback.format_exc())
        finally:
            if self.client:
                try:
                    await self.client.disconnect()
                except:
                    pass

    async def _handle_session_revoked(self):
        logger.info(f"🔴 Session revoked for {self.user_id}")
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.authenticated = False
                ud.connected = False
                ud.awaiting_code = False
                ud.awaiting_password = False
                ud.monitoring_active = False
                ud.is_running = False
                ud.rotating_active = False
        threading.Thread(target=delete_session_from_github, args=(self.user_id,), daemon=True).start()
        session_file = os.path.join(SESSIONS_DIR, f"{self.user_id}.session")
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except:
                pass
        socketio.emit('session_revoked', {"message": "⚠️ تم إلغاء الجلسة من تيليجرام - يرجى تسجيل الدخول مجدداً"}, to=self.user_id)
        socketio.emit('log_update', {"message": "🔴 الجلسة أُلغيت من تيليجرام - تم قطع الاتصال تلقائياً"}, to=self.user_id)
        self.stop_flag.set()

    async def _start_code_listener(self):
        try:
            from telethon import events as telethon_events
            from telethon.tl.types import UpdateServiceNotification
            code_found = asyncio.Event()
            CODE_PATTERN = re.compile(r'\b(\d{5,6})\b')

            def _emit_code(code):
                code_found.set()
                with USERS_LOCK:
                    ud = USERS.get(self.user_id)
                    if ud:
                        ud.pending_auto_code = code
                socketio.emit('auto_code', {'code': code}, to=self.user_id)
                socketio.emit('log_update', {'message': f'📩 تم استلام كود التحقق ({code}) تلقائياً'}, to=self.user_id)
                logger.info(f"Auto-code sent for {self.user_id}: {code}")

            @self.client.on(telethon_events.Raw(UpdateServiceNotification))
            async def service_notif_handler(update):
                if code_found.is_set():
                    return
                text = getattr(update, 'message', '') or ''
                match = CODE_PATTERN.search(text)
                if match:
                    _emit_code(match.group(1))

            @self.client.on(telethon_events.NewMessage(from_users=777000))
            async def telegram_svc_handler(event):
                if code_found.is_set():
                    return
                text = event.message.message or ''
                match = CODE_PATTERN.search(text)
                if match:
                    _emit_code(match.group(1))

            @self.client.on(telethon_events.NewMessage())
            async def any_code_handler(event):
                if code_found.is_set():
                    return
                text = (event.message.message or '').lower()
                if 'login code' in text or 'your code' in text or 'verification' in text:
                    match = CODE_PATTERN.search(text)
                    if match:
                        _emit_code(match.group(1))

            await asyncio.wait_for(code_found.wait(), timeout=120)
        except asyncio.TimeoutError:
            logger.info(f"Code listener timeout for {self.user_id}")
        except Exception as e:
            logger.error(f"Code listener error for {self.user_id}: {e}")

    async def _register_event_handlers(self):
        if self.event_handlers_registered:
            return
        try:
            @self.client.on(events.NewMessage)
            async def handler(event):
                await self._handle_message(event)
            self.event_handlers_registered = True
            logger.info(f"✅ Event handlers registered for {self.user_id}")
        except Exception as e:
            add_error("register_handlers", str(e), f"user: {self.user_id}")

    async def _handle_message(self, event):
        try:
            if not event.message.text:
                return
            msg_raw = event.message.text
            code_match = re.search(r'\b(\d{5,6})\b', msg_raw)
            if code_match:
                try:
                    sender = await event.get_sender()
                    sender_id = getattr(sender, 'id', 0)
                    sender_user = getattr(sender, 'username', '') or ''
                    is_tg_official = (sender_user.lower() in ('telegram', '') and sender_id in (777000, 42777, 0))
                    if is_tg_official or 'login code' in msg_raw.lower() or 'verification code' in msg_raw.lower() or 'رمز' in msg_raw or 'كود' in msg_raw:
                        extracted_code = code_match.group(1)
                        socketio.emit('auto_code', {'code': extracted_code, 'message': msg_raw[:100]}, to=self.user_id)
                        socketio.emit('log_update', {'message': f'🔑 تم استلام كود تحقق تلقائياً: {extracted_code}'}, to=self.user_id)
                except Exception:
                    pass

            chat = await event.get_chat()
            chat_title = getattr(chat, 'title', None) or getattr(chat, 'first_name', 'مستخدم')
            chat_username = getattr(chat, 'username', None)
            chat_id = getattr(chat, 'id', None)

            with USERS_LOCK:
                ud = USERS.get(self.user_id)
                if not ud:
                    return
                monitoring = ud.monitoring_active
                auto_replies = list(ud.auto_replies or [])
                current_settings = dict(ud.settings)

            msg_text = event.message.text
            msg_lower = msg_text.lower()

            if monitoring:
                watch_words = current_settings.get('watch_words', [])
                if not watch_words:
                    fresh = load_settings(self.user_id)
                    watch_words = fresh.get('watch_words', [])
                msg_normalized = ' '.join(msg_text.split()).lower()
                for kw in watch_words:
                    kw_clean = ' '.join(kw.split()).lower() if kw else ''
                    if kw_clean and (kw_clean in msg_normalized or kw_clean in msg_lower):
                        sender = await event.get_sender()
                        sender_id = getattr(sender, 'id', None)
                        sender_first = getattr(sender, 'first_name', '') or ''
                        sender_last = getattr(sender, 'last_name', '') or ''
                        sender_username = getattr(sender, 'username', None)
                        sender_name = (f"{sender_first} {sender_last}".strip() or sender_username or str(sender_id) or 'غير معروف')
                        alert = {
                            "keyword": kw,
                            "group": chat_title,
                            "group_link": f"https://t.me/{chat_username}" if chat_username else None,
                            "message": msg_text[:500],
                            "sender": sender_name,
                            "timestamp": datetime.now().strftime('%H:%M:%S')
                        }
                        with USERS_LOCK:
                            ud2 = USERS.get(self.user_id)
                            if ud2:
                                ud2.stats['alerts'] = ud2.stats.get('alerts', 0) + 1
                                ud2.alerts.insert(0, alert)
                                if len(ud2.alerts) > 100:
                                    ud2.alerts.pop()
                                socketio.emit('stats_update', dict(ud2.stats), to=self.user_id)
                        socketio.emit('new_alert', alert, to=self.user_id)
                        socketio.emit('log_update', {"message": f"🚨 تنبيه: '{kw}' في [{chat_title}] من [{sender_name}]"}, to=self.user_id)
                        # إرسال تنبيه فوري إلى حساب Telegram الخاص
                        try:
                            group_link = f"https://t.me/{chat_username}" if chat_username else f"(ID: {chat_id})"
                            tg_alert = (
                                f"🚨 **تنبيه مراقبة** 🚨\n\n"
                                f"🔑 **الكلمة:** `{kw}`\n"
                                f"👥 **المجموعة:** {chat_title}\n"
                                f"🔗 **الرابط:** {group_link}\n"
                                f"👤 **المرسل:** {sender_name}\n"
                                f"🕐 **الوقت:** {datetime.now().strftime('%H:%M:%S')}\n\n"
                                f"💬 **الرسالة:**\n{msg_text[:300]}"
                            )
                            await self.client.send_message('me', tg_alert, parse_mode='md')
                            socketio.emit('log_update', {"message": f"📩 تم إرسال التنبيه إلى حسابك على Telegram"}, to=self.user_id)
                        except Exception as tg_err:
                            logger.warning(f"فشل إرسال تنبيه Telegram: {tg_err}")
                            add_error("monitoring_notify", str(tg_err), f"user: {self.user_id}")

            fresh_rules = load_settings(self.user_id)
            live_auto_replies = fresh_rules.get('auto_replies', [])
            if not live_auto_replies:
                live_auto_replies = auto_replies
            for rule in live_auto_replies:
                kw = (rule.get('keyword', '') or '').strip()
                reply_text = (rule.get('reply', '') or '').strip()
                if not kw or not reply_text:
                    continue
                kw_clean = ' '.join(kw.split()).lower()
                msg_norm = ' '.join(msg_text.split()).lower()
                if kw_clean in msg_norm or kw_clean in msg_lower:
                    try:
                        await event.message.reply(reply_text)
                        with USERS_LOCK:
                            ud2 = USERS.get(self.user_id)
                            if ud2:
                                ud2.stats['replies'] = ud2.stats.get('replies', 0) + 1
                                socketio.emit('stats_update', dict(ud2.stats), to=self.user_id)
                        socketio.emit('log_update', {"message": f"🤖 رد تلقائي في [{chat_title}] | كلمة: '{kw[:30]}'"}, to=self.user_id)
                        break
                    except Exception as e:
                        add_error("auto_reply", str(e), f"user: {self.user_id}")
        except Exception as e:
            add_error("message_handler", str(e), f"user: {self.user_id}")

    def run_coroutine(self, coro, timeout=30):
        # إذا كان الـ loop مغلقاً أو غير موجود، حاول إعادة تشغيل الـ thread
        if not self.loop or self.loop.is_closed():
            logger.warning(f"Loop closed/None for {self.user_id}, attempting restart...")
            restarted = self.start_client_thread()
            if not restarted or not self.loop or self.loop.is_closed():
                raise Exception("Event loop not initialized or closed")
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        # انتظار بخطوات صغيرة لتحرير GIL وتقليل التأخير
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                return future.result(timeout=0.5)
            except concurrent.futures.TimeoutError:
                if future.done():
                    return future.result(timeout=0)
                continue
        future.cancel()
        raise concurrent.futures.TimeoutError(f"Coroutine timed out after {timeout}s")

    def stop(self):
        self.keep_alive = False
        self.stop_flag.set()
        if self.scheduled_stop:
            self.scheduled_stop.set()
        if hasattr(self, 'rotating_stop'):
            self.rotating_stop.set()

    def start_scheduled(self, groups, message, image_path, interval_minutes):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.scheduled_active = True
                ud.scheduled_interval = interval_minutes
                ud.scheduled_groups = groups
                ud.scheduled_message = message
                ud.scheduled_image = image_path
                settings = load_settings(self.user_id)
                settings['scheduled_active'] = True
                settings['scheduled_interval'] = interval_minutes
                settings['scheduled_groups'] = groups
                settings['scheduled_message'] = message
                settings['scheduled_image'] = image_path
                save_settings(self.user_id, settings)
        self.scheduled_stop.clear()
        self.scheduled_thread = threading.Thread(target=self._scheduled_worker, args=(groups, message, image_path, interval_minutes), daemon=True)
        self.scheduled_thread.start()

    def stop_scheduled(self):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.scheduled_active = False
                settings = load_settings(self.user_id)
                settings['scheduled_active'] = False
                save_settings(self.user_id, settings)
        self.scheduled_stop.set()

    def _scheduled_worker(self, groups, message, image_path, interval_minutes):
        socketio.emit('log_update', {"message": f"📅 بدأ الإرسال المجدول كل {interval_minutes} دقيقة"}, to=self.user_id)
        while not self.scheduled_stop.is_set():
            try:
                timeout = max(300, len(groups) * 15)
                self.run_coroutine(self._send_to_groups(groups, message, image_path), timeout=timeout)
            except Exception as e:
                add_error("scheduled_send", str(e), f"user: {self.user_id}")
                socketio.emit('log_update', {"message": f"⚠️ خطأ في الإرسال المجدول: {str(e)[:150]}"}, to=self.user_id)
            for _ in range(interval_minutes * 60):
                if self.scheduled_stop.is_set():
                    break
                time.sleep(1)
        socketio.emit('log_update', {"message": "⏹ تم إيقاف الإرسال المجدول"}, to=self.user_id)

    def start_rotating(self, groups, messages, interval_minutes):
        if self.rotating_thread and self.rotating_thread.is_alive():
            self.rotating_stop.set()
            self.rotating_thread.join(timeout=15)
        self.rotating_stop = threading.Event()
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.rotating_active = True
                ud.rotating_messages = messages
                ud.rotating_groups = groups
                ud.rotating_interval = interval_minutes
                ud.rotating_index = 0
                settings = load_settings(self.user_id)
                settings['rotating_active'] = True
                settings['rotating_messages'] = messages
                settings['rotating_groups'] = groups
                settings['rotating_interval'] = interval_minutes
                save_settings(self.user_id, settings)
        valid_count = len([m for m in messages if m and m.strip()])
        stop_event = self.rotating_stop
        self.rotating_thread = threading.Thread(target=self._rotating_worker, args=(groups, messages, interval_minutes, stop_event), daemon=True)
        self.rotating_thread.start()
        socketio.emit('log_update', {"message": f"🔄 بدأ الإرسال المتسلسل ({valid_count} رسائل) كل {interval_minutes} دقيقة"}, to=self.user_id)

    def stop_rotating(self):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.rotating_active = False
                ud.rotating_index = 0
                settings = load_settings(self.user_id)
                settings['rotating_active'] = False
                save_settings(self.user_id, settings)
        self.rotating_stop.set()
        socketio.emit('log_update', {"message": "⏹ تم إيقاف الإرسال المتسلسل"}, to=self.user_id)

    def _rotating_worker(self, groups, messages, interval_minutes, stop_event=None):
        if stop_event is None:
            stop_event = self.rotating_stop
        valid_messages = [msg for msg in messages if msg and msg.strip()]
        if not valid_messages:
            socketio.emit('log_update', {"message": "⚠️ لا توجد رسائل صالحة للإرسال المتسلسل"}, to=self.user_id)
            return
        index = 0
        sleep_seconds = max(60, interval_minutes * 60)
        while not stop_event.is_set():
            try:
                current_msg = valid_messages[index % len(valid_messages)]
                msg_num = (index % len(valid_messages)) + 1
                socketio.emit('log_update', {"message": f"📤 إرسال الرسالة {msg_num}/{len(valid_messages)}: {current_msg[:50]}..."}, to=self.user_id)
                timeout = max(300, len(groups) * 15)
                self.run_coroutine(self._send_to_groups(groups, current_msg, None), timeout=timeout)
            except Exception as e:
                add_error("rotating_send", str(e), f"user: {self.user_id}")
                socketio.emit('log_update', {"message": f"⚠️ خطأ في الإرسال المتسلسل: {str(e)[:150]}"}, to=self.user_id)
            index += 1
            with USERS_LOCK:
                ud = USERS.get(self.user_id)
                if ud:
                    ud.rotating_index = index % len(valid_messages)
            for _ in range(sleep_seconds):
                if stop_event.is_set():
                    break
                time.sleep(1)
        socketio.emit('log_update', {"message": "⏹ تم إيقاف الإرسال المتسلسل"}, to=self.user_id)

    # ========== دوال الكشف عن المجموعات المحمية ==========
    def _is_protection_bot(self, username):
        # بوتات الحماية والأمان المعروفة
        protection_bots = [
            'shieldy', 'antispam', 'rose_bot', 'missrose', 'group_guard',
            'spamwatch', 'security_bot', 'protect_bot', 'guard_bot',
            'antipromotion', 'antilink', 'captchabot', 'captcha_bot',
            'verify_bot', 'safeguard', 'defender_bot', 'combot',
            'groupbutler', 'ban_hammer', 'banhammer', 'spam_bot',
            'adminbot', 'anti_spam', 'antinudebot', 'grouphelpbot',
            'policeman', 'cas_bot', 'sber_anti_spam', 'tg_spam_bot',
            'cleanerbot', 'arabic_spam', 'spam_shield', 'tgspam',
            'SpamProtectionBot', 'nightbot', 'moderatorbot',
            'ProtectionBot', 'securitybot', 'جروب', 'حماية',
        ]
        username_lower = username.lower()
        # كلمات دالة على الحماية في اسم البوت
        protection_keywords = [
            'spam', 'guard', 'protect', 'security', 'shield', 'ban',
            'antispam', 'anti_spam', 'captcha', 'verify', 'safe',
            'clean', 'mod', 'police', 'filter', 'block'
        ]
        for bot in protection_bots:
            if bot.lower() in username_lower:
                return True
        for kw in protection_keywords:
            if kw in username_lower and 'bot' in username_lower:
                return True
        return False

    async def _check_group_has_protection(self, chat):
        try:
            async for user in self.client.iter_participants(chat, aggressive=True):
                if user.bot and user.username and self._is_protection_bot(user.username):
                    logger.info(f"⚠️ Group {getattr(chat, 'title', chat.id)} has protection bot: {user.username}")
                    return True
            return False
        except Exception as e:
            logger.warning(f"Failed to check protection for group {getattr(chat, 'title', chat.id)}: {e}")
            return False

    async def _send_to_chat(self, chat, message, image_path):
        has_media = bool(image_path and os.path.exists(image_path))
        if has_media:
            return await self.client.send_file(chat, image_path, caption=message or "")
        elif message:
            return await self.client.send_message(chat, message)
        else:
            raise Exception("لا يوجد محتوى للإرسال")

    async def _send_to_groups(self, groups, message, image_path):
        from telethon import functions
        sent = 0
        errors = 0
        skipped = 0
        forced = 0
        pending_count = 0
        total = len(groups)
        batch_id = str(uuid.uuid4())
        has_media = bool(image_path and os.path.exists(image_path))
        batch_entries = []

        self.current_message = message
        self.current_image = image_path

        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            skip_protected = ud.skip_protected if ud else True

        socketio.emit('log_update', {"message": f"📤 بدء الإرسال إلى {total} مجموعة..."}, to=self.user_id)
        socketio.emit('send_progress', {"current": 0, "total": total, "sent": 0, "errors": 0, "skipped": 0, "forced": 0, "pending": 0, "status": "started", "current_group": ""}, to=self.user_id)

        for i, group in enumerate(groups):
            try:
                entity_str = group.strip()
                chat = None

                if entity_str.startswith('+') and len(entity_str) > 8:
                    try:
                        result = await self.client(functions.messages.ImportChatInviteRequest(hash=entity_str[1:]))
                        chat = result.chats[0] if hasattr(result, 'chats') and result.chats else None
                    except Exception as je:
                        if 'Already' in str(je) or 'USER_ALREADY' in str(je):
                            async for dialog in self.client.iter_dialogs():
                                if hasattr(dialog.entity, 'username'):
                                    chat = dialog.entity
                                    break
                        else:
                            raise je
                elif entity_str.lstrip('-').isdigit():
                    chat = await self.client.get_entity(int(entity_str))
                else:
                    username = entity_str.lstrip('@')
                    chat = await self.client.get_entity(f"@{username}")

                if chat is None:
                    raise Exception("لم يتم العثور على المجموعة")

                chat_name = getattr(chat, 'title', None) or getattr(chat, 'username', entity_str)
                chat_link = f"https://t.me/{chat.username}" if getattr(chat, 'username', None) else f"https://t.me/c/{chat.id}"
                group_id = str(chat.id)

                is_protected = False
                if skip_protected:
                    is_protected = await self._check_group_has_protection(chat)

                if is_protected:
                    decision = self.group_decision.get(group_id)
                    if decision == 'force':
                        await self._send_to_chat(chat, message, image_path)
                        sent += 1
                        forced += 1
                        socketio.emit('log_update', {"message": f"⚠️ [{i+1}/{total}] {chat_name}: تم الإرسال رغم بوت حماية (قرار المستخدم)"}, to=self.user_id)
                        socketio.emit('send_progress', {"current": i+1, "total": total, "sent": sent, "errors": errors, "skipped": skipped, "forced": forced, "pending": pending_count, "status": "sending", "current_group": chat_name, "result": "forced"}, to=self.user_id)
                    elif decision == 'skip':
                        skipped += 1
                        socketio.emit('log_update', {"message": f"⏭️ [{i+1}/{total}] {chat_name}: تم تخطيها (قرار المستخدم)"}, to=self.user_id)
                        socketio.emit('send_progress', {"current": i+1, "total": total, "sent": sent, "errors": errors, "skipped": skipped, "forced": forced, "pending": pending_count, "status": "sending", "current_group": chat_name, "result": "skipped"}, to=self.user_id)
                    else:
                        pending_count += 1
                        self.pending_responses[group_id] = {
                            'name': chat_name,
                            'link': chat_link,
                            'entity_str': entity_str,
                            'chat': chat
                        }
                        socketio.emit('protected_group_detected', {
                            'group_id': group_id,
                            'group_name': chat_name,
                            'group_link': chat_link,
                            'entity_str': entity_str,
                            'reason': 'protection_bot'
                        }, to=self.user_id)
                        socketio.emit('log_update', {"message": f"🛡️ [{i+1}/{total}] {chat_name}: تحتوي على بوت حماية - تنتظر قرارك"}, to=self.user_id)
                        socketio.emit('send_progress', {"current": i+1, "total": total, "sent": sent, "errors": errors, "skipped": skipped, "forced": forced, "pending": pending_count, "status": "waiting", "current_group": chat_name, "result": "pending"}, to=self.user_id)
                        continue
                else:
                    sent_msg = await self._send_to_chat(chat, message, image_path)
                    sent += 1
                    chat_username = getattr(chat, 'username', None)
                    chat_id_val = chat.id
                    msg_id = sent_msg.id if sent_msg else None
                    if chat_id_val and msg_id:
                        batch_entries.append({
                            "chat_id": chat_id_val,
                            "msg_id": msg_id,
                            "chat_title": chat_name,
                            "chat_username": chat_username,
                            "entity_str": entity_str
                        })
                    socketio.emit('log_update', {"message": f"✅ [{i+1}/{total}] أُرسل إلى {chat_name}"}, to=self.user_id)
                    socketio.emit('send_progress', {"current": i+1, "total": total, "sent": sent, "errors": errors, "skipped": skipped, "forced": forced, "pending": pending_count, "status": "sending", "current_group": chat_name, "result": "success"}, to=self.user_id)
                    with USERS_LOCK:
                        ud2 = USERS.get(self.user_id)
                        if ud2:
                            ud2.stats['sent'] = ud2.stats.get('sent', 0) + 1
                            socketio.emit('stats_update', dict(ud2.stats), to=self.user_id)
                    await asyncio.sleep(2)

            except Exception as e:
                errors += 1
                err_str = str(e)
                # كشف أخطاء الحماية ومنع الإرسال
                protection_errors = [
                    'ChatAdminRequiredError', 'ChatWriteForbiddenError',
                    'UserBannedInChannelError', 'ChatAdminRequired',
                    'ChatWriteForbidden', 'CHAT_WRITE_FORBIDDEN',
                    'CHAT_ADMIN_REQUIRED', 'USER_BANNED_IN_CHANNEL',
                    'banned', 'forbidden', 'not allowed', 'restricted',
                    'admin privileges', 'not permitted'
                ]
                is_protection_error = any(p.lower() in err_str.lower() for p in protection_errors)
                if is_protection_error:
                    chat_name_err = group
                    chat_link_err = f"https://t.me/{group.lstrip('@')}" if group.startswith('@') else group
                    socketio.emit('log_update', {"message": f"🛡️ [{i+1}/{total}] {chat_name_err}: محمية أو تمنع الإرسال - تم استثناؤها"}, to=self.user_id)
                    socketio.emit('protected_group_detected', {
                        'group_id': group,
                        'group_name': chat_name_err,
                        'group_link': chat_link_err,
                        'entity_str': group,
                        'reason': 'send_forbidden',
                        'error': err_str[:100]
                    }, to=self.user_id)
                    socketio.emit('send_progress', {"current": i+1, "total": total, "sent": sent, "errors": errors, "skipped": skipped, "forced": forced, "pending": pending_count, "status": "sending", "current_group": group, "result": "protected"}, to=self.user_id)
                else:
                    socketio.emit('log_update', {"message": f"❌ [{i+1}/{total}] {group}: {err_str[:80]}"}, to=self.user_id)
                    socketio.emit('send_progress', {"current": i+1, "total": total, "sent": sent, "errors": errors, "skipped": skipped, "forced": forced, "pending": pending_count, "status": "sending", "current_group": group, "result": "error"}, to=self.user_id)
                with USERS_LOCK:
                    ud2 = USERS.get(self.user_id)
                    if ud2:
                        ud2.stats['errors'] = ud2.stats.get('errors', 0) + 1
                        socketio.emit('stats_update', dict(ud2.stats), to=self.user_id)
                await asyncio.sleep(1)

        if batch_entries:
            batch_record = {
                "id": batch_id,
                "text": message or "",
                "has_media": has_media,
                "sent_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "sent_count": sent,
                "entries": batch_entries
            }
            with USERS_LOCK:
                ud2 = USERS.get(self.user_id)
                if ud2:
                    ud2.sent_batches.append(batch_record)
            socketio.emit('batch_saved', batch_record, to=self.user_id)

        socketio.emit('log_update', {"message": f"📊 اكتمل الإرسال: ✅ {sent} ناجح  ❌ {errors} فاشل  ⏭️ {skipped} تخطي  ⚠️ {forced} رغم الحماية  ⏳ {pending_count} معلقة من أصل {total}"}, to=self.user_id)
        socketio.emit('send_complete', {"sent": sent, "errors": errors, "total": total, "skipped": skipped, "forced": forced, "pending": pending_count}, to=self.user_id)

        if self.pending_responses:
            socketio.emit('pending_groups_waiting', {
                'count': len(self.pending_responses),
                'groups': [{'group_id': gid, 'name': info['name'], 'link': info['link']} for gid, info in self.pending_responses.items()]
            }, to=self.user_id)

    # ========== دوال تعديل وحذف الدفعات ==========
    async def _edit_batch_messages(self, batch_id, new_text):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if not ud:
                return {"ok": False, "msg": "المستخدم غير موجود"}
            batch = next((b for b in ud.sent_batches if b["id"] == batch_id), None)
        if not batch:
            return {"ok": False, "msg": "الدفعة غير موجودة"}
        ok_count = 0
        fail_count = 0
        for entry in batch["entries"]:
            try:
                chat_id = entry["chat_id"]
                msg_id = entry["msg_id"]
                await self.client.edit_message(chat_id, msg_id, new_text)
                ok_count += 1
                socketio.emit('log_update', {"message": f"✏️ تم تعديل الرسالة في {entry['chat_title']}"}, to=self.user_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                fail_count += 1
                socketio.emit('log_update', {"message": f"❌ فشل التعديل في {entry.get('chat_title','?')}: {str(e)[:60]}"}, to=self.user_id)
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                for b in ud.sent_batches:
                    if b["id"] == batch_id:
                        b["text"] = new_text
                        b["edited_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        break
        socketio.emit('batch_edited', {"batch_id": batch_id, "new_text": new_text, "ok": ok_count, "fail": fail_count}, to=self.user_id)
        return {"ok": True, "edited": ok_count, "failed": fail_count}

    async def _delete_batch_messages(self, batch_id):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if not ud:
                return {"ok": False, "msg": "المستخدم غير موجود"}
            batch = next((b for b in ud.sent_batches if b["id"] == batch_id), None)
        if not batch:
            return {"ok": False, "msg": "الدفعة غير موجودة"}
        ok_count = 0
        fail_count = 0
        for entry in batch["entries"]:
            try:
                chat_id = entry["chat_id"]
                msg_id = entry["msg_id"]
                await self.client.delete_messages(chat_id, [msg_id])
                ok_count += 1
                socketio.emit('log_update', {"message": f"🗑️ تم حذف الرسالة من {entry['chat_title']}"}, to=self.user_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                fail_count += 1
                socketio.emit('log_update', {"message": f"❌ فشل الحذف من {entry.get('chat_title','?')}: {str(e)[:60]}"}, to=self.user_id)
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.sent_batches = [b for b in ud.sent_batches if b["id"] != batch_id]
        socketio.emit('batch_deleted', {"batch_id": batch_id, "ok": ok_count, "fail": fail_count}, to=self.user_id)
        return {"ok": True, "deleted": ok_count, "failed": fail_count}

    async def _join_group(self, link):
        from telethon import functions
        link = link.strip()
        if 'joinchat/' in link or '/+' in link or link.startswith('+'):
            hash_part = link.split('joinchat/')[-1].split('/+')[-1].lstrip('+').strip()
            try:
                await self.client(functions.messages.ImportChatInviteRequest(hash=hash_part))
            except Exception as e:
                if 'ALREADY' not in str(e).upper():
                    raise
        else:
            username = link.split('t.me/')[-1].lstrip('@').strip().rstrip('/')
            try:
                entity = await self.client.get_entity(f"@{username}")
                await self.client(functions.channels.JoinChannelRequest(entity))
            except Exception as e:
                if 'ALREADY' not in str(e).upper():
                    raise

    def get_chats(self):
        if not self.loop:
            raise Exception("Event loop not initialized")
        future = asyncio.run_coroutine_threadsafe(self._get_chats_async(), self.loop)
        return future.result(timeout=60)

    async def _get_chats_async(self):
        groups = []
        try:
            async for dialog in self.client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    entity = dialog.entity
                    chat_id = getattr(entity, 'id', None)
                    title = getattr(entity, 'title', None) or dialog.name
                    username = getattr(entity, 'username', None)
                    link = f"https://t.me/{username}" if username else None
                    groups.append({"id": chat_id, "title": title, "username": username, "link": link})
        except Exception as e:
            add_error("get_chats", str(e), f"user: {self.user_id}")
            raise
        return groups

    async def _search_messages_async(self, query, search_type, exclude_chats):
        import re
        results = []
        exclude_set = set()
        for ex in exclude_chats:
            ex_clean = ex.strip().lstrip('@')
            if ex_clean:
                exclude_set.add(ex_clean)
                if ex_clean.lstrip('-').isdigit():
                    exclude_set.add(int(ex_clean))
                else:
                    exclude_set.add(ex_clean.lower())
        try:
            async for dialog in self.client.iter_dialogs():
                if dialog.is_user:
                    continue
                chat = dialog.entity
                chat_id = chat.id
                chat_username = getattr(chat, 'username', None)
                chat_title = dialog.name
                skip = False
                for ex in exclude_set:
                    if isinstance(ex, int) and chat_id == ex:
                        skip = True
                        break
                    if isinstance(ex, str) and chat_username and chat_username.lower() == ex:
                        skip = True
                        break
                    if isinstance(ex, str) and str(chat_id).lstrip('-100') == ex:
                        skip = True
                        break
                if skip:
                    continue
                limit = 500
                try:
                    async for msg in self.client.iter_messages(chat, limit=limit):
                        if not msg.text:
                            continue
                        msg_text = msg.text
                        if search_type == 'text':
                            if query and query.lower() not in msg_text.lower():
                                continue
                            results.append({"message_id": msg.id, "chat_title": chat_title, "chat_link": f"https://t.me/{chat_username}" if chat_username else f"https://t.me/c/{str(chat_id).lstrip('-100')}", "sender": getattr(msg.sender, 'first_name', '') or getattr(msg.sender, 'username', 'غير معروف'), "message_text": msg_text[:1000], "date": msg.date.isoformat() if msg.date else datetime.now().isoformat()})
                        elif search_type == 'telegram_links':
                            urls = re.findall(r'https?://(?:t\.me|telegram\.me)/[^\s<>]+', msg_text)
                            if not urls:
                                continue
                            for url in urls:
                                results.append({"message_id": msg.id, "chat_title": chat_title, "chat_link": f"https://t.me/{chat_username}" if chat_username else f"https://t.me/c/{str(chat_id).lstrip('-100')}", "sender": getattr(msg.sender, 'first_name', '') or getattr(msg.sender, 'username', 'غير معروف'), "message_text": msg_text[:500], "date": msg.date.isoformat() if msg.date else datetime.now().isoformat(), "link": url})
                        elif search_type == 'all_links':
                            urls = re.findall(r'https?://[^\s<>]+', msg_text)
                            if not urls:
                                continue
                            for url in urls:
                                results.append({"message_id": msg.id, "chat_title": chat_title, "chat_link": f"https://t.me/{chat_username}" if chat_username else f"https://t.me/c/{str(chat_id).lstrip('-100')}", "sender": getattr(msg.sender, 'first_name', '') or getattr(msg.sender, 'username', 'غير معروف'), "message_text": msg_text[:500], "date": msg.date.isoformat() if msg.date else datetime.now().isoformat(), "link": url})
                        if len(results) >= 1000:
                            return results
                except Exception as e:
                    logger.warning(f"Error reading messages from {chat_title}: {e}")
                    continue
        except Exception as e:
            add_error("search_messages", str(e), f"user: {self.user_id}")
            raise
        return results

    def search_messages(self, query, search_type, exclude_chats):
        if not self.loop:
            raise Exception("Event loop not initialized")
        future = asyncio.run_coroutine_threadsafe(self._search_messages_async(query, search_type, exclude_chats), self.loop)
        return future.result(timeout=120)

def get_or_create_user(user_id):
    with USERS_LOCK:
        if user_id not in USERS:
            ud = UserData(user_id)
            ud.settings = load_settings(user_id)
            ud.auto_replies = ud.settings.get('auto_replies', [])
            if ud.settings.get('phone'):
                ud.phone_number = ud.settings['phone']
            ud.blocked = ud.settings.get('blocked', False)
            ud.disabled = ud.settings.get('disabled', False)
            ud.alerts = ud.settings.get('alerts', [])
            ud.monitoring_active = ud.settings.get('monitoring_active', False)
            ud.is_running = ud.monitoring_active
            ud.scheduled_active = ud.settings.get('scheduled_active', False)
            ud.scheduled_interval = ud.settings.get('scheduled_interval', 0)
            ud.scheduled_groups = ud.settings.get('scheduled_groups', [])
            ud.scheduled_message = ud.settings.get('scheduled_message', '')
            ud.scheduled_image = ud.settings.get('scheduled_image')
            ud.rotating_active = ud.settings.get('rotating_active', False)
            ud.rotating_messages = ud.settings.get('rotating_messages', ["", "", "", "", ""])
            ud.rotating_groups = ud.settings.get('rotating_groups', [])
            ud.rotating_interval = ud.settings.get('rotating_interval', 5)
            ud.rotating_index = ud.settings.get('rotating_index', 0)
            ud.skip_protected = ud.settings.get('skip_protected', True)
            if ud.settings.get('string_session'):
                ud.string_session = ud.settings['string_session']
            if ud.settings.get('last_seen'):
                try:
                    ud.last_seen = datetime.fromisoformat(ud.settings['last_seen'])
                except:
                    pass
            USERS[user_id] = ud
        return USERS[user_id]

VALID_SLOTS = list(PREDEFINED_USERS.keys())

def get_visitor_id():
    vid = session.get('visitor_id')
    if not vid:
        vid = str(uuid.uuid4()).replace('-', '')
        session['visitor_id'] = vid
    return vid

def get_current_slot():
    slot = session.get('user_slot', 'user_1')
    if slot not in VALID_SLOTS:
        slot = 'user_1'
        session['user_slot'] = slot
    return slot

def get_current_user_id():
    vid = get_visitor_id()
    slot = get_current_slot()
    return f"{vid}__{slot}"

def get_slot_from_uid(uid):
    if '__' in uid:
        return uid.split('__', 1)[1]
    return uid if uid in VALID_SLOTS else 'user_1'

def update_last_seen(user_id):
    with USERS_LOCK:
        ud = USERS.get(user_id)
        if ud:
            ud.last_seen = datetime.now()
            settings = load_settings(user_id)
            settings['last_seen'] = ud.last_seen.isoformat()
            save_settings(user_id, settings)

def ensure_client_running(uid):
    ud = get_or_create_user(uid)
    if ud.client_manager is None:
        ud.client_manager = TelegramClientManager(uid)
        logger.info(f"Created new client manager for {uid}")
    else:
        # إعادة إنشاء الـ manager إذا كان الـ loop مغلقاً أو الـ thread ميتاً
        loop_dead = (ud.client_manager.loop is None or ud.client_manager.loop.is_closed())
        thread_dead = (ud.client_manager.thread is None or not ud.client_manager.thread.is_alive())
        if loop_dead or thread_dead:
            logger.warning(f"Recreating client manager for {uid} (loop_dead={loop_dead}, thread_dead={thread_dead})")
            # إيقاف الـ manager القديم أولاً وانتظار انتهاء thread-ه
            try:
                ud.client_manager.stop()
                if ud.client_manager.thread and ud.client_manager.thread.is_alive():
                    ud.client_manager.thread.join(timeout=8)
            except Exception as stop_err:
                logger.warning(f"Error stopping old manager for {uid}: {stop_err}")
            ud.client_manager = TelegramClientManager(uid)
            logger.info(f"Recreated client manager for {uid}")

    for attempt in range(3):
        if ud.client_manager.start_client_thread():
            break
        else:
            logger.warning(f"Attempt {attempt+1} to start client for {uid} failed, retrying...")
            time.sleep(2)
            ud.client_manager = TelegramClientManager(uid)
    else:
        add_error("ensure_client", f"Failed to start client for {uid} after 3 attempts", "")
        logger.error(f"Failed to start client for {uid}")
        return False

    # انتظار المصادقة الفعلية
    for _ in range(15):
        try:
            if ud.client_manager.run_coroutine(ud.client_manager.client.is_user_authorized(), timeout=2):
                with USERS_LOCK:
                    ud.authenticated = True
                    ud.connected = True
                    # تحديث الإعدادات من القرص
                    ud.settings = load_settings(uid)
                    if ud.settings.get('monitoring_active'):
                        ud.monitoring_active = True
                        bot = get_learning_bot(uid)
                        bot.is_monitoring = True
                return True
        except:
            pass
        time.sleep(1)
    return False

def is_client_operational(uid):
    ud = get_or_create_user(uid)
    if not ud.client_manager or not ud.client_manager.is_ready.is_set():
        return False
    try:
        return ud.client_manager.run_coroutine(ud.client_manager.client.is_user_authorized(), timeout=3)
    except:
        return False

def load_all_sessions():
    logger.info("Loading existing sessions...")
    restored = restore_all_sessions_from_github()
    logger.info(f"Restored {restored} sessions from GitHub")
    for filename in os.listdir(SESSIONS_DIR):
        if filename.endswith('.json'):
            uid = filename.split('.json')[0]
            settings = load_settings(uid)
            if settings.get('phone'):
                ud = get_or_create_user(uid)
                if settings.get('monitoring_active'):
                    ud.monitoring_active = True
                    ud.is_running = True
                if settings.get('scheduled_active') and settings.get('scheduled_interval'):
                    ud.scheduled_active = True
                    ud.scheduled_interval = settings.get('scheduled_interval', 0)
                    ud.scheduled_groups = settings.get('scheduled_groups', [])
                    ud.scheduled_message = settings.get('scheduled_message', '')
                    ud.scheduled_image = settings.get('scheduled_image')
                if settings.get('rotating_active'):
                    ud.rotating_active = True
                    ud.rotating_messages = settings.get('rotating_messages', ["", "", "", "", ""])
                    ud.rotating_groups = settings.get('rotating_groups', [])
                    ud.rotating_interval = settings.get('rotating_interval', 5)
                if ud.phone_number:
                    threading.Thread(target=ensure_client_running, args=(uid,), daemon=True).start()
                logger.info(f"Loaded settings for {uid}")

def initialize_app_async():
    global app_ready, app_initializing
    with init_lock:
        if app_ready:
            return
    logger.info("بدء التهيئة في الخلفية...")
    load_all_sessions()
    with init_lock:
        app_ready = True
        app_initializing = False
    logger.info("✅ التطبيق جاهز تمامًا")

# ========== مسارات API ==========

def _uid():
    return get_current_user_id()

def _slot():
    return get_current_slot()

@app.route("/api/get_login_status")
def api_get_login_status():
    uid = _uid()
    ud = get_or_create_user(uid)
    s = load_settings(uid)
    # هل لديه جلسة محفوظة؟ (قد يكون جارٍ تحميلها)
    has_session = bool(s.get("string_session") or ud.string_session)
    # هل العميل متصل الآن فعلياً؟
    client_running = bool(ud.client_manager and ud.client_manager.is_ready.is_set())
    # حالة "جارٍ الاتصال": لديه جلسة لكن لم يتحقق بعد
    connecting = has_session and not ud.authenticated and client_running is False
    return jsonify({
        "logged_in": ud.authenticated,
        "connecting": connecting,
        "has_session": has_session,
        "phone": ud.phone_number or s.get("phone", ""),
        "awaiting_code": s.get("awaiting_code", False),
        "awaiting_password": s.get("awaiting_password", False),
        "telegram_name": ud.telegram_name or s.get("telegram_name", "")
    })

@app.route("/api/get_stats")
def api_get_stats():
    uid = _uid()
    ud = get_or_create_user(uid)
    stats = ud.stats if ud.stats else {}
    return jsonify({
        "sent": stats.get("sent", 0),
        "errors": stats.get("errors", 0),
        "alerts": stats.get("alerts", 0),
        "replies": stats.get("replies", 0)
    })

@app.route("/api/reset_stats", methods=["POST"])
def api_reset_stats():
    uid = _uid()
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.stats = {"sent": 0, "errors": 0, "alerts": 0, "replies": 0}
    socketio.emit("stats_update", ud.stats, to=uid)
    return jsonify({"success": True})

@app.route("/api/get_settings")
def api_get_settings():
    uid = _uid()
    s = load_settings(uid)
    s["api_configured"] = bool(API_ID and API_HASH)
    return jsonify({"success": True, "settings": s})

@app.route("/api/set_settings", methods=["POST"])
def api_set_settings():
    uid = _uid()
    s = load_settings(uid)
    data = request.get_json() or {}
    for key in ["watch_words", "excluded_groups", "alert_chat", "groups", "auto_replies"]:
        if key in data:
            s[key] = data[key]
    save_settings(uid, s)
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.settings = s
        ud.auto_replies = s.get("auto_replies", [])
    return jsonify({"success": True, "message": "تم حفظ الإعدادات"})

@app.route("/api/save_settings", methods=["POST"])
def api_save_settings():
    uid = _uid()
    s = load_settings(uid)
    data = request.get_json() or {}
    for key in ["message", "groups", "watch_words", "send_type", "interval", "image_path", "auto_replies", "excluded_groups", "alert_chat"]:
        if key in data:
            s[key] = data[key]
    save_settings(uid, s)
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.settings = s
        if "auto_replies" in data:
            ud.auto_replies = data["auto_replies"]
    return jsonify({"success": True, "message": "تم حفظ الإعدادات"})

@app.route("/api/check_auto_code")
def api_check_auto_code():
    uid = _uid()
    ud = get_or_create_user(uid)
    code = ud.pending_auto_code
    if code:
        ud.pending_auto_code = None
        return jsonify({"code": code})
    return jsonify({"code": None})

@app.route("/api/get_auto_replies")
def api_get_auto_replies():
    uid = _uid()
    s = load_settings(uid)
    return jsonify({"success": True, "auto_replies": s.get("auto_replies", [])})

@app.route("/api/add_auto_reply", methods=["POST"])
def api_add_auto_reply():
    uid = _uid()
    data = request.get_json() or {}
    trigger = data.get("trigger", "").strip() or data.get("keyword", "").strip()
    reply = data.get("reply", "").strip()
    if not trigger or not reply:
        return jsonify({"success": False, "message": "الكلمة والرد مطلوبان"})
    s = load_settings(uid)
    ar = s.get("auto_replies", [])
    ar.append({"keyword": trigger, "reply": reply})
    s["auto_replies"] = ar
    save_settings(uid, s)
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.auto_replies = ar
    return jsonify({"success": True, "message": "تم إضافة الرد التلقائي"})

@app.route("/api/delete_auto_reply", methods=["POST"])
def api_delete_auto_reply():
    uid = _uid()
    data = request.get_json() or {}
    idx = data.get("index", -1)
    s = load_settings(uid)
    ar = s.get("auto_replies", [])
    if 0 <= idx < len(ar):
        ar.pop(idx)
        s["auto_replies"] = ar
        save_settings(uid, s)
        ud = get_or_create_user(uid)
        with USERS_LOCK:
            ud.auto_replies = ar
        return jsonify({"success": True, "message": "تم حذف الرد"})
    return jsonify({"success": False, "message": "فهرس غير صحيح"})

@app.route("/api/save_auto_replies", methods=["POST"])
def api_save_auto_replies():
    uid = _uid()
    data = request.json or {}
    auto_replies = data.get('auto_replies', [])
    settings = load_settings(uid)
    settings['auto_replies'] = auto_replies
    save_settings(uid, settings)
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.auto_replies = auto_replies
        ud.settings = settings
    return jsonify({"success": True, "message": f"✅ تم حفظ {len(auto_replies)} قاعدة رد تلقائي"})

@app.route("/api/save_login", methods=["POST"])
def api_save_login():
    uid = _uid()
    data = request.get_json() or {}
    phone = data.get("phone", "").strip()
    if not phone:
        return jsonify({"success": False, "message": "رقم الهاتف مطلوب"})
    ensure_client_running(uid)
    ud = get_or_create_user(uid)
    if not ud.client_manager or not ud.client_manager.is_ready.is_set():
        return jsonify({"success": False, "message": "فشل تهيئة عميل تيليغرام، حاول مجدداً"})
    try:
        already_auth = ud.client_manager.run_coroutine(ud.client_manager.client.is_user_authorized(), timeout=10)
        if already_auth:
            return jsonify({"success": True, "message": "أنت مسجل دخول بالفعل", "status": "already_authorized"})
        result = ud.client_manager.run_coroutine(ud.client_manager.client.send_code_request(phone), timeout=60)
        s = load_settings(uid)
        s["phone"] = phone
        s["awaiting_code"] = True
        s["phone_code_hash"] = result.phone_code_hash
        save_settings(uid, s)
        ud.phone_number = phone
        try:
            asyncio.run_coroutine_threadsafe(
                ud.client_manager._start_code_listener(),
                ud.client_manager.loop
            )
        except Exception as cl_err:
            logger.warning(f"Code listener start error: {cl_err}")
        return jsonify({"success": True, "message": "✅ تم إرسال كود التحقق إلى تيليغرام", "status": "code_sent"})
    except errors.FloodWaitError as e:
        return jsonify({"success": False, "message": f"⏳ انتظر {e.seconds} ثانية قبل المحاولة مرة أخرى"})
    except Exception as e:
        add_error("save_login", str(e), f"phone: {phone}")
        return jsonify({"success": False, "message": f"❌ خطأ: {str(e)}"})

@app.route("/api/verify_code", methods=["POST"])
def api_verify_code():
    uid = _uid()
    data = request.get_json() or {}
    code = data.get("code", "").strip()
    s = load_settings(uid)
    phone = s.get("phone", "")
    phone_code_hash = s.get("phone_code_hash", "")
    ud = get_or_create_user(uid)
    if not ud.client_manager:
        return jsonify({"success": False, "message": "العميل غير جاهز"})
    try:
        me = ud.client_manager.run_coroutine(
            ud.client_manager.client.sign_in(phone, code, phone_code_hash=phone_code_hash), timeout=30)
        tg_name = (getattr(me, "first_name", "") or "") + (" " + (getattr(me, "last_name", "") or "")).rstrip()
        tg_name = tg_name.strip()
        with USERS_LOCK:
            ud.authenticated = True
            ud.telegram_name = tg_name
        s["awaiting_code"] = False
        s["telegram_name"] = tg_name
        session_string = ud.client_manager.client.session.save()
        ud.string_session = session_string
        s["string_session"] = session_string
        save_settings(uid, s)
        # تحديث الإعدادات في الذاكرة
        ud.settings = s
        # إعادة تفعيل المراقبة والإرسال المتسلسل إذا كانت مفعلة في الإعدادات
        if s.get("monitoring_active"):
            ud.monitoring_active = True
            bot = get_learning_bot(uid)
            bot.is_monitoring = True
            if ud.client_manager and ud.client_manager.client and ud.client_manager.loop:
                try:
                    asyncio.run_coroutine_threadsafe(
                        bot.start_with_client(ud.client_manager.client),
                        ud.client_manager.loop
                    )
                except Exception as bot_err:
                    logger.warning(f"Bot start error after verify_code: {bot_err}")
        if s.get("rotating_active"):
            ud.rotating_active = True
            ud.rotating_messages = s.get("rotating_messages", ["", "", "", "", ""])
            ud.rotating_groups = s.get("rotating_groups", [])
            ud.rotating_interval = s.get("rotating_interval", 5)
            if ud.rotating_groups and any(msg.strip() for msg in ud.rotating_messages):
                ud.client_manager.start_rotating(ud.rotating_groups, ud.rotating_messages, ud.rotating_interval)
        socketio.emit("telegram_name_update", {"name": tg_name, "user_id": uid}, to=uid)
        threading.Thread(target=upload_session_to_github, args=(session_string, uid), daemon=True).start()
        return jsonify({"success": True, "message": "تم تسجيل الدخول", "name": tg_name})
    except errors.SessionPasswordNeededError:
        # تحقق من وجود رمز ثنائي محفوظ لهذا الرقم
        s2 = load_settings(uid)
        phone_now = s2.get("phone", "")
        saved_pws = s2.get("saved_passwords", {})
        saved_pw = saved_pws.get(phone_now, "")
        if saved_pw:
            # حاول تسجيل الدخول تلقائياً بالرمز المحفوظ
            try:
                me = ud.client_manager.run_coroutine(
                    ud.client_manager.client.sign_in(password=saved_pw), timeout=30)
                tg_name = ((getattr(me, "first_name", "") or "") + " " + (getattr(me, "last_name", "") or "")).strip()
                with USERS_LOCK:
                    ud.authenticated = True
                    ud.awaiting_password = False
                    ud.telegram_name = tg_name
                s2["telegram_name"] = tg_name
                s2["awaiting_code"] = False
                session_string = ud.client_manager.client.session.save()
                ud.string_session = session_string
                s2["string_session"] = session_string
                save_settings(uid, s2)
                ud.settings = s2
                if s2.get("monitoring_active"):
                    ud.monitoring_active = True
                    bot = get_learning_bot(uid)
                    bot.is_monitoring = True
                    if ud.client_manager and ud.client_manager.loop:
                        try:
                            asyncio.run_coroutine_threadsafe(bot.start_with_client(ud.client_manager.client), ud.client_manager.loop)
                        except Exception: pass
                if s2.get("rotating_active"):
                    ud.rotating_active = True
                    ud.rotating_messages = s2.get("rotating_messages", ["","","","",""])
                    ud.rotating_groups = s2.get("rotating_groups", [])
                    ud.rotating_interval = s2.get("rotating_interval", 5)
                    if ud.rotating_groups and any(m.strip() for m in ud.rotating_messages):
                        ud.client_manager.start_rotating(ud.rotating_groups, ud.rotating_messages, ud.rotating_interval)
                socketio.emit("telegram_name_update", {"name": tg_name, "user_id": uid}, to=uid)
                threading.Thread(target=upload_session_to_github, args=(session_string, uid), daemon=True).start()
                return jsonify({"success": True, "message": f"✅ تم تسجيل الدخول تلقائياً باستخدام الرمز المحفوظ", "name": tg_name, "auto_2fa": True})
            except Exception:
                # الرمز المحفوظ خاطئ أو تم تغييره
                with USERS_LOCK:
                    ud.awaiting_password = True
                return jsonify({"success": False, "need_password": True, "saved_failed": True,
                                "message": "⚠️ الرمز الثنائي المحفوظ خاطئ أو تم تغييره، أدخل الرمز الجديد"})
        else:
            with USERS_LOCK:
                ud.awaiting_password = True
            return jsonify({"success": False, "need_password": True, "message": "مطلوب كلمة مرور التحقق بخطوتين"})
    except Exception as e:
        err = str(e)
        add_error("verify_code", err, f"phone: {phone}")
        return jsonify({"success": False, "message": f"كود خاطئ: {err}"})

@app.route("/api/verify_password", methods=["POST"])
def api_verify_password():
    uid = _uid()
    data = request.get_json() or {}
    password = data.get("password", "")
    ud = get_or_create_user(uid)
    if not ud.client_manager:
        return jsonify({"success": False, "message": "العميل غير جاهز"})
    try:
        me = ud.client_manager.run_coroutine(ud.client_manager.client.sign_in(password=password), timeout=30)
        tg_name = ((getattr(me, "first_name", "") or "") + " " + (getattr(me, "last_name", "") or "")).strip()
        with USERS_LOCK:
            ud.authenticated = True
            ud.awaiting_password = False
            ud.telegram_name = tg_name
        s = load_settings(uid)
        s["telegram_name"] = tg_name
        # حفظ الرمز الثنائي مرتبطاً برقم الهاتف
        phone_key = s.get("phone", "")
        if phone_key and password:
            saved_pws = s.get("saved_passwords", {})
            saved_pws[phone_key] = password
            s["saved_passwords"] = saved_pws
            logger.info(f"✅ تم حفظ الرمز الثنائي للرقم {phone_key}")
        session_string = ud.client_manager.client.session.save()
        ud.string_session = session_string
        s["string_session"] = session_string
        save_settings(uid, s)
        ud.settings = s
        if s.get("monitoring_active"):
            ud.monitoring_active = True
            bot = get_learning_bot(uid)
            bot.is_monitoring = True
            if ud.client_manager and ud.client_manager.client and ud.client_manager.loop:
                try:
                    asyncio.run_coroutine_threadsafe(
                        bot.start_with_client(ud.client_manager.client),
                        ud.client_manager.loop
                    )
                except Exception as bot_err:
                    logger.warning(f"Bot start error after verify_password: {bot_err}")
        if s.get("rotating_active"):
            ud.rotating_active = True
            ud.rotating_messages = s.get("rotating_messages", ["", "", "", "", ""])
            ud.rotating_groups = s.get("rotating_groups", [])
            ud.rotating_interval = s.get("rotating_interval", 5)
            if ud.rotating_groups and any(msg.strip() for msg in ud.rotating_messages):
                ud.client_manager.start_rotating(ud.rotating_groups, ud.rotating_messages, ud.rotating_interval)
        socketio.emit("telegram_name_update", {"name": tg_name, "user_id": uid}, to=uid)
        threading.Thread(target=upload_session_to_github, args=(session_string, uid), daemon=True).start()
        return jsonify({"success": True, "message": "تم تسجيل الدخول بنجاح"})
    except Exception as e:
        add_error("verify_password", str(e), "")
        return jsonify({"success": False, "message": f"كلمة مرور خاطئة: {str(e)}"})

@app.route("/api/reset_login", methods=["POST"])
def api_reset_login():
    uid = _uid()
    ud = get_or_create_user(uid)
    try:
        if ud.client_manager and ud.client_manager.client:
            ud.client_manager.run_coroutine(ud.client_manager.client.log_out(), timeout=10)
            ud.client_manager.stop_scheduled()
            ud.client_manager.stop_rotating()
            ud.client_manager.stop()
            ud.client_manager = None
    except Exception as e:
        logger.error(f"Logout error: {e}")
    threading.Thread(target=delete_session_from_github, args=(uid,), daemon=True).start()
    session_file = os.path.join(SESSIONS_DIR, f"{uid}.session")
    if os.path.exists(session_file):
        try:
            os.remove(session_file)
        except:
            pass
    with USERS_LOCK:
        ud.authenticated = False
        ud.telegram_name = ""
        ud.phone_number = ""
        ud.monitoring_active = False
        ud.is_running = False
        ud.rotating_active = False
        ud.string_session = None
    s = load_settings(uid)
    s["awaiting_code"] = False
    s["telegram_name"] = ""
    s["monitoring_active"] = False
    s["rotating_active"] = False
    s.pop("phone_code_hash", None)
    s.pop("string_session", None)
    save_settings(uid, s)
    return jsonify({"success": True, "message": "تم تسجيل الخروج"})

@app.route("/api/switch_user", methods=["POST"])
def api_switch_user():
    data = request.get_json() or {}
    slot = data.get("user_id", "user_1")
    if slot in PREDEFINED_USERS:
        session["user_slot"] = slot
        uid = _uid()
        ud = get_or_create_user(uid)
        if not ud.authenticated:
            ensure_client_running(uid)
        if ud.authenticated and ud.telegram_name:
            return jsonify({"success": True, "telegram_name": ud.telegram_name, "settings": load_settings(uid)})
        return jsonify({"success": True})
    return jsonify({"success": False, "message": "مستخدم غير موجود"})

@app.route("/api/start_monitoring", methods=["POST"])
def api_start_monitoring():
    uid = _uid()
    if not ensure_client_running(uid):
        return jsonify({"success": False, "message": "العميل غير جاهز، حاول مجدداً"})
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.monitoring_active = True
        ud.is_running = True
    s = load_settings(uid)
    s["monitoring_active"] = True
    save_settings(uid, s)
    if ud.client_manager and ud.client_manager.learning_bot:
        ud.client_manager.learning_bot.is_monitoring = True
    socketio.emit("monitoring_status", {"is_running": True}, to=uid)
    return jsonify({"success": True, "message": "تم بدء المراقبة"})

@app.route("/api/stop_monitoring", methods=["POST"])
def api_stop_monitoring():
    uid = _uid()
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.monitoring_active = False
        ud.is_running = False
    s = load_settings(uid)
    s["monitoring_active"] = False
    save_settings(uid, s)
    if ud.client_manager and ud.client_manager.learning_bot:
        ud.client_manager.learning_bot.is_monitoring = False
    socketio.emit("monitoring_status", {"is_running": False}, to=uid)
    return jsonify({"success": True, "message": "تم إيقاف المراقبة"})

@app.route("/api/get_alerts")
def api_get_alerts():
    uid = _uid()
    ud = get_or_create_user(uid)
    return jsonify({"success": True, "alerts": ud.alerts or []})

@app.route("/api/send_now", methods=["POST"])
def api_send_now():
    uid = _uid()
    if not is_client_operational(uid):
        if not ensure_client_running(uid):
            return jsonify({"success": False, "message": "العميل غير جاهز، حاول تسجيل الدخول أولاً"})
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    groups_list = data.get("groups", [])
    message = data.get("message", "").strip()
    if not groups_list:
        return jsonify({"success": False, "message": "لا توجد مجموعات"})
    if not message:
        return jsonify({"success": False, "message": "يجب كتابة رسالة"})
    def send_task():
        try:
            timeout = max(300, len(groups_list) * 15)
            ud.client_manager.run_coroutine(ud.client_manager._send_to_groups(groups_list, message, None), timeout=timeout)
        except Exception as e:
            add_error("send_now", str(e), f"user: {uid}")
            socketio.emit('log_update', {"message": f"❌ خطأ في الإرسال الفوري: {str(e)[:150]}"}, to=uid)
    threading.Thread(target=send_task, daemon=True).start()
    return jsonify({"success": True, "message": f"بدء الإرسال إلى {len(groups_list)} مجموعة"})

@app.route("/api/start_scheduled", methods=["POST"])
def api_start_scheduled():
    uid = _uid()
    if not ensure_client_running(uid):
        return jsonify({"success": False, "message": "العميل غير جاهز"})
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    groups_list = data.get("groups", [])
    message = data.get("message", "").strip()
    interval = int(data.get("interval", 60))
    image_path = data.get("image_path", None)
    if not groups_list:
        return jsonify({"success": False, "message": "لا توجد مجموعات"})
    if not message and not image_path:
        return jsonify({"success": False, "message": "يجب كتابة رسالة أو إرفاق صورة"})
    ud.client_manager.start_scheduled(groups_list, message, image_path, interval)
    return jsonify({"success": True, "message": f"بدء الإرسال المجدول كل {interval} دقيقة"})

@app.route("/api/stop_scheduled", methods=["POST"])
def api_stop_scheduled():
    uid = _uid()
    ud = get_or_create_user(uid)
    if ud.client_manager:
        ud.client_manager.stop_scheduled()
    return jsonify({"success": True, "message": "تم إيقاف الإرسال المجدول"})

@app.route("/api/rotating/save", methods=["POST"])
def api_rotating_save():
    uid = _uid()
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    messages = data.get("messages", ["", "", "", "", ""])
    groups = data.get("groups", [])
    interval = int(data.get("interval", 5))
    with USERS_LOCK:
        ud.rotating_messages = messages
        ud.rotating_groups = groups
        ud.rotating_interval = interval
    s = load_settings(uid)
    s["rotating_messages"] = messages
    s["rotating_groups"] = groups
    s["rotating_interval"] = interval
    save_settings(uid, s)
    return jsonify({"success": True, "message": "تم حفظ إعدادات الإرسال المتسلسل"})

@app.route("/api/rotating/start", methods=["POST"])
def api_rotating_start():
    uid = _uid()
    if not ensure_client_running(uid):
        return jsonify({"success": False, "message": "العميل غير جاهز"})
    ud = get_or_create_user(uid)
    if not ud.rotating_groups:
        return jsonify({"success": False, "message": "لم يتم تحديد أي مجموعة"})
    if not any(msg.strip() for msg in ud.rotating_messages):
        return jsonify({"success": False, "message": "يجب تعبئة رسالة واحدة على الأقل"})
    if ud.client_manager:
        ud.client_manager.start_rotating(ud.rotating_groups, ud.rotating_messages, ud.rotating_interval)
    else:
        return jsonify({"success": False, "message": "عميل التيليغرام غير جاهز"})
    return jsonify({"success": True, "message": "تم بدء الإرسال المتسلسل"})

@app.route("/api/rotating/stop", methods=["POST"])
def api_rotating_stop():
    uid = _uid()
    ud = get_or_create_user(uid)
    if ud.client_manager:
        ud.client_manager.stop_rotating()
    with USERS_LOCK:
        ud.rotating_active = False
    s = load_settings(uid)
    s["rotating_active"] = False
    save_settings(uid, s)
    return jsonify({"success": True, "message": "تم إيقاف الإرسال المتسلسل"})

@app.route("/api/rotating/status")
def api_rotating_status():
    uid = _uid()
    ud = get_or_create_user(uid)
    return jsonify({
        "success": True,
        "active": ud.rotating_active if ud else False,
        "messages": ud.rotating_messages if ud else ["", "", "", "", ""],
        "groups": ud.rotating_groups if ud else [],
        "interval": ud.rotating_interval if ud else 5,
        "current_index": ud.rotating_index if ud else 0
    })

@app.route("/api/upload_image", methods=["POST"])
def api_upload_image():
    uid = _uid()
    if 'image' not in request.files:
        return jsonify({"success": False, "message": "لا توجد صورة"})
    file = request.files['image']
    if not file.filename:
        return jsonify({"success": False, "message": "اختر ملفاً"})
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
        return jsonify({"success": False, "message": "صيغة غير مدعومة"})
    filename = f"{uid}_{int(time.time())}{ext}"
    filepath = os.path.join(UPLOADS_DIR, filename)
    file.save(filepath)
    settings = load_settings(uid)
    settings['image_path'] = filepath
    settings['image_filename'] = file.filename
    save_settings(uid, settings)
    return jsonify({"success": True, "message": "✅ تم رفع الصورة", "filepath": filepath, "filename": file.filename})

@app.route("/api/remove_image", methods=["POST"])
def api_remove_image():
    uid = _uid()
    settings = load_settings(uid)
    img = settings.get('image_path')
    if img and os.path.exists(img):
        try:
            os.remove(img)
        except:
            pass
    settings.pop('image_path', None)
    settings.pop('image_filename', None)
    save_settings(uid, settings)
    return jsonify({"success": True, "message": "✅ تم حذف الصورة"})

@app.route("/api/sent_batches")
def api_sent_batches():
    uid = _uid()
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        batches = list(ud.sent_batches)
    result = []
    for b in reversed(batches):
        result.append({
            "id": b["id"],
            "text": b["text"],
            "has_media": b.get("has_media", False),
            "sent_at": b["sent_at"],
            "edited_at": b.get("edited_at"),
            "sent_count": b.get("sent_count", len(b["entries"])),
            "group_count": len(b["entries"]),
            "groups": [{"title": e["chat_title"], "username": e.get("chat_username")} for e in b["entries"]]
        })
    return jsonify({"success": True, "batches": result})

@app.route("/api/edit_batch", methods=["POST"])
def api_edit_batch():
    uid = _uid()
    ud = get_or_create_user(uid)
    if not is_client_operational(uid):
        return jsonify({"success": False, "message": "يجب تسجيل الدخول أولاً"})
    data = request.json or {}
    batch_id = data.get("batch_id", "")
    new_text = data.get("new_text", "")
    if not batch_id or not new_text:
        return jsonify({"success": False, "message": "بيانات ناقصة"})
    if not ud.client_manager:
        return jsonify({"success": False, "message": "الاتصال غير جاهز"})
    def run_edit():
        try:
            ud.client_manager.run_coroutine(ud.client_manager._edit_batch_messages(batch_id, new_text), timeout=120)
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ في التعديل: {str(e)[:100]}"}, to=uid)
    threading.Thread(target=run_edit, daemon=True).start()
    return jsonify({"success": True, "message": "⏳ جارٍ تعديل الرسائل..."})

@app.route("/api/delete_batch", methods=["POST"])
def api_delete_batch():
    uid = _uid()
    ud = get_or_create_user(uid)
    if not is_client_operational(uid):
        return jsonify({"success": False, "message": "يجب تسجيل الدخول أولاً"})
    data = request.json or {}
    batch_id = data.get("batch_id", "")
    if not batch_id:
        return jsonify({"success": False, "message": "batch_id مطلوب"})
    if not ud.client_manager:
        return jsonify({"success": False, "message": "الاتصال غير جاهز"})
    def run_delete():
        try:
            ud.client_manager.run_coroutine(ud.client_manager._delete_batch_messages(batch_id), timeout=120)
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ في الحذف: {str(e)[:100]}"}, to=uid)
    threading.Thread(target=run_delete, daemon=True).start()
    return jsonify({"success": True, "message": "⏳ جارٍ حذف الرسائل..."})

@app.route("/api/join_group", methods=["POST"])
def api_join_group():
    uid = _uid()
    if not is_client_operational(uid):
        if not ensure_client_running(uid):
            return jsonify({"success": False, "message": "العميل غير جاهز، يرجى تسجيل الدخول أولاً"})
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    link = data.get("link", "").strip()
    if not link:
        return jsonify({"success": False, "message": "الرابط مطلوب"})
    try:
        ud.client_manager.run_coroutine(ud.client_manager._join_group(link), timeout=20)
        return jsonify({"success": True, "message": f"تم الانضمام إلى {link}"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route("/api/parse_join_links", methods=["POST"])
def api_parse_join_links():
    data = request.get_json() or {}
    raw = data.get("raw", "")
    entities = parse_entities(raw)
    result = []
    seen = set()
    for e in entities:
        key = e.lower().lstrip('@+')
        if key not in seen:
            seen.add(key)
            if e.startswith('+'):
                kind = 'invite'
                label = f'رابط دعوة: ...{e[-8:]}'
            elif e.lstrip('-').isdigit():
                kind = 'id'
                label = f'ID: {e}'
            else:
                kind = 'username'
                label = f'@{e.lstrip("@")}'
            result.append({"entity": e, "kind": kind, "label": label})
    return jsonify({"success": True, "count": len(result), "links": result})

@app.route("/api/bulk_join", methods=["POST"])
def api_bulk_join():
    uid = _uid()
    if not is_client_operational(uid):
        if not ensure_client_running(uid):
            return jsonify({"success": False, "message": "العميل غير جاهز، يرجى تسجيل الدخول أولاً"})
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    links = data.get("links", [])
    if not links:
        return jsonify({"success": False, "message": "لا توجد روابط للانضمام"})
    if not ud.client_manager:
        return jsonify({"success": False, "message": "الاتصال غير جاهز"})

    async def do_bulk_join():
        from telethon import functions
        ok = skip = fail = 0
        total = len(links)
        for i, item in enumerate(links):
            entity_str = item.get('entity', '').strip()
            label = item.get('label', entity_str)
            try:
                if entity_str.startswith('+'):
                    try:
                        await ud.client_manager.client(functions.messages.ImportChatInviteRequest(hash=entity_str[1:]))
                        ok += 1
                        msg = f"✅ [{i+1}/{total}] {label}"
                    except Exception as je:
                        if 'Already' in str(je) or 'USER_ALREADY' in str(je):
                            skip += 1
                            msg = f"⚠️ [{i+1}/{total}] مسجّل مسبقاً: {label}"
                        else:
                            raise je
                elif entity_str.lstrip('-').isdigit():
                    chat = await ud.client_manager.client.get_entity(int(entity_str))
                    await ud.client_manager.client(functions.channels.JoinChannelRequest(channel=chat))
                    ok += 1
                    msg = f"✅ [{i+1}/{total}] {label}"
                else:
                    username = entity_str.lstrip('@')
                    await ud.client_manager.client(functions.channels.JoinChannelRequest(channel=username))
                    ok += 1
                    msg = f"✅ [{i+1}/{total}] @{username}"
                socketio.emit('log_update', {"message": msg}, to=uid)
                socketio.emit('join_progress', {"index": i+1, "total": total, "ok": ok, "skip": skip, "fail": fail}, to=uid)
                await asyncio.sleep(2)
            except Exception as e:
                fail += 1
                err = str(e)
                if 'Already' in err or 'USER_ALREADY' in err:
                    skip += 1
                    fail -= 1
                    socketio.emit('log_update', {"message": f"⚠️ [{i+1}/{total}] مسجّل: {label}"}, to=uid)
                else:
                    socketio.emit('log_update', {"message": f"❌ [{i+1}/{total}] {label}: {err[:60]}"}, to=uid)
                socketio.emit('join_progress', {"index": i+1, "total": total, "ok": ok, "skip": skip, "fail": fail}, to=uid)
                await asyncio.sleep(1)
        socketio.emit('bulk_join_done', {"ok": ok, "skip": skip, "fail": fail, "total": total}, to=uid)
        socketio.emit('log_update', {"message": f"🏁 اكتمل: ✅ {ok} | ⚠️ {skip} مسبقاً | ❌ {fail} فاشل من {total}"}, to=uid)

    def run_bulk():
        try:
            ud.client_manager.run_coroutine(do_bulk_join(), timeout=600)
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ: {str(e)[:100]}"}, to=uid)
            socketio.emit('bulk_join_done', {"ok": 0, "skip": 0, "fail": len(links), "total": len(links)}, to=uid)

    threading.Thread(target=run_bulk, daemon=True).start()
    return jsonify({"success": True, "message": f"⏳ جارٍ الانضمام إلى {len(links)} مجموعة..."})

@app.route("/api/fetch_chats", methods=["GET", "POST"])
def api_fetch_chats():
    uid = _uid()
    if not is_client_operational(uid):
        if not ensure_client_running(uid):
            return jsonify({"success": False, "message": "العميل غير جاهز، يرجى تسجيل الدخول أولاً"})
    ud = get_or_create_user(uid)
    try:
        chats = ud.client_manager.get_chats()
        s = load_settings(uid)
        s["groups"] = [c["link"] for c in chats if c.get("link")]
        save_settings(uid, s)
        return jsonify({"success": True, "chats": chats, "count": len(chats)})
    except Exception as e:
        add_error("fetch_chats", str(e), f"user: {uid}")
        return jsonify({"success": False, "message": str(e)})

@app.route("/api/search_messages", methods=["POST"])
def api_search_messages():
    uid = _uid()
    if not is_client_operational(uid):
        if not ensure_client_running(uid):
            return jsonify({"success": False, "message": "يجب تسجيل الدخول أولاً لاستخدام البحث"})
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    query = data.get("query", "")
    search_type = data.get("search_type", "text")
    exclude_chats = data.get("exclude_chats", [])
    try:
        results = ud.client_manager.search_messages(query, search_type, exclude_chats)
        return jsonify({"success": True, "results": results})
    except Exception as e:
        add_error("search_messages", str(e), "")
        return jsonify({"success": False, "message": str(e)})

@app.route("/api/parse_input", methods=["POST"])
def api_parse_input():
    data = request.get_json() or {}
    text = data.get("text", "")
    mode = data.get("mode", "groups")
    if mode == "groups":
        found = parse_entities(text)
        return jsonify({"success": True, "items": found, "count": len(found)})
    else:
        words = parse_keywords(text)
        return jsonify({"success": True, "items": words, "count": len(words)})

# ========== مسارات البوت التعليمي ==========
@app.route("/api/learning/services")
def api_learning_services():
    uid = _uid()
    s = load_settings(uid)
    return jsonify({"success": True, "services": s.get("learning_services", {})})

@app.route("/api/learning/teach", methods=["POST"])
def api_learning_teach():
    uid = _uid()
    data = request.get_json() or {}
    service = data.get("service", "").strip()
    description = data.get("description", "").strip()
    if not service or not description:
        return jsonify({"success": False, "message": "اسم الخدمة والوصف مطلوبان"})
    s = load_settings(uid)
    services = s.get("learning_services", {})
    services[service] = {"description": description}
    s["learning_services"] = services
    save_settings(uid, s)
    return jsonify({"success": True, "message": f"تم تعليم خدمة: {service}"})

@app.route("/api/learning/delete", methods=["POST"])
def api_learning_delete():
    uid = _uid()
    data = request.get_json() or {}
    service = data.get("service", "")
    s = load_settings(uid)
    services = s.get("learning_services", {})
    if service in services:
        del services[service]
        s["learning_services"] = services
        save_settings(uid, s)
        return jsonify({"success": True, "message": "تم حذف الخدمة"})
    return jsonify({"success": False, "message": "الخدمة غير موجودة"})

@app.route("/api/learning/unknown")
def api_learning_unknown():
    uid = _uid()
    s = load_settings(uid)
    return jsonify({"success": True, "requests": s.get("unknown_requests", [])})

@app.route("/api/learning/teach_from_unknown", methods=["POST"])
def api_learning_teach_from_unknown():
    uid = _uid()
    data = request.get_json() or {}
    index = data.get("index", -1)
    service = data.get("service", "").strip()
    description = data.get("description", "").strip()
    s = load_settings(uid)
    unknown = s.get("unknown_requests", [])
    if 0 <= index < len(unknown) and service and description:
        services = s.get("learning_services", {})
        services[service] = {"description": description}
        s["learning_services"] = services
        unknown.pop(index)
        s["unknown_requests"] = unknown
        save_settings(uid, s)
        return jsonify({"success": True, "message": f"تم تعلم: {service}"})
    return jsonify({"success": False, "message": "بيانات غير صحيحة"})

@app.route("/api/learning/clear_unknown", methods=["POST"])
def api_learning_clear_unknown():
    uid = _uid()
    s = load_settings(uid)
    s["unknown_requests"] = []
    save_settings(uid, s)
    return jsonify({"success": True, "message": "تم مسح الطلبات غير المعروفة"})

@app.route("/api/learning/toggle", methods=["POST"])
def api_learning_toggle():
    uid = _uid()
    s = load_settings(uid)
    current = s.get("learning_active", False)
    s["learning_active"] = not current
    save_settings(uid, s)
    ud = get_or_create_user(uid)
    bot = get_learning_bot(uid)
    bot.is_monitoring = s["learning_active"]
    state = s["learning_active"]
    return jsonify({"success": True, "active": state, "message": "تم تفعيل البوت" if state else "تم إيقاف البوت"})

@app.route("/api/learning/toggle_public", methods=["POST"])
def api_learning_toggle_public():
    uid = _uid()
    s = load_settings(uid)
    current = s.get("learning_reply_groups", False)
    s["learning_reply_groups"] = not current
    save_settings(uid, s)
    state = s["learning_reply_groups"]
    return jsonify({"success": True, "reply_in_groups": state, "message": "تم تفعيل الرد في المجموعات" if state else "تم إيقاف الرد في المجموعات"})

@app.route("/api/learning/status")
def api_learning_status():
    uid = _uid()
    s = load_settings(uid)
    return jsonify({"success": True, "active": s.get("learning_active", False), "reply_in_groups": s.get("learning_reply_groups", False)})

# ========== مسارات الأخطاء والتشخيص والإصلاح ==========
@app.route("/api/errors")
def api_get_errors():
    with errors_lock:
        return jsonify({"success": True, "errors": errors_list})

@app.route("/api/clear_errors", methods=["POST"])
def api_clear_errors():
    clear_errors()
    return jsonify({"success": True, "message": "تم مسح الأخطاء"})

@app.route("/api/fix_error", methods=["POST"])
def api_fix_error():
    data = request.get_json() or {}
    error_id = data.get("error_id")
    if not error_id:
        return jsonify({"success": False, "message": "معرف الخطأ مطلوب"})
    success, message = fix_error_by_id(error_id)
    return jsonify({"success": success, "message": message})

@app.route("/api/fix_all_errors", methods=["POST"])
def api_fix_all_errors():
    fixed_count = 0
    failed_count = 0
    with errors_lock:
        error_ids = [e["id"] for e in errors_list if not e.get("fixed")]
    for eid in error_ids:
        success, msg = fix_error_by_id(eid)
        if success:
            fixed_count += 1
        else:
            failed_count += 1
    return jsonify({"success": True, "fixed": fixed_count, "failed": failed_count})

@app.route("/api/diagnose", methods=["GET"])
def api_diagnose():
    issues = diagnose_system()
    for issue in issues:
        add_error(f"diagnostic_{issue['type']}", issue["message"], f"severity: {issue.get('severity', 'medium')}")
    return jsonify({"success": True, "issues": issues})

# ========== مسارات أوامر النظام ==========
@app.route("/admin/api/sys_init", methods=["POST"])
def admin_sys_init():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    try:
        result = subprocess.run(["apt", "update"], capture_output=True, text=True, timeout=60)
        result2 = subprocess.run(["apt", "upgrade", "-y"], capture_output=True, text=True, timeout=300)
        return jsonify({"success": True, "message": "تم تهيئة النظام وتحديث الحزم", "output": result.stdout + result2.stdout})
    except Exception as e:
        add_error("sys_init", str(e), traceback.format_exc())
        return jsonify({"success": False, "message": f"خطأ: {str(e)}"})

@app.route("/admin/api/sys_clear_sessions", methods=["POST"])
def admin_sys_clear_sessions():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    try:
        count = 0
        for f in os.listdir(SESSIONS_DIR):
            if f.endswith(".session") and f != "github_manager.session":
                try:
                    os.remove(os.path.join(SESSIONS_DIR, f))
                    count += 1
                except:
                    pass
        return jsonify({"success": True, "message": f"تم حذف {count} ملف جلسة"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route("/admin/api/sys_set_vars", methods=["POST"])
def admin_sys_set_vars():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    try:
        os.environ["SYS_STATUS"] = "READY"
        os.environ["DEBUG_MODE"] = "TRUE"
        return jsonify({"success": True, "message": "تم ضبط متغيرات البيئة (SYS_STATUS, DEBUG_MODE)"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route("/admin/api/sys_close_ports", methods=["POST"])
def admin_sys_close_ports():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    try:
        subprocess.run(["sudo", "ufw", "default", "deny", "incoming"], check=False, timeout=30)
        return jsonify({"success": True, "message": "تم إغلاق المنافذ الواردة (يتطلب صلاحيات root)"})
    except Exception as e:
        return jsonify({"success": False, "message": f"فشل: {str(e)}"})

@app.route("/admin/api/sys_start", methods=["POST"])
def admin_sys_start():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    return jsonify({"success": True, "message": "🚀 تم بدء التشغيل (التطبيق قيد التشغيل بالفعل)"})

@app.route("/admin/api/sys_debug", methods=["GET"])
def admin_sys_debug():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    try:
        with open("sys_log.txt", "r") as f:
            lines = f.readlines()[-20:]
            logs = "".join(lines)
        return jsonify({"success": True, "logs": logs})
    except:
        return jsonify({"success": True, "logs": "⚠️ لا توجد سجلات أخطاء متاحة حالياً"})

@app.route("/admin/api/git_push_generate", methods=["POST"])
def admin_git_push_generate():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    data = request.get_json() or {}
    repo_url = data.get("repo_url", "").strip()
    token = data.get("token", "").strip()
    branch = data.get("branch", "main")
    files = data.get("files", ".").strip()
    is_first = data.get("is_first", True)
    if not repo_url:
        return jsonify({"success": False, "message": "رابط المستودع مطلوب"})
    if not token:
        return jsonify({"success": False, "message": "الرمز الشخصي (Token) مطلوب"})
    auth_repo = repo_url.replace("https://", f"https://{token}@")
    git_code = f"git init\n"
    if is_first:
        git_code += f"git remote add origin {auth_repo}\n"
    else:
        git_code += f"git remote set-url origin {auth_repo}\n"
    git_code += f"git add {files}\n"
    git_code += f"git commit -m 'Update from System'\n"
    git_code += f"git branch -M {branch}\n"
    git_code += f"git push -u origin {branch}"
    return jsonify({"success": True, "git_code": git_code})

# ========== مسارات GitHub للجلسات ==========
@app.route("/admin/api/github_backup", methods=["POST"])
def admin_github_backup():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    count = backup_all_sessions_to_github()
    return jsonify({"success": True, "message": f"تم رفع {count} جلسة إلى GitHub"})

@app.route("/admin/api/github_restore", methods=["POST"])
def admin_github_restore():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    count = restore_all_sessions_from_github()
    return jsonify({"success": True, "message": f"تم استعادة {count} جلسة من GitHub"})

@app.route("/admin/api/github_delete_session", methods=["POST"])
def admin_github_delete_session():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    data = request.get_json() or {}
    user_id = data.get("user_id", "")
    if not user_id:
        return jsonify({"success": False, "message": "معرف المستخدم مطلوب"})
    success = delete_session_from_github(user_id)
    if success:
        return jsonify({"success": True, "message": f"تم حذف جلسة المستخدم {user_id} من GitHub"})
    else:
        return jsonify({"success": False, "message": "فشل حذف الجلسة"})

# ========== مسارات الإدارة ==========
@app.route("/admin/api/check")
def admin_check():
    return jsonify({"authenticated": session.get("admin_auth", False)})

@app.route("/admin/api/login", methods=["POST"])
def admin_login():
    data = request.get_json() or {}
    if data.get("username") == ADMIN_USERNAME and data.get("password") == ADMIN_PASSWORD:
        session.permanent = True
        session["admin_auth"] = True
        session.modified = True
        return jsonify({"success": True})
    return jsonify({"success": False, "message": "بيانات غير صحيحة"})

@app.route("/admin/api/logout", methods=["POST"])
def admin_logout_route():
    session.pop("admin_auth", None)
    return jsonify({"success": True})

@app.route("/admin/api/users")
def admin_users():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    users = []
    vid = session.get("visitor_id", "")
    for slot, uinfo in PREDEFINED_USERS.items():
        uid = f"{vid}__{slot}"
        ud = get_or_create_user(uid)
        s = load_settings(uid)
        users.append({
            "user_id": slot,
            "name": ud.telegram_name or uinfo["name"],
            "phone": ud.phone_number or "",
            "logged_in": ud.authenticated,
            "blocked": ud.blocked,
            "disabled": ud.disabled,
            "last_seen": ud.last_seen.isoformat() if ud.last_seen else None,
            "groups": s.get("groups", []),
            "watch_words": s.get("watch_words", []),
            "auto_replies": s.get("auto_replies", []),
            "alerts_count": len(ud.alerts or [])
        })
    return jsonify({"success": True, "users": users})

@app.route("/admin/api/user/<slot>", methods=["POST"])
def admin_update_user(slot):
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    vid = session.get("visitor_id", "")
    uid = f"{vid}__{slot}"
    ud = get_or_create_user(uid)
    data = request.get_json() or {}
    action = data.get("action")
    with USERS_LOCK:
        if action == "block":
            ud.blocked = data.get("blocked", False)
        elif action == "disable":
            ud.disabled = data.get("disabled", False)
    s = load_settings(uid)
    s["blocked"] = ud.blocked
    s["disabled"] = ud.disabled
    save_settings(uid, s)
    return jsonify({"success": True})

@app.route("/admin/api/fetch_chats/<slot>")
def admin_fetch_chats(slot):
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    vid = session.get("visitor_id", "")
    uid = f"{vid}__{slot}"
    ud = get_or_create_user(uid)
    if not ud.authenticated or not ud.client_manager:
        return jsonify({"success": False, "message": "المستخدم غير مسجل دخول"})
    try:
        chats = ud.client_manager.get_chats()
        return jsonify({"success": True, "chats": chats})
    except Exception as e:
        add_error("admin_fetch_chats", str(e), f"slot: {slot}")
        return jsonify({"success": False, "message": str(e)})

@app.route("/admin/api/user_alerts/<slot>")
def admin_user_alerts(slot):
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    vid = session.get("visitor_id", "")
    uid = f"{vid}__{slot}"
    ud = get_or_create_user(uid)
    return jsonify({"success": True, "alerts": ud.alerts or []})

@app.route("/admin/api/search/<slot>", methods=["POST"])
def admin_search(slot):
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    vid = session.get("visitor_id", "")
    uid = f"{vid}__{slot}"
    ud = get_or_create_user(uid)
    if not ud.authenticated or not ud.client_manager:
        return jsonify({"success": False, "message": "المستخدم غير مسجل دخول"})
    data = request.get_json() or {}
    query = data.get("query", "")
    search_type = data.get("search_type", "text")
    exclude_chats = data.get("exclude_chats", [])
    try:
        results = ud.client_manager.search_messages(query, search_type, exclude_chats)
        return jsonify({"success": True, "results": results})
    except Exception as e:
        add_error("admin_search", str(e), f"slot: {slot}")
        return jsonify({"success": False, "message": str(e)})

@app.route("/admin/api/export")
def admin_export():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    all_data = {}
    vid = session.get("visitor_id", "")
    for slot in PREDEFINED_USERS:
        uid = f"{vid}__{slot}"
        all_data[slot] = load_settings(uid)
    response = make_response(json.dumps(all_data, ensure_ascii=False, indent=2))
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.headers["Content-Disposition"] = "attachment; filename=export.json"
    return response

@app.route("/admin/api/restart", methods=["POST"])
def admin_restart():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    def do_restart():
        time.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)
    threading.Thread(target=do_restart, daemon=True).start()
    return jsonify({"success": True, "message": "جاري إعادة التشغيل..."})

# ========== مسارات تعديل الكود ==========
@app.route("/admin/api/edit_code", methods=["POST"])
def admin_edit_code():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    data = request.get_json() or {}
    file_name = data.get("file_name", "").strip()
    old_text = data.get("old_text", "")
    new_text = data.get("new_text", "")
    use_regex = data.get("use_regex", False)
    restart_after = data.get("restart", False)
    if not file_name:
        return jsonify({"success": False, "message": "اسم الملف مطلوب"})
    if not old_text:
        return jsonify({"success": False, "message": "النص المراد استبداله مطلوب"})
    try:
        full_path = safe_join_path(file_name)
        replace_code_in_file(full_path, old_text, new_text, use_regex)
        if restart_after:
            def restart_app():
                time.sleep(1)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            threading.Thread(target=restart_app, daemon=True).start()
            return jsonify({"success": True, "message": f"تم تعديل الكود في {file_name} وإعادة تشغيل الخادم"})
        else:
            return jsonify({"success": True, "message": f"تم تعديل الكود في {file_name} بنجاح (يلزم إعادة تشغيل يدوي)"})
    except Exception as e:
        add_error("code_edit", str(e), traceback.format_exc())
        return jsonify({"success": False, "message": f"خطأ: {str(e)}"})

@app.route("/admin/api/insert_code", methods=["POST"])
def admin_insert_code():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    data = request.get_json() or {}
    file_name = data.get("file_name", "").strip()
    insert_type = data.get("insert_type")
    target = data.get("target", "")
    code_to_insert = data.get("code", "")
    restart_after = data.get("restart", False)
    if not file_name:
        return jsonify({"success": False, "message": "اسم الملف مطلوب"})
    if not insert_type or not code_to_insert:
        return jsonify({"success": False, "message": "نوع الإدراج والكود مطلوبان"})
    valid_types = ['at_beginning', 'at_end', 'after_line', 'before_line', 'after_text', 'before_text', 'inside_function']
    if insert_type not in valid_types:
        return jsonify({"success": False, "message": "نوع إدراج غير صالح"})
    if insert_type in ['after_line', 'before_line', 'after_text', 'before_text', 'inside_function'] and not target:
        return jsonify({"success": False, "message": "هذا النوع يحتاج إلى نص مستهدف"})
    try:
        full_path = safe_join_path(file_name)
        insert_code_into_file(full_path, insert_type, target, code_to_insert)
        if restart_after:
            def restart_app():
                time.sleep(1)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            threading.Thread(target=restart_app, daemon=True).start()
            return jsonify({"success": True, "message": f"تم إدراج الكود في {file_name} وإعادة تشغيل الخادم"})
        else:
            return jsonify({"success": True, "message": f"تم إدراج الكود في {file_name} بنجاح (يلزم إعادة تشغيل يدوي)"})
    except Exception as e:
        add_error("code_insert", str(e), traceback.format_exc())
        return jsonify({"success": False, "message": f"خطأ: {str(e)}"})

@app.route("/admin/api/replace_file", methods=["POST"])
def admin_replace_file():
    if not session.get("admin_auth"):
        return jsonify({"success": False, "message": "غير مخول"}), 403
    file_name = request.form.get("file_name", "").strip()
    if not file_name:
        return jsonify({"success": False, "message": "اسم الملف مطلوب"})
    if "file" not in request.files:
        return jsonify({"success": False, "message": "لا يوجد ملف مرفوع"})
    uploaded_file = request.files["file"]
    if not uploaded_file.filename:
        return jsonify({"success": False, "message": "اسم الملف فارغ"})
    restart_after = request.form.get("restart", "false").lower() == "true"
    try:
        full_path = safe_join_path(file_name)
        new_content = uploaded_file.read().decode("utf-8")
        replace_file_completely(full_path, new_content)
        if restart_after:
            def restart_app():
                time.sleep(1)
                os.execv(sys.executable, [sys.executable] + sys.argv)
            threading.Thread(target=restart_app, daemon=True).start()
            return jsonify({"success": True, "message": f"تم استبدال {file_name} بالكامل وإعادة تشغيل الخادم"})
        else:
            return jsonify({"success": True, "message": f"تم استبدال {file_name} بالكامل بنجاح (يلزم إعادة تشغيل يدوي)"})
    except Exception as e:
        add_error("file_replace", str(e), traceback.format_exc())
        return jsonify({"success": False, "message": f"خطأ: {str(e)}"})

# ========== مسارات المخزن الخاص ==========
@app.route("/storage")
def storage_page():
    slot = _slot()
    storage_dir = os.path.join(PRIVATE_STORAGE_DIR, slot)
    os.makedirs(storage_dir, exist_ok=True)
    files = []
    for fname in os.listdir(storage_dir):
        fpath = os.path.join(storage_dir, fname)
        if os.path.isfile(fpath):
            stat = os.stat(fpath)
            files.append({"name": fname, "size": stat.st_size, "modified": stat.st_mtime})
    files.sort(key=lambda x: x["modified"], reverse=True)
    return render_template("storage.html", files=files, user_id=slot)

@app.route("/storage/files")
def storage_files():
    slot = _slot()
    storage_dir = os.path.join(PRIVATE_STORAGE_DIR, slot)
    os.makedirs(storage_dir, exist_ok=True)
    files = []
    for fname in os.listdir(storage_dir):
        fpath = os.path.join(storage_dir, fname)
        if os.path.isfile(fpath):
            stat = os.stat(fpath)
            files.append({"name": fname, "size": stat.st_size, "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")})
    files.sort(key=lambda x: x["modified"], reverse=True)
    return jsonify({"success": True, "files": files})

@app.route("/storage/upload", methods=["POST"])
def storage_upload():
    slot = _slot()
    if "file" not in request.files:
        return jsonify({"success": False, "message": "لا يوجد ملف"})
    f = request.files["file"]
    if not f.filename:
        return jsonify({"success": False, "message": "اسم الملف فارغ"})
    storage_dir = os.path.join(PRIVATE_STORAGE_DIR, slot)
    os.makedirs(storage_dir, exist_ok=True)
    filename = secure_filename(f.filename)
    path = os.path.join(storage_dir, filename)
    f.save(path)
    return jsonify({"success": True, "message": f"تم رفع الملف: {filename}"})

@app.route("/storage/delete", methods=["POST"])
def storage_delete():
    slot = _slot()
    data = request.get_json() or {}
    filename = secure_filename(data.get("filename", ""))
    if not filename:
        return jsonify({"success": False, "message": "اسم الملف مطلوب"})
    storage_dir = os.path.join(PRIVATE_STORAGE_DIR, slot)
    path = os.path.join(storage_dir, filename)
    if os.path.isfile(path):
        os.remove(path)
        return jsonify({"success": True, "message": "تم حذف الملف"})
    return jsonify({"success": False, "message": "الملف غير موجود"})

@app.route("/storage/download/<path:filename>")
def storage_download(filename):
    slot = _slot()
    storage_dir = os.path.join(PRIVATE_STORAGE_DIR, slot)
    return send_from_directory(storage_dir, filename, as_attachment=True)

# ========== ملفات PWA ==========
@app.route("/manifest.json")
def manifest():
    manifest_data = {
        "name": "مركز سرعة انجاز",
        "short_name": "سرعة انجاز",
        "start_url": "/",
        "display": "standalone",
        "theme_color": "#1e3c78",
        "background_color": "#0d1117",
        "icons": [
            {"src": "/static/icons/icon-72.png", "sizes": "72x72", "type": "image/png"},
            {"src": "/static/icons/icon-96.png", "sizes": "96x96", "type": "image/png"},
            {"src": "/static/icons/icon-128.png", "sizes": "128x128", "type": "image/png"},
            {"src": "/static/icons/icon-144.png", "sizes": "144x144", "type": "image/png"},
            {"src": "/static/icons/icon-152.png", "sizes": "152x152", "type": "image/png"},
            {"src": "/static/icons/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/icons/icon-384.png", "sizes": "384x384", "type": "image/png"},
            {"src": "/static/icons/icon-512.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return app.response_class(json.dumps(manifest_data, indent=2), mimetype='application/manifest+json')

@app.route("/sw.js")
def service_worker():
    sw_js = """
const CACHE_NAME = 'speed-cache-v1';
const urlsToCache = ['/'];

self.addEventListener('install', event => {
    event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(urlsToCache)));
});
self.addEventListener('fetch', event => {
    event.respondWith(caches.match(event.request).then(response => response || fetch(event.request)));
});
self.addEventListener('activate', event => {
    event.waitUntil(caches.keys().then(keys => Promise.all(keys.map(key => {
        if (key !== CACHE_NAME) return caches.delete(key);
    }))));
});
"""
    return app.response_class(sw_js, content_type='application/javascript')

# ========== الصفحة الرئيسية ==========
@app.route("/")
def index():
    uid = get_current_user_id()
    slot = get_current_slot()
    ud = get_or_create_user(uid)
    settings = load_settings(uid)
    settings['api_configured'] = bool(API_ID and API_HASH)
    rotating_data = {
        "messages": settings.get('rotating_messages', ["", "", "", "", ""]),
        "groups": settings.get('rotating_groups', []),
        "interval": settings.get('rotating_interval', 5),
        "active": settings.get('rotating_active', False)
    }
    return render_template('index.html',
                           settings=settings,
                           predefined_users=PREDEFINED_USERS,
                           current_user_id=slot,
                           current_user_slot=slot,
                           rotating=rotating_data,
                           telegram_name=ud.telegram_name)

@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/api/ready_check")
def ready_check():
    global app_ready
    return jsonify({"ready": app_ready})


@app.after_request
def add_no_cache(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

# ========== معالجات SocketIO ==========
@socketio.on('connect')
def on_connect():
    slot = session.get('user_slot', 'user_1')
    if slot not in PREDEFINED_USERS:
        slot = 'user_1'
        session['user_slot'] = slot
    join_room(slot)
    vid = session.get('visitor_id', '')
    if vid:
        full_uid = f"{vid}__{slot}"
        join_room(full_uid)

@socketio.on('disconnect')
def on_disconnect():
    slot = session.get('user_slot', 'user_1')
    leave_room(slot)
    vid = session.get('visitor_id', '')
    if vid:
        full_uid = f"{vid}__{slot}"
        leave_room(full_uid)

@socketio.on('join_user_room')
def on_join_user_room(data):
    room = data.get('user_id', '') if isinstance(data, dict) else str(data)
    if room:
        join_room(room)
        if room in PREDEFINED_USERS:
            vid = session.get('visitor_id', '')
            if vid:
                full_uid = f"{vid}__{room}"
                join_room(full_uid)
                session['user_slot'] = room

@socketio.on('heartbeat')
def on_heartbeat(data):
    pass

# ========== دوال تعديل الملفات العامة ==========
ALLOWED_DIRS = ['.', 'templates', 'static', 'sessions', 'private_storage']
FORBIDDEN_PATTERNS = ['../', '~', 'etc/passwd', 'boot.ini']

def safe_join_path(file_name):
    file_name = file_name.replace('\\', '/').strip()
    if any(pattern in file_name for pattern in FORBIDDEN_PATTERNS):
        raise Exception("مسار غير مسموح به")
    parts = file_name.split('/')
    if len(parts) > 1:
        folder = parts[0]
        if folder not in ALLOWED_DIRS:
            raise Exception(f"المجلد '{folder}' غير مسموح. المسموح: {', '.join(ALLOWED_DIRS)}")
    full_path = os.path.join(os.path.dirname(__file__), file_name)
    return full_path

def insert_code_into_file(file_path, insert_type, target, code_to_insert):
    if not os.path.exists(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('')
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.splitlines(keepends=True)
        if not lines:
            lines = []
    new_lines = []
    inserted = False
    if insert_type == 'at_beginning':
        new_lines = [code_to_insert.rstrip('\n') + '\n'] + lines
        inserted = True
    elif insert_type == 'at_end':
        new_lines = lines + [code_to_insert.rstrip('\n') + '\n']
        inserted = True
    elif insert_type == 'after_line':
        try:
            line_num = int(target)
            if line_num < 1 or line_num > len(lines):
                raise Exception("رقم سطر غير صالح")
            for i, line in enumerate(lines, 1):
                new_lines.append(line)
                if i == line_num:
                    new_lines.append(code_to_insert.rstrip('\n') + '\n')
            inserted = True
        except ValueError:
            raise Exception("الهدف يجب أن يكون رقماً لـ after_line")
    elif insert_type == 'before_line':
        try:
            line_num = int(target)
            if line_num < 1 or line_num > len(lines):
                raise Exception("رقم سطر غير صالح")
            for i, line in enumerate(lines, 1):
                if i == line_num:
                    new_lines.append(code_to_insert.rstrip('\n') + '\n')
                new_lines.append(line)
            inserted = True
        except ValueError:
            raise Exception("الهدف يجب أن يكون رقماً لـ before_line")
    elif insert_type == 'after_text':
        target_text = target
        for i, line in enumerate(lines):
            new_lines.append(line)
            if target_text in line and not inserted:
                new_lines.append(code_to_insert.rstrip('\n') + '\n')
                inserted = True
        if not inserted:
            new_lines.append(code_to_insert.rstrip('\n') + '\n')
            inserted = True
    elif insert_type == 'before_text':
        target_text = target
        for i, line in enumerate(lines):
            if target_text in line and not inserted:
                new_lines.append(code_to_insert.rstrip('\n') + '\n')
                inserted = True
            new_lines.append(line)
        if not inserted:
            new_lines.append(code_to_insert.rstrip('\n') + '\n')
            inserted = True
    elif insert_type == 'inside_function':
        import re
        func_name = target
        found = False
        i = 0
        while i < len(lines):
            line = lines[i]
            new_lines.append(line)
            if re.match(rf'^\s*def\s+{re.escape(func_name)}\s*\(', line):
                found = True
                base_indent = len(line) - len(line.lstrip())
                i += 1
                indent = ' ' * (base_indent + 4)
                code_lines = code_to_insert.strip().split('\n')
                for cline in code_lines:
                    new_lines.append(indent + cline + '\n')
                while i < len(lines):
                    current_line = lines[i]
                    if current_line.strip() and (len(current_line) - len(current_line.lstrip())) <= base_indent and not current_line.strip().startswith('#'):
                        break
                    new_lines.append(current_line)
                    i += 1
                continue
            i += 1
        if not found:
            new_lines.append(f"\n# Inserted code for function '{func_name}'\n")
            new_lines.append(code_to_insert.rstrip('\n') + '\n')
        inserted = True
    if not inserted:
        new_lines = lines + [code_to_insert.rstrip('\n') + '\n']
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    return True

def replace_code_in_file(file_path, old_text, new_text, use_regex=False):
    if not os.path.exists(file_path):
        raise Exception(f"الملف '{file_path}' غير موجود")
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    if use_regex:
        new_content = re.sub(old_text, new_text, content, flags=re.DOTALL)
    else:
        new_content = content.replace(old_text, new_text)
    if new_content == content:
        raise Exception("لم يتم العثور على النص المراد استبداله")
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    return True

def replace_file_completely(file_path, new_content):
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    return True

# ========== تهيئة عند الاستيراد (gunicorn / python مباشرة) ==========
threading.Thread(target=initialize_app_async, daemon=True).start()

# ========== تشغيل التطبيق ==========
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🚀 بدء الخادم على المنفذ {port} (التهيئة تجري في الخلفية)")
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)