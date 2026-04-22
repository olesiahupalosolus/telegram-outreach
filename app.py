import os
import uuid
import threading
import time
import json
import asyncio
import io
import secrets
import requests
import pandas as pd
from flask import Flask, render_template, request, jsonify
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# --- Налаштування шляхів ---
base_dir = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__,
            template_folder=os.path.join(base_dir, 'templates'),
            static_folder=os.path.join(base_dir, 'static'))

# SECRET_KEY береться з env. У продакшні — обовʼязково задати!
# Якщо не задано — згенерується випадковий ключ на сесію (сесії не будуть переживати рестарт).
_env_secret = os.environ.get('SECRET_KEY')
if not _env_secret:
    if os.environ.get('FLASK_ENV') == 'production' or os.environ.get('RAILWAY_ENVIRONMENT'):
        raise RuntimeError(
            "SECRET_KEY environment variable is required in production. "
            "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
        )
    _env_secret = secrets.token_hex(32)
    print("[WARN] SECRET_KEY not set — using ephemeral random key (dev only).")
app.secret_key = _env_secret

ACCOUNTS_FILE = os.path.join(base_dir, 'accounts.json')
SESSIONS_DIR = os.path.join(base_dir, 'sessions')
os.makedirs(SESSIONS_DIR, exist_ok=True)

# Multi-session support: each browser tab gets its own session_id
all_sessions = {}
pending_auth = {}

# ─── Helpers ────────────────────────────────────────────────────────────────

def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return json.loads(content) if content else {}
        except Exception:
            return {}
    return {}

def save_accounts(accounts):
    with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)

def read_google_sheet(sheet_url):
    if '/d/' not in sheet_url:
        raise ValueError("Невірне посилання на Google Sheet")
    sheet_id = sheet_url.split('/d/')[1].split('/')[0]
    gid = None
    if 'gid=' in sheet_url:
        gid = sheet_url.split('gid=')[1].split('&')[0].split('#')[0]
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    if gid:
        csv_url += f"&gid={gid}"
    response = requests.get(csv_url, timeout=15)
    if response.status_code != 200:
        raise ValueError("Не вдалося отримати таблицю. Переконайтесь що вона відкрита (Спільний доступ → Переглядач).")
    df = pd.read_csv(io.StringIO(response.text))
    return df

# ─── Routes ─────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    accounts = load_accounts()
    return render_template('index.html', accounts=list(accounts.keys()))

@app.route('/preview', methods=['POST'])
def preview():
    try:
        data = request.json
        df = read_google_sheet(data['sheet_url'])
        return jsonify({
            'success': True,
            'columns': list(df.columns),
            'preview': df.head(5).fillna('').to_dict('records'),
            'total': len(df)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/send', methods=['POST'])
def send():
    data = request.json
    session_id = data.get('session_id')
    if not session_id:
        return jsonify({'success': False, 'error': 'Немає session_id'})

    if session_id in all_sessions and all_sessions[session_id].get('running'):
        return jsonify({'success': False, 'error': 'Розсилка вже запущена в цій вкладці'})

    account_name = data.get('account')
    accounts = load_accounts()
    if account_name not in accounts:
        return jsonify({'success': False, 'error': 'Акаунт не знайдено'})

    try:
        df = read_google_sheet(data['sheet_url'])
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

    username_col = data['username_col']
    message_col = data['message_col']
    delay = int(data.get('delay', 20))

    contacts = df[[username_col, message_col]].dropna().to_dict('records')
    contacts = [c for c in contacts if str(c[username_col]).strip() and str(c[message_col]).strip()]

    all_sessions[session_id] = {
        'running': True,
        'total': len(contacts),
        'sent': 0,
        'errors': [],
        'current': '',
        'done': False,
        'account': account_name
    }

    account = accounts[account_name]

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_send_all(session_id, account, contacts, username_col, message_col, delay))
        loop.close()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return jsonify({'success': True})

async def _send_all(session_id, account, contacts, username_col, message_col, delay):
    session_file = os.path.join(SESSIONS_DIR, account['session'])
    client = TelegramClient(session_file, int(account['api_id']), account['api_hash'])
    try:
        await client.connect()
        for contact in contacts:
            if not all_sessions.get(session_id, {}).get('running'):
                break
            username = str(contact[username_col]).strip().lstrip('@')
            message = str(contact[message_col]).strip()
            all_sessions[session_id]['current'] = f'@{username}'
            try:
                await client.send_message(username, message)
                all_sessions[session_id]['sent'] += 1
            except Exception as e:
                all_sessions[session_id]['errors'].append(f'@{username}: {str(e)}')
            await asyncio.sleep(delay)
    except Exception as e:
        all_sessions[session_id]['errors'].append(f'Критична помилка: {str(e)}')
    finally:
        all_sessions[session_id]['running'] = False
        all_sessions[session_id]['done'] = True
        all_sessions[session_id]['current'] = ''
        await client.disconnect()

@app.route('/status/<session_id>')
def get_status(session_id):
    if session_id not in all_sessions:
        return jsonify({'running': False, 'total': 0, 'sent': 0, 'errors': [], 'current': '', 'done': False})
    return jsonify(all_sessions[session_id])

@app.route('/stop/<session_id>', methods=['POST'])
def stop(session_id):
    if session_id in all_sessions:
        all_sessions[session_id]['running'] = False
    return jsonify({'success': True})

# ─── Account Management ──────────────────────────────────────────────────────

@app.route('/accounts')
def get_accounts():
    return jsonify(list(load_accounts().keys()))

@app.route('/accounts/add/start', methods=['POST'])
def add_account_start():
    data = request.json
    name = data.get('name', '').strip()
    api_id = data.get('api_id', '').strip()
    api_hash = data.get('api_hash', '').strip()
    phone = data.get('phone', '').strip()

    if not all([name, api_id, api_hash, phone]):
        return jsonify({'success': False, 'error': 'Заповніть усі поля'})

    session_name = f"acc_{name.replace(' ', '_')}"
    session_file = os.path.join(SESSIONS_DIR, session_name)

    pending_auth[name] = {
        'api_id': int(api_id),
        'api_hash': api_hash,
        'phone': phone,
        'session': session_name,
        'phone_code_hash': None
    }

    result = {'success': False, 'error': ''}

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_request_code(name, session_file, int(api_id), api_hash, phone, result))
        loop.close()

    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=30)
    return jsonify(result)

async def _request_code(name, session_file, api_id, api_hash, phone, result):
    client = TelegramClient(session_file, api_id, api_hash)
    try:
        await client.connect()
        authorized = await client.is_user_authorized()
        if authorized:
            result['success'] = True
            result['already_authorized'] = True
        else:
            r = await client.send_code_request(phone)
            pending_auth[name]['phone_code_hash'] = r.phone_code_hash
            result['success'] = True
    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
    finally:
        await client.disconnect()

@app.route('/accounts/add/verify', methods=['POST'])
def add_account_verify():
    data = request.json
    name = data.get('name')
    code = data.get('code', '').strip()
    password = data.get('password', '').strip()

    if name not in pending_auth:
        return jsonify({'success': False, 'error': 'Сесія авторизації не знайдена.'})

    auth = pending_auth[name]
    result = {'success': False, 'error': '', 'need_password': False}

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_verify_code(name, auth, code, password, result))
        loop.close()

    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=30)

    if result.get('success'):
        accounts = load_accounts()
        accounts[name] = {
            'api_id': auth['api_id'],
            'api_hash': auth['api_hash'],
            'phone': auth['phone'],
            'session': auth['session']
        }
        save_accounts(accounts)
        if name in pending_auth:
            del pending_auth[name]

    return jsonify(result)

async def _verify_code(name, auth, code, password, result):
    session_file = os.path.join(SESSIONS_DIR, auth['session'])
    client = TelegramClient(session_file, auth['api_id'], auth['api_hash'])
    try:
        await client.connect()
        if auth.get('already_authorized') or await client.is_user_authorized():
            result['success'] = True
            return
        await client.sign_in(auth['phone'], code, phone_code_hash=auth['phone_code_hash'])
        result['success'] = True
    except SessionPasswordNeededError:
        if password:
            try:
                await client.sign_in(password=password)
                result['success'] = True
            except Exception as e:
                result['error'] = str(e)
        else:
            result['need_password'] = True
            result['error'] = 'Потрібен пароль 2FA'
    except PhoneCodeInvalidError:
        result['error'] = 'Невірний код. Спробуйте ще раз.'
    except Exception as e:
        result['error'] = str(e)
    finally:
        await client.disconnect()

@app.route('/accounts/delete', methods=['POST'])
def delete_account():
    data = request.json
    name = data.get('name')
    accounts = load_accounts()
    if name in accounts:
        session_base = os.path.join(SESSIONS_DIR, accounts[name]['session'])
        for suffix in ['.session', '.session-journal', '.session-shm', '.session-wal']:
            f = session_base + suffix
            if os.path.exists(f):
                try: os.remove(f)
                except: pass
        del accounts[name]
        save_accounts(accounts)
    if name in pending_auth:
        del pending_auth[name]
    return jsonify({'success': True})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
