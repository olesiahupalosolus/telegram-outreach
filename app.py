import os
import uuid
import threading
import time
import asyncio
import io
import secrets
import requests
import pandas as pd
from datetime import datetime
from functools import wraps

from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, flash, abort
)
from flask_sqlalchemy import SQLAlchemy
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    login_required, current_user
)
from werkzeug.security import generate_password_hash, check_password_hash
from email_validator import validate_email, EmailNotValidError

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# --- Налаштування шляхів ---
base_dir = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__,
            template_folder=os.path.join(base_dir, 'templates'),
            static_folder=os.path.join(base_dir, 'static'))

# ─── SECRET_KEY ─────────────────────────────────────────────────────────────
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

# ─── Database ───────────────────────────────────────────────────────────────
# Railway автоматично інжектить DATABASE_URL коли підключений Postgres-сервіс.
# Локально fallback на SQLite файл.
db_url = os.environ.get('DATABASE_URL', f"sqlite:///{os.path.join(base_dir, 'app.db')}")
# Railway/Heroku style 'postgres://' → 'postgresql://' для SQLAlchemy
if db_url.startswith('postgres://'):
    db_url = db_url.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# ─── Login Manager ──────────────────────────────────────────────────────────
login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Будь ласка, увійдіть в систему.'

# ─── Models ─────────────────────────────────────────────────────────────────

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(120), nullable=True)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    accounts = db.relationship('TelegramAccount', backref='user', lazy=True, cascade='all, delete-orphan')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class InviteCode(db.Model):
    __tablename__ = 'invite_codes'
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(32), unique=True, nullable=False, index=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    used_by_user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    used_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    created_by = db.relationship('User', foreign_keys=[created_by_user_id])
    used_by = db.relationship('User', foreign_keys=[used_by_user_id])

    @property
    def is_used(self):
        return self.used_by_user_id is not None


class TelegramAccount(db.Model):
    __tablename__ = 'telegram_accounts'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    name = db.Column(db.String(120), nullable=False)
    api_id = db.Column(db.Integer, nullable=False)
    api_hash = db.Column(db.String(255), nullable=False)
    phone = db.Column(db.String(32), nullable=False)
    session_string = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (db.UniqueConstraint('user_id', 'name', name='uq_user_account_name'),)


with app.app_context():
    db.create_all()


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


# ─── In-memory state ────────────────────────────────────────────────────────
# Розсилки: ключ — session_id браузерної вкладки
all_sessions = {}
# Pending Telegram auth: ключ — (user_id, account_name)
pending_auth = {}


# ─── Helpers ────────────────────────────────────────────────────────────────

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            abort(403)
        return f(*args, **kwargs)
    return decorated


def generate_invite_code():
    return secrets.token_urlsafe(6)[:8]


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


def get_user_account(user_id, name):
    return TelegramAccount.query.filter_by(user_id=user_id, name=name).first()


# ─── Auth Routes ────────────────────────────────────────────────────────────

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''

        if not email or not password:
            flash('Введіть email та пароль', 'error')
            return render_template('login.html', email=email)

        user = User.query.filter_by(email=email).first()
        if not user or not user.check_password(password):
            flash('Невірний email або пароль', 'error')
            return render_template('login.html', email=email)

        login_user(user, remember=True)
        next_url = request.args.get('next')
        return redirect(next_url or url_for('index'))

    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    # Якщо в базі немає жодного юзера — реєстрація відкрита, перший стає адміном
    user_count = User.query.count()
    bootstrap_mode = (user_count == 0)

    invite_code_param = request.args.get('code', '').strip()

    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password') or ''
        password2 = request.form.get('password2') or ''
        name = (request.form.get('name') or '').strip()
        code = (request.form.get('code') or '').strip()

        # Валідація email
        try:
            v = validate_email(email, check_deliverability=False)
            email = v.normalized
        except EmailNotValidError as e:
            flash(f'Невірний email: {e}', 'error')
            return render_template('register.html', email=email, name=name,
                                   code=code, bootstrap=bootstrap_mode)

        if len(password) < 8:
            flash('Пароль має бути мінімум 8 символів', 'error')
            return render_template('register.html', email=email, name=name,
                                   code=code, bootstrap=bootstrap_mode)
        if password != password2:
            flash('Паролі не співпадають', 'error')
            return render_template('register.html', email=email, name=name,
                                   code=code, bootstrap=bootstrap_mode)

        if User.query.filter_by(email=email).first():
            flash('Користувач з таким email вже існує', 'error')
            return render_template('register.html', email=email, name=name,
                                   code=code, bootstrap=bootstrap_mode)

        invite = None
        if not bootstrap_mode:
            if not code:
                flash('Потрібен код запрошення', 'error')
                return render_template('register.html', email=email, name=name,
                                       code=code, bootstrap=False)
            invite = InviteCode.query.filter_by(code=code).first()
            if not invite:
                flash('Невірний код запрошення', 'error')
                return render_template('register.html', email=email, name=name,
                                       code=code, bootstrap=False)
            if invite.is_used:
                flash('Цей код вже використано', 'error')
                return render_template('register.html', email=email, name=name,
                                       code=code, bootstrap=False)

        # Створюємо юзера
        user = User(email=email, name=name or None, is_admin=bootstrap_mode)
        user.set_password(password)
        db.session.add(user)
        db.session.flush()

        if invite:
            invite.used_by_user_id = user.id
            invite.used_at = datetime.utcnow()

        db.session.commit()

        login_user(user, remember=True)
        flash('Реєстрація успішна! Ласкаво просимо.', 'success')
        return redirect(url_for('index'))

    return render_template('register.html', code=invite_code_param, bootstrap=bootstrap_mode)


@app.route('/logout', methods=['POST', 'GET'])
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


# ─── Admin: Invite Codes ────────────────────────────────────────────────────

@app.route('/admin/invites')
@login_required
@admin_required
def admin_invites():
    codes = InviteCode.query.order_by(InviteCode.created_at.desc()).all()
    return render_template('admin_invites.html', codes=codes)


@app.route('/admin/invites/create', methods=['POST'])
@login_required
@admin_required
def admin_invites_create():
    # Гарантуємо унікальність
    for _ in range(10):
        code = generate_invite_code()
        if not InviteCode.query.filter_by(code=code).first():
            break
    else:
        return jsonify({'success': False, 'error': 'Не вдалося згенерувати унікальний код'})

    invite = InviteCode(code=code, created_by_user_id=current_user.id)
    db.session.add(invite)
    db.session.commit()
    return jsonify({'success': True, 'code': code})


@app.route('/admin/invites/delete', methods=['POST'])
@login_required
@admin_required
def admin_invites_delete():
    code_id = request.json.get('id')
    invite = db.session.get(InviteCode, int(code_id))
    if invite and not invite.is_used:
        db.session.delete(invite)
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Код не знайдено або вже використано'})


# ─── Main Routes ────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    accounts = TelegramAccount.query.filter_by(user_id=current_user.id).order_by(TelegramAccount.created_at).all()
    return render_template('index.html',
                           accounts=[a.name for a in accounts],
                           user=current_user)


@app.route('/preview', methods=['POST'])
@login_required
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
@login_required
def send():
    data = request.json
    session_id = data.get('session_id')
    if not session_id:
        return jsonify({'success': False, 'error': 'Немає session_id'})

    if session_id in all_sessions and all_sessions[session_id].get('running'):
        return jsonify({'success': False, 'error': 'Розсилка вже запущена в цій вкладці'})

    account_name = data.get('account')
    account = get_user_account(current_user.id, account_name)
    if not account:
        return jsonify({'success': False, 'error': 'Акаунт не знайдено'})
    if not account.session_string:
        return jsonify({'success': False, 'error': 'Акаунт не авторизовано. Додайте його заново.'})

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
        'account': account_name,
        'user_id': current_user.id
    }

    account_data = {
        'api_id': account.api_id,
        'api_hash': account.api_hash,
        'session_string': account.session_string,
    }

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_send_all(session_id, account_data, contacts, username_col, message_col, delay))
        loop.close()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return jsonify({'success': True})


async def _send_all(session_id, account, contacts, username_col, message_col, delay):
    client = TelegramClient(StringSession(account['session_string']),
                            int(account['api_id']),
                            account['api_hash'])
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
        try:
            await client.disconnect()
        except Exception:
            pass


@app.route('/status/<session_id>')
@login_required
def get_status(session_id):
    s = all_sessions.get(session_id)
    if not s:
        return jsonify({'running': False, 'total': 0, 'sent': 0, 'errors': [], 'current': '', 'done': False})
    # Не показуємо чужі сесії
    if s.get('user_id') and s['user_id'] != current_user.id:
        return jsonify({'running': False, 'total': 0, 'sent': 0, 'errors': [], 'current': '', 'done': False})
    return jsonify(s)


@app.route('/stop/<session_id>', methods=['POST'])
@login_required
def stop(session_id):
    s = all_sessions.get(session_id)
    if s and s.get('user_id') == current_user.id:
        s['running'] = False
    return jsonify({'success': True})


# ─── Account Management ─────────────────────────────────────────────────────

@app.route('/accounts')
@login_required
def get_accounts():
    accounts = TelegramAccount.query.filter_by(user_id=current_user.id).order_by(TelegramAccount.created_at).all()
    return jsonify([a.name for a in accounts])


@app.route('/accounts/add/start', methods=['POST'])
@login_required
def add_account_start():
    data = request.json
    name = (data.get('name') or '').strip()
    api_id = (data.get('api_id') or '').strip()
    api_hash = (data.get('api_hash') or '').strip()
    phone = (data.get('phone') or '').strip()

    if not all([name, api_id, api_hash, phone]):
        return jsonify({'success': False, 'error': 'Заповніть усі поля'})

    # Унікальність назви в межах юзера
    if TelegramAccount.query.filter_by(user_id=current_user.id, name=name).first():
        return jsonify({'success': False, 'error': 'У вас вже є акаунт з такою назвою'})

    try:
        api_id_int = int(api_id)
    except ValueError:
        return jsonify({'success': False, 'error': 'API ID має бути числом'})

    pending_key = (current_user.id, name)
    pending_auth[pending_key] = {
        'api_id': api_id_int,
        'api_hash': api_hash,
        'phone': phone,
        'phone_code_hash': None,
        'session_string': None,
        'client': None,
    }

    result = {'success': False, 'error': ''}

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_request_code(pending_key, api_id_int, api_hash, phone, result))
        loop.close()

    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=30)
    return jsonify(result)


async def _request_code(pending_key, api_id, api_hash, phone, result):
    client = TelegramClient(StringSession(), api_id, api_hash)
    try:
        await client.connect()
        authorized = await client.is_user_authorized()
        if authorized:
            # Малоймовірно з порожньою StringSession, але про всяк випадок
            session_string = client.session.save()
            pending_auth[pending_key]['session_string'] = session_string
            result['success'] = True
            result['already_authorized'] = True
        else:
            r = await client.send_code_request(phone)
            pending_auth[pending_key]['phone_code_hash'] = r.phone_code_hash
            # Зберігаємо проміжний state клієнта у вигляді string-сесії,
            # щоб потім можна було відновити та продовжити sign_in.
            pending_auth[pending_key]['session_string'] = client.session.save()
            result['success'] = True
    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


@app.route('/accounts/add/verify', methods=['POST'])
@login_required
def add_account_verify():
    data = request.json
    name = (data.get('name') or '').strip()
    code = (data.get('code') or '').strip()
    password = (data.get('password') or '').strip()

    pending_key = (current_user.id, name)
    if pending_key not in pending_auth:
        return jsonify({'success': False, 'error': 'Сесія авторизації не знайдена.'})

    auth = pending_auth[pending_key]
    result = {'success': False, 'error': '', 'need_password': False}

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_verify_code(pending_key, auth, code, password, result))
        loop.close()

    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=30)

    if result.get('success'):
        # Зберігаємо акаунт у БД
        existing = get_user_account(current_user.id, name)
        if existing:
            existing.api_id = auth['api_id']
            existing.api_hash = auth['api_hash']
            existing.phone = auth['phone']
            existing.session_string = auth['session_string']
        else:
            acc = TelegramAccount(
                user_id=current_user.id,
                name=name,
                api_id=auth['api_id'],
                api_hash=auth['api_hash'],
                phone=auth['phone'],
                session_string=auth['session_string'],
            )
            db.session.add(acc)
        db.session.commit()
        pending_auth.pop(pending_key, None)

    return jsonify(result)


async def _verify_code(pending_key, auth, code, password, result):
    # Відновлюємо клієнт з проміжної StringSession (зберігає dc/auth_key
    # після send_code_request, що потрібно для sign_in).
    session_str = auth.get('session_string') or ''
    client = TelegramClient(StringSession(session_str), auth['api_id'], auth['api_hash'])
    try:
        await client.connect()
        if auth.get('already_authorized') or await client.is_user_authorized():
            auth['session_string'] = client.session.save()
            result['success'] = True
            return
        try:
            await client.sign_in(auth['phone'], code, phone_code_hash=auth['phone_code_hash'])
            auth['session_string'] = client.session.save()
            result['success'] = True
        except SessionPasswordNeededError:
            if password:
                try:
                    await client.sign_in(password=password)
                    auth['session_string'] = client.session.save()
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
        try:
            await client.disconnect()
        except Exception:
            pass


@app.route('/accounts/delete', methods=['POST'])
@login_required
def delete_account():
    data = request.json
    name = (data.get('name') or '').strip()
    account = get_user_account(current_user.id, name)
    if account:
        db.session.delete(account)
        db.session.commit()
    pending_auth.pop((current_user.id, name), None)
    return jsonify({'success': True})


# ─── Entrypoint ─────────────────────────────────────────────────────────────

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
