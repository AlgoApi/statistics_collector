import bisect
import json
import os
import time
import sys
import threading
import logging
import io
import asyncio
import traceback
from datetime import datetime, timedelta
from datetime import date as DateType
from typing import Sequence, Dict, Any, Optional
import requests
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy import Integer, String, DateTime, Date, or_, desc, NullPool
from sqlalchemy.orm import declarative_base, sessionmaker, mapped_column, Mapped
from flask import Flask, request, jsonify, render_template_string
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from pyrogram.enums import ChatType
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import matplotlib
import matplotlib.pyplot as plt
from pytz import timezone
from werkzeug.datastructures import Headers
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy import select, func
from sqlalchemy import text as text_sql
from sqlalchemy.orm import aliased
from common import flood_retry, safe_send, TelegramAccumulator
from asgiref.wsgi import WsgiToAsgi
from dotenv import load_dotenv
load_dotenv()

matplotlib.use('Agg')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
BEARER_STATIC = os.getenv("BEARER_STATIC")
USERNAMES_REPORTED_EXCLUDE = ["Boobsmarley_assistant_bot", "Boobsmarley", "AlgoApiBot", "drippineveryday", "AlgoApi", "internetrastroystvo", "hehsike"]
ADMIN_USERNAMES = ["Boobsmarley", "AlgoApi", "drippineveryday", "internetrastroystvo"]

proxy_url:str

def load_session_config(phone: str, informal_contact: bool = True) -> Optional[Dict[str, Any]]:
    path = os.path.join('.', f'{phone}.json' if informal_contact else f'formal_contact/{phone}.json')
    if not os.path.exists(path):
        logger.error(f"Файл конфигурации {path} не найден!")
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка чтения {path}: {e}")
        return None

def build_socks5_proxy_url(config) -> str|None:
    proxy = config.get("proxy")
    if not proxy or not isinstance(proxy, list):
        logger.error("cannot find proxy")
        return None

    ptype = (config.get("proxy_type") or proxy[0]).lower()

    if len(proxy) > 4:
        return f"{ptype}://{proxy[4]}:{proxy[5]}@{proxy[1]}:{int(proxy[2])}"
    return f"{ptype}://{proxy[1]}:{int(proxy[2])}"

config = load_session_config("bob_stat_collector")
proxy_url = build_socks5_proxy_url(config)

def send_error_to_admin(err_text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": ADMIN_ID,
            "text": f"⚠️ **CRITICAL ERROR**\n\n`{err_text[-4000:]}`",
        }
        proxies = None
        if proxy_url:
            proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
        response = requests.post(url, json=payload, timeout=10, proxies=proxies)
        if response.status_code >= 400:
            print(f"Could not send error to admin: {response.status_code} {response.text}", file=sys.stderr)
    except Exception as e:
        print(f"Could not send error to admin: {e}", file=sys.stderr)

class TelegramErrorTableHandler(logging.Handler):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._local = threading.local()

    def emit(self, record):
        if getattr(self._local, "sending", False):
            return
        self._local.sending = True
        try:
            log_entry = self.format(record)
            send_error_to_admin(f"Уровень: {record.levelname}\n{log_entry}")
        except Exception:
            import traceback, sys
            traceback.print_exc(file=sys.stderr)
        finally:
            self._local.sending = False

def global_exception_handler(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    err_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    logger.critical(f"Uncaught exception: {err_msg}")
    send_error_to_admin(err_msg)

# Вывод в консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Вывод админу в ТГ (только ошибки)
tg_handler = TelegramErrorTableHandler()
tg_handler.setLevel(logging.ERROR)
tg_handler.setFormatter(formatter)
logger.addHandler(tg_handler)

sys.excepthook = global_exception_handler
threading.excepthook = lambda args: global_exception_handler(args.exc_type, args.exc_value, args.exc_traceback)

DB_URI = os.getenv("DB_URI", f"mysql+asyncmy://${os.getenv('DB_USER')}:${os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:3306/${os.getenv('DB_NAME')}")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
ADMIN_CHANNEL_ID = int(os.getenv("ADMIN_CHANNEL_ID"))
CHANNEL_MESSAGE_ID = os.getenv("CHANNEL_MESSAGE_ID")
URL_FORM = os.getenv("FORM_URL")
if CHANNEL_MESSAGE_ID and CHANNEL_MESSAGE_ID != "":
    CHANNEL_MESSAGE_ID = int(CHANNEL_MESSAGE_ID)
else:
    CHANNEL_MESSAGE_ID=None
WEBAPP_URL = os.getenv("WEBAPP_URL")

# Список параметров для статистики
INT_FIELDS = [
    "int_agent_registered", "int_model_registered",
    "int_all_agent_registered", "int_all_model_registered",
    "int_lid_accepted", "int_lid_active", "int_lid_rejected", "int_lid_sum"
]

checkpoints_field = [
    "int_lid_sum"
]

checkpoints_value = {
    "int_lid_sum": [30, 60, 70, 90, 140, 180, 220]
}

def merge_find(data: list[int], target: int) -> int|None:
    i = bisect.bisect_left(data, target)
    return data[i - 1] if i > 0 else None

TRANSLATED_INT_FIELDS = {"int_agent_registered": "Агентов записано сегодня", "int_model_registered": "Моделей записано сегодня",
                         "int_all_agent_registered": "ВСЕГО Агентов записано", "int_all_model_registered": "ВСЕГО Моделей записано",
                         "int_lid_accepted": "Согласились работать", "int_lid_active": "Активные диалоги",
                         "int_lid_sum": "Вобщем отписанных", "int_lid_rejected": "Отказались"}

# --- БАЗА ДАННЫХ ---
Base = declarative_base()


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    telegram_user: Mapped[str] = mapped_column(String(100))
    text_conclusions: Mapped[str] = mapped_column(String(1000))
    text_difficult: Mapped[str] = mapped_column(String(1000))

    int_agent_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_all_agent_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_all_model_active: Mapped[int] = mapped_column(Integer, default=0)
    int_all_model_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_accepted: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_active: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_rejected: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_sum: Mapped[int] = mapped_column(Integer, default=0)
    int_model_registered: Mapped[int] = mapped_column(Integer, default=0)

    def to_dict(self):
        data = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # NOTE DetachedInstanceError if lazy loading used
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()

        return data


class DailyStats(Base):
    __tablename__ = "daily_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[DateType] = mapped_column(Date, unique=True)
    total_reports: Mapped[int] = mapped_column(Integer)
    total_lids: Mapped[int] = mapped_column(Integer)
    total_lids_rejected: Mapped[int] = mapped_column(Integer)
    total_model_registered: Mapped[int] = mapped_column(Integer)
    total_agent_registered: Mapped[int] = mapped_column(Integer)


engine: AsyncEngine = create_async_engine(
    DB_URI,
    poolclass=NullPool,
    future=True,
    echo=False
)

# фабрика асинхронных сессий
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def wait_for_db_and_create(
    engine: AsyncEngine,
    retries: int = 10,
    delay: float = 3.0,
    retry_sql: str = "SELECT 1"
) -> None:
    for attempt in range(1, retries + 1):
        try:
            logger.info("Attempt %s to connect to DB", attempt)
            # пробуем открыть асинхронное соединение и выполнить простой запрос
            async with engine.connect() as conn:
                await conn.execute(retry_sql)
            # создаём таблицы в транзакции (run_sync выносит вызов в sync контекст)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("DB ready and metadata created")
            return
        except OperationalError as e:
            logger.warning("DB not ready (%s). retrying in %ss...", e, delay)
            await asyncio.sleep(delay)
        except Exception as e:
            # на случай других ошибок — логируем и повторяем попытки
            logger.exception("Unexpected error while connecting to DB: %s", e)
            await asyncio.sleep(delay)

    raise RuntimeError("Could not connect to DB after %d retries" % retries)

# --- FLASK СЕРВЕР ---
app = Flask(__name__)

@app.route('/stat', methods=['GET'], strict_slashes=False)
async def get_data():
    data = request.json
    headers: Headers
    headers = request.headers
    if not data: return jsonify({"error": "No data"}), 400
    if not headers: return jsonify({"error": "No headers"}), 400
    auth_header = headers.get('Authorization')
    if not auth_header:
        return jsonify({"error": f"auth"}), 401
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != 'bearer':
        return jsonify({"error": f"auth"}), 401
    if parts[1] != BEARER_STATIC:
        return jsonify({"error": f"auth"}), 403

    telegram_user = data.get('telegram_user')
    try:
        count = int(data.get('count'))
    except TypeError:
        return jsonify({"error": "Invalid data"}), 400
    if telegram_user is None or count is None:
        return jsonify({"error": "No data"}), 400

    async with AsyncSessionLocal() as session:
        try:
            stmt = (
                select(Report)
                .where(Report.telegram_user == telegram_user)
                .order_by(desc(Report.created_at))
                .limit(count)
            )
            result = await session.execute(stmt)
            records = result.scalars().all()
            if records:
                records_dict = [r.to_dict() for r in records]
                return jsonify({"data": records_dict}, 200)
            return jsonify({'error': 'Not found'}), 404
        except SQLAlchemyError as e:
            return jsonify({"error": f"error getting data: {e}"}), 500

@app.route('/bob_stat', methods=['POST'], strict_slashes=False)
async def webhook():
    data = request.json
    headers: Headers
    headers = request.headers
    if not data: return jsonify({"error": "No data"}), 400
    if not headers: return jsonify({"error": "No headers"}), 400
    try:
        if data.get('telegram_user') is None or data.get('int_lid-sum') is None:
            logger.error(f"invalid data: {data}")
            return jsonify({"error": "invalid data"}), 400
        ref = headers.get('Reference') or headers.get('Referer') or headers.get('Host')
        if ref is not None and "tamelaos.fun" not in ref:
            logger.error(f"invalid headers: {dict(headers)}")
            return jsonify({"error": "invalid headers"}), 400
        async with AsyncSessionLocal() as session:
            async with session.begin():
                new_report = Report(
                    telegram_user=data.get('telegram_user', 'Unknown'),
                    text_conclusions=data.get('text_conclusions', ''),
                    text_difficult=data.get('text_difficult', ''),
                    int_agent_registered=int(float(data.get('int_agent_registered', 0))),
                    int_all_agent_registered=int(float(data.get('int_all_agent_registered', 0))),
                    int_all_model_active=int(float(data.get('int_all_model_active', 0))),
                    int_all_model_registered=int(float(data.get('int_all_model_registered', 0))),
                    int_lid_accepted=int(float(data.get('int_lid-accepted', 0))),
                    int_lid_active=int(float(data.get('int_lid-active', 0))),
                    int_lid_rejected=int(float(data.get('int_lid-rejected', 0))),
                    int_lid_sum=int(float(data.get('int_lid-sum', 0))),
                    int_model_registered=int(float(data.get('int_model_registered', 0)))
                )
                session.add(new_report)
            await session.close()

        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/bob_stat/webapp', strict_slashes=False)
def webapp_page():
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
        <script src="https://telegram.org/js/telegram-web-app.js"></script>
        <style>
            :root {
                --bg-color: var(--tg-theme-bg-color, #ffffff);
                --text-color: var(--tg-theme-text-color, #000000);
                --hint-color: var(--tg-theme-hint-color, #999999);
            }
            body { 
                margin: 0; 
                padding: 10px; 
                background-color: var(--bg-color); 
                color: var(--text-color);
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            }
            .header {
                text-align: center;
                font-size: 20px;
                font-weight: bold;
                margin-bottom: 15px;
                color: var(--text-color);
            }
            .form-wrapper {
                flex-grow: 1;
                border: 2px solid var(--hint-color); /* Цвет рамки подстраивается под тему ТГ */
                border-radius: 12px;
                overflow: hidden; /* Чтобы углы iframe тоже казались скругленными */
                background: #ffffff; /* Белый фон для самой формы */
                height: calc(100vh - 70px); /* Вычитаем высоту заголовка и отступов */
                -webkit-overflow-scrolling: touch;
                overflow-y: auto;
            }
            .footer {
                text-align: center;
                padding: 10px 0;
                font-size: 13px;
                color: var(--hint-color);
                border-top: 1px solid rgba(0,0,0,0.1);
            }
            .footer a {
                color: var(--link-color);
                text-decoration: none;
            }
            #error-message { display: none; text-align: center; padding-top: 50px; }
            iframe { border: none; width: 100%; height: 100%; display: block; }
        </style>
    </head>
    <body>
        <div class="header">📊 Отчёт "Основа"</div>
        <div id="error-message">
            <h2>Доступ запрещен</h2>
            <p>Пожалуйста, используйте официального бота.</p>
        </div>

        <div class="form-wrapper">
            <iframe id="google-frame" frameborder="0"></iframe>
        </div>
        
        <div class="footer">
            Разработка и поддержка: <a href="https://t.me/AlgoApi">@AlgoApi aka Tamelaos</a>
        </div>

        <script>
            const webapp = window.Telegram.WebApp;
            webapp.expand();
            webapp.ready();

            const user = webapp.initDataUnsafe.user;
            const frame = document.getElementById('google-frame');
            const errorDiv = document.getElementById('error-message');

            if (user && (user.id || user.username)) {
                const userInfo = user.username ? (user.username) : ("FirstName:" + user.first_name + " | ID: " + user.id);

                const baseUrl = "{{URL_FORM}}";
                const entryId = "entry.1132010469";
                const finalUrl = baseUrl + "&" + entryId + "=" + encodeURIComponent(userInfo);

                frame.src = finalUrl;
                frame.style.display = "block";
            } else {
                errorDiv.style.display = "block";
                document.querySelector('.form-wrapper').style.display = "none";
                document.querySelector('.header').style.display = "none";
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template, URL_FORM=URL_FORM)
asgi_app = WsgiToAsgi(app)
# --- БОТ ---
def get_bot_proxy_pyrogram(config: Dict[str, Any]) -> Optional[dict]:

    proxy = config.get("proxy")
    if not proxy or not isinstance(proxy, list):
        return None

    ptype = (config.get("proxy_type") or proxy[0]).lower()

    return {
        "scheme": ptype,
        "hostname": proxy[1],
        "port": int(proxy[2]),
        "username": proxy[4] if len(proxy) > 4 else None,
        "password": proxy[5] if len(proxy) > 5 else None,
    }

pyro_proxy = get_bot_proxy_pyrogram(config)

bot = Client("bob_stat_collector", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, proxy=pyro_proxy)

@bot.on_message(filters.command("start"))
async def start_cmd(c, m):
    if m.chat.type == ChatType.PRIVATE:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📝 Сдать отчёт", web_app=WebAppInfo(url=WEBAPP_URL))
        ]])
        await m.reply("Mentor - @Boobsmarley\nSupport - @internetrastroystvo\nDeveloper - @AlgoApi\nКнопки доступны и активны всегда, можно просто тыкнуть по кнопке, по которой уже тыкали", reply_markup=kb)
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🤖 Открыть в ЛС", url=f"https://t.me/{c.me.username}")
        ]])
        await m.reply(
            "В группах функции отчёта недоступны. Пожалуйста, нажмите кнопку ниже, чтобы перейти в личные сообщения с ботом.",
            reply_markup=kb
        )


# Команда принудительной отправки
@bot.on_message(filters.command("force_report") & filters.user(ADMIN_USERNAMES))
async def force_cmd(c, m):
    await m.reply("Запускаю формирование отчетов вручную...")
    await send_daily_reports()
    await m.reply("Принудительная отправка завершена.")


@bot.on_message(filters.command("graph") & filters.user(ADMIN_USERNAMES))
async def graph_cmd(c, m):
    try:
        async with AsyncSessionLocal() as session:
            stmt = select(DailyStats).order_by(DailyStats.date.desc()).limit(7)
            result = await session.execute(stmt)
            stats = result.scalars().all()

        stats.reverse()
        if not stats:
            return await m.reply("Нет данных.")

        dates = [s.date.strftime('%d.%m') for s in stats]
        total_lids = [s.total_lids for s in stats]
        total_lids_rejected = [s.total_lids_rejected for s in stats]
        total_model_registered = [s.total_model_registered for s in stats]
        total_agent_registered = [s.total_agent_registered for s in stats]

        plt.figure(figsize=(8, 4))
        plt.plot(dates, total_lids, marker='o', label="Вобщем отписанных")
        plt.plot(dates, total_lids_rejected, marker='v', label="Отказов")
        plt.plot(dates, total_model_registered, marker='P', label="Моделей записано")
        plt.plot(dates, total_agent_registered, marker='s', label="Агентов записано")
        plt.xlabel("Дата")
        plt.ylabel("Значение")
        plt.title('Сумма лидов за неделю')
        plt.legend()
        plt.grid()
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()

        total_registered = sum(total_model_registered) + sum(total_agent_registered)
        sum_total_lids = sum(total_lids)
        if sum_total_lids > 0:
            registration_percent = (total_registered / sum_total_lids) * 100
        else:
            registration_percent = 0.0

        await m.reply_photo(buf, caption=f"📈 Общая конверсия: {registration_percent:.2f}%")
    except Exception:
        logger.exception("Ошибка при формировании графика")
        await m.reply("Ошибка при формировании графика. Посмотрите логи.")


@bot.on_message(filters.command("whoami"))
async def whoami(client: Client, message):
    await message.reply_text(f"CHAT -> id: {message.chat.id} type: {message.chat.type} title: {getattr(message.chat, 'title', None)}, reply_to_message_id: {message.reply_to_message_id}")
    try:
        await message.reply_text(f"video_id: {message.video.file_id}")
    except AttributeError:
        pass
    try:
        await message.reply_text(f"photo_id: {message.photo.file_id}")
    except AttributeError:
        pass
    try:
        await message.reply_text(f"gif_id: {message.animation.file_id}")
    except AttributeError:
        pass


# --- ЛОГИКА ОТЧЕТОВ ---
async def send_daily_reports():
    try:
        await bot.get_chat(ADMIN_CHANNEL_ID)
        moscow_now = datetime.now(timezone('Europe/Moscow'))
        today = moscow_now.date()
        end_time = moscow_now # .replace(hour=23, minute=59, second=0, microsecond=0)
        start_time = end_time - timedelta(hours=int(os.getenv("HOURS_PERIOD_REPORT", 24)))
        logger.info(f"Сбор отчетов за период: {start_time} - {end_time}")
        reports = []
        async with AsyncSessionLocal() as session:
            subq = (
                select(
                    Report,
                    func.lead(Report.created_at)
                    .over(
                        partition_by=Report.telegram_user,
                        order_by=Report.created_at.asc()
                    )
                    .label("next_created"),
                )
                .where(
                    Report.created_at >= start_time,
                    Report.created_at < end_time,
                )
            ).subquery()

            report_alias = aliased(Report, subq)
            cond_keep = or_(
                subq.c.next_created == None,
                func.timestampdiff(text_sql("SECOND"), subq.c.created_at, subq.c.next_created) > 300
            )

            stmt = select(report_alias).where(cond_keep)

            reports: Sequence[Report] = (await session.scalars(stmt)).all()

        if not reports:
            await bot.send_message(ADMIN_CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=f"📅 {today}: Отчетов нет.")
            return

        usernames_reported = list()
        checkpointed_reports: dict[str, tuple[str, int, int]] = dict()
        acc = TelegramAccumulator(bot, chat_id=ADMIN_CHANNEL_ID, send_delay=0.3)
        for r in reports:
            text = (f"📄 **Отчет #{r.id}** ({r.created_at.strftime('%d/%m/%Y, %H:%M:%S')})\n"
                    f"👤 Сотрудник: **@{r.telegram_user}**\n"
                    f"Сложности: {r.text_difficult}\n"
                    f"Выводы: {r.text_conclusions}\n"
                    f"- - - - - - - - - - - - - - -")
            for field in INT_FIELDS:
                field_data = int(getattr(r, field))
                field_name = TRANSLATED_INT_FIELDS.get(field, "undefined")
                if field in checkpoints_field:
                    checkpoint = merge_find(checkpoints_value[field], field_data)
                    if checkpoint:
                        user_val = getattr(r, "telegram_user", None)
                        if user_val is None:
                            continue
                        if not isinstance(user_val, str):
                            user_val = str(user_val)
                        text += f"\n{field_name}: {field_data} | Преодолел чекпоинт 🎉🎉 {checkpoint} 🎉🎉"
                        checkpointed_reports.update({user_val: (field_name, checkpoint, field_data)})
                    else:
                        text += f"\n{field_name}: {field_data}"
                else:
                    text += f"\n{field_name}: {field_data}"
            #await safe_send(bot, ADMIN_CHANNEL_ID, text, reply_to_message_id=CHANNEL_MESSAGE_ID)
            text += "\n\n"
            await acc.add(text, reply_to_message_id=CHANNEL_MESSAGE_ID)
            # await bot.send_message(ADMIN_CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=text)
            usernames_reported.append(r.telegram_user)
            await asyncio.sleep(0.3)

        await acc.flush(reply_to_message_id=CHANNEL_MESSAGE_ID)

        unreported_text = "❌ Не отправили отчёты вовремя (INT) сотрудников(ка): \n"
        unreported_exists = 0
        async for member in bot.get_chat_members(CHANNEL_ID):
            if member.user.id not in usernames_reported and member.user.username not in usernames_reported:
                if member.user.id not in USERNAMES_REPORTED_EXCLUDE and member.user.username not in USERNAMES_REPORTED_EXCLUDE:
                    if member.user.username:
                        unreported_text += f" L @{member.user.username}\n"
                    else:
                        unreported_text += f" L @{member.user.id}\n"
                    unreported_exists += 1
        if unreported_exists > 0:
            await asyncio.sleep(0.3)
            unreported_text = unreported_text.replace("(INT)", f"{unreported_exists}")
            await safe_send(bot, ADMIN_CHANNEL_ID, unreported_text, reply_to_message_id=CHANNEL_MESSAGE_ID)

        await asyncio.sleep(0.3)
        summary = f"📊 **ИТОГИ ДНЯ {today}**\nВсего отчетов: {len(reports)}\n"
        daily_lids_sum = 0
        daily_lids_rejected_sum = 0
        daily_agent_registered_sum = 0
        daily_model_registered_sum = 0
        for field in INT_FIELDS:
            values = [getattr(r, field) for r in reports]
            f_min = min(values)
            f_max = max(values)
            f_avg = sum(values) / len(values)

            if field == "int_lid_sum": daily_lids_sum = sum(values)
            if field == "int_lid_rejected": daily_lids_rejected_sum = sum(values)
            if field == "int_agent_registered": daily_agent_registered_sum = sum(values)
            if field == "int_model_registered": daily_model_registered_sum = sum(values)

            summary += (f"\n🔹 **{TRANSLATED_INT_FIELDS.get(field, 'undefined')}**:\n"
                        f"   L Максимум: {f_max} | Среднее: {f_avg:.1f} | Минимум: {f_min}")

        total_registered = daily_model_registered_sum + daily_agent_registered_sum
        if daily_lids_sum > 0:
            registration_percent = (total_registered / daily_lids_sum) * 100
        else:
            registration_percent = 0.0
        summary += f"\n📈 Общая конверсия: {registration_percent:.2f}%\n"
        await safe_send(bot, ADMIN_CHANNEL_ID, summary, reply_to_message_id=CHANNEL_MESSAGE_ID)

        async with AsyncSessionLocal() as session:
            stmt = select(DailyStats).where(DailyStats.date == today)
            result = await session.execute(stmt)
            ds = result.scalars().first()

            if not ds:
                ds = DailyStats(date=today)

            ds.total_reports = len(reports)
            ds.total_lids = daily_lids_sum
            ds.total_lids_rejected = daily_lids_rejected_sum
            ds.total_model_registered = daily_model_registered_sum
            ds.total_agent_registered = daily_agent_registered_sum
            session.add(ds)
            await session.commit()

        checkpoint_summary_exists = 0
        checkpoint_summary = "⭐ Сотрудники прошедшие этап ((INT)): \n"
        for checkpointed_report, checkpoint_data in checkpointed_reports.items():
            checkpoint_name: str
            checkpoint_value: int
            current_value: int
            checkpoint_name, checkpoint_value, current_value = checkpoint_data
            checkpoint_summary += f"  L @{checkpointed_report} \n    прошёл этап: {checkpoint_name}\n     нужно было: {checkpoint_value}, набрал {current_value}\n\n"
            checkpoint_summary_exists += 1
        if checkpoint_summary_exists > 0:
            await asyncio.sleep(0.3)
            checkpoint_summary = checkpoint_summary.replace("(INT)", f"{checkpoint_summary_exists}")
            await safe_send(bot, ADMIN_CHANNEL_ID, checkpoint_summary, reply_to_message_id=CHANNEL_MESSAGE_ID)

    except Exception as e:
        logger.error(f"Report Error: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--api":
        logger.info("Запуск Flask API...")
        app.run(host='0.0.0.0', port=5000)
    else:
        logger.info("Подготовка бота...")
        async def setup_and_run():
            moscow_tz = timezone("Europe/Moscow")
            scheduler = AsyncIOScheduler(timezone=moscow_tz)
            scheduler.add_job(send_daily_reports, 'cron', hour=23, minute=59)
            scheduler.start()
            logger.info("Планировщик запущен на 23:59 МСК")

            await bot.start()
            logger.info("Бот запущен и ожидает сообщений...")
            await idle()
            await bot.stop()

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(setup_and_run())
        except KeyboardInterrupt:
            pass
