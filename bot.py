import bisect
import os
import sys
import threading
import logging
import io
import asyncio
import traceback
from datetime import datetime, timedelta
from datetime import date as DateType
from typing import Sequence
import requests
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Date, func, select
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session, mapped_column, Mapped
from flask import Flask, request, jsonify, render_template_string
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import matplotlib
import matplotlib.pyplot as plt
from pytz import timezone
from werkzeug.datastructures import Headers

matplotlib.use('Agg')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
USERNAMES_REPORTED_EXCLUDE = ["Boobsmarley_assistant_bot", "Boobsmarley", "AlgoApiBot", "drippineveryday"]

def send_error_to_admin(err_text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": ADMIN_ID,
            "text": f"⚠️ **CRITICAL ERROR**\n\n`{err_text[-4000:]}`",  # Ограничение TG
            "parse_mode": "Markdown"
        }
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code >= 400:
            logger.error(f"Could not send error to admin: {response.text}")
    except Exception as e:
        logger.error(f"Could not send error to admin: {e}")

class TelegramErrorTableHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        send_error_to_admin(f"Уровень: {record.levelname}\n{log_entry}")

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

DB_URI = os.getenv("DB_URI")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
CHANNEL_MESSAGE_ID = int(os.getenv("CHANNEL_MESSAGE_ID"))
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
                         "int_lid_sum": "Вобщем отписанных", "int_lid_rejected": "Согласились"}

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
    int_all_model_registered: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_accepted: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_active: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_rejected: Mapped[int] = mapped_column(Integer, default=0)
    int_lid_sum: Mapped[int] = mapped_column(Integer, default=0)
    int_model_registered: Mapped[int] = mapped_column(Integer, default=0)


class DailyStats(Base):
    __tablename__ = "daily_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[DateType] = mapped_column(Date, unique=True)
    total_reports: Mapped[int] = mapped_column(Integer)
    total_lids: Mapped[int] = mapped_column(Integer)
    total_lids_rejected: Mapped[int] = mapped_column(Integer)
    total_model_registered: Mapped[int] = mapped_column(Integer)
    total_agent_registered: Mapped[int] = mapped_column(Integer)


engine = create_engine(DB_URI, pool_recycle=3600)
SessionFactory = sessionmaker(bind=engine)
Session = scoped_session(SessionFactory)

# --- FLASK СЕРВЕР ---
app = Flask(__name__)


@app.route('/bob_stat', methods=['POST'])
def webhook():
    data = request.json
    headers: Headers
    headers = request.headers
    if not data: return jsonify({"error": "No data"}), 400
    if not headers: return jsonify({"error": "No headers"}), 400
    try:
        if data.get('telegram_user') is None or data.get('int_lid-sum') is None:
            logger.error(f"invalid data: {data}")
            return jsonify({"error": "invalid data"}), 400
        if headers.get('reference') is not None and headers.get('reference', {}).get('host') != "tamelaos.fun":
            logger.error(f"invalid headers: {headers}")
            return jsonify({"error": "invalid headers"}), 400
        session = Session()
        new_report = Report(
            telegram_user=data.get('telegram_user', 'Unknown'),
            text_conclusions=data.get('text_conclusions', ''),
            text_difficult=data.get('text_difficult', ''),
            int_agent_registered=int(data.get('int_agent_registered', 0)),
            int_all_agent_registered=int(data.get('int_all_agent_registered', 0)),
            int_all_model_registered=int(data.get('int_all_model_registered', 0)),
            int_lid_accepted=int(data.get('int_lid-accepted', 0)),
            int_lid_active=int(data.get('int_lid-active', 0)),
            int_lid_rejected=int(data.get('int_lid-rejected', 0)),
            int_lid_sum=int(data.get('int_lid-sum', 0)),
            int_model_registered=int(data.get('int_model_registered', 0))
        )
        session.add(new_report)
        session.commit()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        Session.remove()


@app.route('/bob_stat/webapp')
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
            <iframe id="ya-frame" frameborder="0"></iframe>
        </div>
        
        <div class="footer">
            Разработка и поддержка: <a href="https://t.me/AlgoApi">@AlgoApi aka Tamelaos</a>
        </div>

        <script>
            const webapp = window.Telegram.WebApp;
            webapp.expand();
            webapp.ready();

            const user = webapp.initDataUnsafe.user;
            const frame = document.getElementById('ya-frame');
            const errorDiv = document.getElementById('error-message');

            if (user && (user.id || user.username)) {
                const userInfo = user.username ? ("@" + user.username) : ("FirstName:" + user.first_name + " | ID: " + user.id);

                const baseUrl = "https://forms.yandex.ru/u/69935851505690fe69657291/?iframe=1";
                const finalUrl = baseUrl + "&telegram_user=" + encodeURIComponent(userInfo);

                frame.src = finalUrl;
                frame.style.display = "block";

                const script = document.createElement('script');
                script.src = "https://forms.yandex.ru/_static/embed.js";
                document.head.appendChild(script);
            } else {
                errorDiv.style.display = "block";
                document.querySelector('.form-wrapper').style.display = "none";
                document.querySelector('.header').style.display = "none";
            }
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template)

# --- БОТ ---
bot = Client("bob_stat_collector", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@bot.on_message(filters.command("start"))
async def start_cmd(c, m):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📝 Открыть форму", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await m.reply("Бот готов к работе.", reply_markup=kb)


# Команда принудительной отправки
@bot.on_message(filters.command("force_report"))
async def force_cmd(c, m):
    await m.reply("Запускаю формирование отчетов вручную...")
    await send_daily_reports()
    await m.reply("Принудительная отправка завершена.")


@bot.on_message(filters.command("graph"))
async def graph_cmd(c, m):
    session = Session()
    try:
        stats = session.query(DailyStats).order_by(DailyStats.date.desc()).limit(7).all()
        stats.reverse()
        if not stats: return await m.reply("Нет данных.")

        plt.figure(figsize=(8, 4))
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_lids for s in stats], marker='o', color="blue", label="Вобщем отписанных")
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_lids_rejected for s in stats], marker='v', color="red", label="Отказов")
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_model_registered for s in stats], marker='P', color="green", label="Моделей записано")
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_agent_registered for s in stats], marker='P', color="cyan", label="Агентов записано")
        plt.xlabel("Дата")
        plt.ylabel("Значение")
        plt.title('Сумма лидов за неделю')
        plt.legend()
        plt.grid()
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()
        await m.reply_photo(buf)
    finally:
        Session.remove()


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
    session = Session()
    try:
        await bot.get_chat(CHANNEL_ID)
        moscow_now = datetime.now(timezone('Europe/Moscow'))
        today = moscow_now.date()
        end_time = moscow_now.replace(hour=21, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        logger.info(f"Сбор отчетов за период: {start_time} - {end_time}")
        stmt = select(Report).where(
            Report.created_at >= start_time,
            Report.created_at < end_time
        )
        reports: Sequence[Report] = session.scalars(stmt).all()

        if not reports:
            await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=f"📅 {today}: Отчетов нет.")
            return

        usernames_reported = list()
        checkpointed_reports: dict[str, tuple[str, int, int]] = dict()
        for r in reports:
            text = (f"📄 **Отчет #{r.id}** ({r.created_at.strftime('%d/%m/%Y, %H:%M:%S')})\n"
                    f"👤 Сотрудник: **@{r.telegram_user}**\n"
                    f"Сложности: {r.text_difficult}\n"
                    f"Выводы: {r.text_conclusions}\n"
                    f"---------------")
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
            await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=text)
            usernames_reported.append(r.telegram_user)
            await asyncio.sleep(0.3)

        unreported_text = "❌ Не отправили отчёты вовремя: "
        unreported_exists = False
        async for member in bot.get_chat_members(CHANNEL_ID):
            if member.user.id not in usernames_reported and member.user.username not in usernames_reported:
                if member.user.id not in USERNAMES_REPORTED_EXCLUDE and member.user.username not in USERNAMES_REPORTED_EXCLUDE:
                    if member.user.username:
                        unreported_text += f" L {member.user.username}\n"
                    else:
                        unreported_text += f" L {member.user.id}\n"
                    unreported_exists = True
        if unreported_exists:
            await asyncio.sleep(0.3)
            await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=unreported_text)

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
        await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=summary)

        ds = session.query(DailyStats).filter_by(date=today).first()
        if not ds: ds = DailyStats(date=today)
        ds.total_reports = len(reports)
        ds.total_lids = daily_lids_sum
        ds.total_lids_rejected = daily_lids_rejected_sum
        ds.total_model_registered = daily_model_registered_sum
        ds.total_agent_registered = daily_agent_registered_sum
        session.add(ds)
        session.commit()

        checkpoint_summary_exists = False
        checkpoint_summary = "⭐ Сотрудники прошедшие этап: \n"
        for checkpointed_report, checkpoint_data in checkpointed_reports.items():
            checkpoint_name: str
            checkpoint_value: int
            current_value: int
            checkpoint_name, checkpoint_value, current_value = checkpoint_data
            checkpoint_summary += f"  L @{checkpointed_report} прошёл этап: {checkpoint_name}=={checkpoint_value}, набрал {current_value}\n"
            checkpoint_summary_exists = True
        if checkpoint_summary_exists:
            await asyncio.sleep(0.3)
            await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=summary)

    except Exception as e:
        logger.error(f"Report Error: {e}")
    finally:
        Session.remove()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--api":
        logger.info("Запуск Flask API...")
        Base.metadata.create_all(engine)
        app.run(host='0.0.0.0', port=5000)
    else:
        logger.info("Подготовка бота...")

        Base.metadata.create_all(engine)

        async def setup_and_run():
            moscow_tz = timezone("Europe/Moscow")
            scheduler = AsyncIOScheduler(timezone=moscow_tz)
            scheduler.add_job(send_daily_reports, 'cron', hour=21, minute=0)
            scheduler.start()
            logger.info("Планировщик запущен на 21:00 МСК")

            await bot.start()
            logger.info("Бот запущен и ожидает сообщений...")
            await idle()
            await bot.stop()

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(setup_and_run())
        except KeyboardInterrupt:
            pass
