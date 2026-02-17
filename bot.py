import os
import sys
import threading
import logging
import io
import asyncio
import traceback
from datetime import datetime, timedelta, time

import requests
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Date, func
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from flask import Flask, request, jsonify, render_template_string
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import matplotlib
import matplotlib.pyplot as plt
from pytz import timezone

# Настройка Matplotlib для Docker
matplotlib.use('Agg')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("BOT_TOKEN")


def send_error_to_admin(err_text):
    """Отправка ошибки через прямой HTTP запрос (Requests)"""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": ADMIN_ID,
            "text": f"⚠️ **CRITICAL ERROR**\n\n`{err_text[-4000:]}`",  # Ограничение TG
            "parse_mode": "Markdown"
        }
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.error(f"Could not send error to admin: {e}")

class TelegramErrorTableHandler(logging.Handler):
    """Кастомный хендлер: перехватывает logger.error() и выше"""
    def emit(self, record):
        log_entry = self.format(record)
        send_error_to_admin(f"Уровень: {record.levelname}\n{log_entry}")

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Перехватчик для необработанных исключений"""
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
    "int_lid_accepted", "int_lid_active", "int_lid_sum", "int_lid_rejected"
]

# --- БАЗА ДАННЫХ ---
Base = declarative_base()


class Report(Base):
    __tablename__ = 'reports'
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.now)
    telegram_user = Column(String(100))
    text_conclusions = Column(String(1000))
    text_difficult = Column(String(1000))
    int_agent_registered = Column(Integer, default=0)
    int_all_agent_registered = Column(Integer, default=0)
    int_all_model_registered = Column(Integer, default=0)
    int_lid_accepted = Column(Integer, default=0)
    int_lid_active = Column(Integer, default=0)
    int_lid_rejected = Column(Integer, default=0)
    int_lid_sum = Column(Integer, default=0)
    int_model_registered = Column(Integer, default=0)


class DailyStats(Base):
    __tablename__ = 'daily_stats'
    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True)
    total_reports = Column(Integer)
    total_lids = Column(Integer)
    total_lids_rejected = Column(Integer)
    total_model_registered = Column(Integer)
    total_agent_registered = Column(Integer)


engine = create_engine(DB_URI, pool_recycle=3600)
SessionFactory = sessionmaker(bind=engine)
Session = scoped_session(SessionFactory)

# --- FLASK СЕРВЕР ---
app = Flask(__name__)


@app.route('/bob_stat', methods=['POST'])
def webhook():
    data = request.json
    if not data: return jsonify({"error": "No data"}), 400
    try:
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


@app.route('/webapp')
def webapp_page():
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
        <script src="https://telegram.org/js/telegram-web-app.js"></script>
        <style>
            body { margin: 0; padding: 0; background-color: var(--tg-theme-bg-color); color: var(--tg-theme-text-color); }
            #error-message { display: none; text-align: center; padding-top: 50px; font-family: sans-serif; }
            iframe { border: none; width: 100%; height: 100vh; display: none; }
        </style>
    </head>
    <body>
        <div id="error-message">
            <h2>Доступ запрещен</h2>
            <p>Пожалуйста, используйте официального бота.</p>
        </div>

        <iframe id="ya-frame" frameborder="0"></iframe>

        <script>
            const webapp = window.Telegram.WebApp;
            webapp.expand();

            const user = webapp.initDataUnsafe.user;
            const frame = document.getElementById('ya-frame');
            const errorDiv = document.getElementById('error-message');

            if (user && (user.id || user.username)) {
                // Формируем инфо о пользователе (username приоритетнее)
                const userInfo = user.username ? ("@" + user.username) : ("FirstName:" + user.first_name "ID: " + user.id);

                // Формируем URL с параметром (замените URL формы на ваш, если он другой)
                // Параметр в URL должен называться так же, как "переменная" в Яндекс Форме
                const baseUrl = "https://forms.yandex.ru/u/69935851505690fe69657291/?iframe=1";
                const finalUrl = baseUrl + "&telegram_user=" + encodeURIComponent(userInfo);

                frame.src = finalUrl;
                frame.style.display = "block";

                // Подгружаем основной скрипт Яндекса для работы iframe
                const script = document.createElement('script');
                script.src = "https://forms.yandex.ru/_static/embed.js";
                document.head.appendChild(script);
            } else {
                errorDiv.style.display = "block";
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
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_lids for s in stats], marker='o', color="blue")
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_lids_rejected for s in stats], marker='!', color="red")
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_model_registered for s in stats], marker='@', color="green")
        plt.plot([s.date.strftime('%d.%m') for s in stats], [s.total_agent_registered for s in stats], marker='#', color="cyan")
        plt.xlabel("Дата")
        plt.ylabel("Значение")
        plt.title('Сумма лидов за неделю')
        plt.legend()
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
        moscow_now = datetime.now(timezone('Europe/Moscow'))
        today = moscow_now.date()
        reports = session.query(Report).filter(func.date(Report.created_at) == today).all()

        if not reports:
            await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=f"📅 {today}: Отчетов нет.")
            return

        for r in reports:
            text = (f"📄 **Отчет #{r.id}** ({r.created_at.strftime('%d/%m/%Y, %H:%M:%S')})\n"
                    f"👤 Сотрудник: **{r.telegram_user}**\n"
                    f"Сложности: {r.text_difficult}\n"
                    f"Выводы: {r.text_conclusions}\n"
                    f"---")
            for field in INT_FIELDS:
                text += f"\n{field}: {getattr(r, field)}"
            await bot.send_message(CHANNEL_ID, reply_to_message_id=CHANNEL_MESSAGE_ID, text=text)
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

            summary += (f"\n🔹 **{field}**:\n"
                        f"   L: {f_max} | Ср: {f_avg:.1f} | Х: {f_min}")

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
