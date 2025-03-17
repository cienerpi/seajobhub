import os
import asyncio
import requests
import logging
import sqlite3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загружаем переменные из .env
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")  # Например, -1002629029765 (ID канала или супергруппы)
VACANCY_BASE_URL = os.getenv("VACANCY_BASE_URL", "https://ukrcrewing.com.ua/en/vacancy/")
DB_FILE = os.getenv("DB_FILE", "vacancies.db")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
CHECK_INTERVAL = 60  # Интервал автоматической проверки (1 минута для теста, можно поставить 1800 для 30 минут)

# Допустимые названия вакансий и соответствующие ID топиков (нижний регистр)
TOPIC_ID_MAPPING = {
    "chief officer": 36,
    "master": 2,
    "2nd officer": 44,
    "3rd officer": 50,
    "chief engineer": 54,
    "2nd engineer": 58,
    "3rd engineer": 62,
    "4th engineer": 66,
    "electrical engineer": 70,
    "bosun": 74,
    "able seaman": 78,
    "ordinary seaman": 82,
    "fitter": 86,
    "motorman": 90,
    "wiper": 94,
    "cook": 98,
    "steward": 102
}

VACANCY_DELIMITER = "===VACANCY==="


### Работа с SQLite

def get_db_connection():
    return sqlite3.connect(DB_FILE)


def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_vacancies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vacancy_id INTEGER NOT NULL UNIQUE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print("Database and table 'processed_vacancies' are ready.")


def get_last_processed_id() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(vacancy_id) FROM processed_vacancies")
    result = cursor.fetchone()
    conn.close()
    # Если таблица пуста, начинаем с 308351 (начальное значение можно изменить)
    last_id = result[0] if result[0] is not None else 308420
    print(f"Last processed vacancy ID: {last_id}")
    return last_id


def save_processed_id(vacancy_id: int):
    # Сохраняем только те ID, для которых вакансия реально была обработана
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT OR IGNORE INTO processed_vacancies (vacancy_id) VALUES (?)", (vacancy_id,))
        conn.commit()
        print(f"Saved processed vacancy ID: {vacancy_id}")
    except Exception as e:
        logger.error(f"Error saving vacancy id {vacancy_id}: {e}")
        print(f"Error saving vacancy id {vacancy_id}: {e}")
    finally:
        conn.close()


### Парсинг вакансии (сырые данные)

def parse_vacancy_page(vacancy_id: int) -> str:
    url = VACANCY_BASE_URL + str(vacancy_id)
    print(f"Requesting vacancy page: {url}")
    try:
        response = requests.get(url, timeout=30)
    except Exception as e:
        print(f"Error requesting URL {url}: {e}")
        logger.error(f"Error requesting URL {url}: {e}")
        return None

    if response.status_code != 200:
        print(f"Vacancy ID {vacancy_id} not found (status {response.status_code}).")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    main_block = soup.find("div", class_="vacancy-full-content")
    if not main_block:
        print(f"Vacancy {vacancy_id}: main block not found.")
        return None

    h1 = main_block.find("h1")
    if not h1:
        print(f"Vacancy {vacancy_id}: no <h1> found.")
        return None
    title = h1.get_text(strip=True)
    print(f"Vacancy {vacancy_id} title: {title}")

    raw_text = main_block.get_text(separator="\n", strip=True)
    combined = f"Job Title: {title}\n" + raw_text
    return combined


### Форматирование через DeepSeek

async def format_vacancy_deepseek(raw_text: str) -> str:
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}"}
    prompt = (
        "You are an expert vacancy formatter. Your task is to process the following raw vacancy information and output "
        "a single, beautifully formatted vacancy in English using plain text. You may use small emojis or emoticons to enhance the presentation.\n\n"
        "DO NOT use any HTML tags or markdown formatting; use only newline characters for line breaks.\n\n"
        "IMPORTANT: Normalize the job title exactly to one of the following: \n"
        "Chief Officer, Master, 2nd Officer, 3rd Officer, Chief Engineer, 2nd Engineer, 3rd Engineer, 4th Engineer, Electrical Engineer, Bosun, Able Seaman, Ordinary Seaman, Fitter, Motorman, Wiper, Cook, Steward.\n\n"
        "If the vacancy does not match any of these job titles, return an empty result (skip it).\n\n"
        "Format the vacancy using the following template exactly:\n\n"
        "<Job Title> on <Vessel Type> \n\n"
        "Joining Date: <Joining Date>\n"
        "Voyage Duration: <Voyage Duration>\n\n"
        "🚢 Vessel Details:\n"
        "• Type: <Vessel Type>\n"
        "• Year Built: <Year Built>\n"
        "• DWT: <DWT>\n"
        "• Crew Composition: <Crew Composition>\n\n"
        "📋 Requirements:\n"
        "• English Proficiency: <English Proficiency>\n"
        "• Age Limit: <Age Limit>\n"
        "• Experience in Position: <Experience in Position>\n\n"
        "💰 Compensation:\n"
        "• Salary: <Salary>\n\n"
        "📞 Contact Information:\n"
        "• Phone: <Phone>\n"
        "• Email: <Email> \n"
        "• Recommended e-mail subject: <Subject> \n"
        "• Recruitment Manager: <Manager Name>\n\n"
        "👔 Employer: <Employer>\n\n"
        "Return ONLY the final formatted text exactly as specified above, using newline characters for line breaks.\n\n"
        "Raw vacancy information:\n"
        f"{raw_text}\n\n"
        "Return only the formatted text."
    )
    data = {
        "model": "deepseek-chat",
        "messages": [{
            "role": "user",
            "content": prompt
        }],
        "max_tokens": 1500
    }
    print("Sending raw vacancy to DeepSeek for formatting...")
    loop = asyncio.get_running_loop()
    try:
        response = await loop.run_in_executor(
            None, lambda: requests.post(url, json=data, headers=headers, timeout=60)
        )
    except Exception as e:
        print(f"DeepSeek request error: {e}")
        raise

    if response.status_code == 200:
        formatted = response.json()["choices"][0]["message"]["content"].strip()
        if not formatted:
            print("DeepSeek returned an empty result (vacancy skipped).")
        else:
            print("Received formatted vacancy from DeepSeek.")
        return formatted
    else:
        error_msg = f"DeepSeek API Error: {response.status_code} - {response.text}"
        print(error_msg)
        raise Exception(error_msg)


### Определение топика для вакансии

def choose_topic(formatted_text: str) -> int:
    # Берем первую строку отформатированного текста для сопоставления
    first_line = formatted_text.strip().split("\n", 1)[0].strip()
    lower_line = first_line.lower()
    print(f"First line for topic matching: '{lower_line}'")
    for keyword, topic_id in TOPIC_ID_MAPPING.items():
        if keyword in lower_line:
            print(f"Matched keyword '{keyword}', topic: {topic_id}")
            return topic_id
    print("No matching topic found.")
    return 0


### Фоновая задача проверки вакансий

async def check_new_vacancies(context: ContextTypes.DEFAULT_TYPE):
    print("Starting vacancy check...")
    logger.info("Starting vacancy check...")
    last_id = get_last_processed_id()
    current_id = last_id + 1
    new_count = 0
    missing_count = 0
    max_missing = 10  # Если подряд не найдено 10 вакансий, останавливаем цикл

    while missing_count < max_missing:
        print(f"Checking vacancy ID: {current_id}")
        raw_vacancy = parse_vacancy_page(current_id)
        if raw_vacancy is None:
            print(f"Vacancy ID {current_id} not found. Skipping.")
            logger.info(f"Vacancy ID {current_id} not found. Skipping.")
            # НЕ сохраняем отсутствующий ID, чтобы он мог быть проверен в следующий раз
            current_id += 1
            missing_count += 1
            continue
        else:
            missing_count = 0  # сброс, если вакансия найдена

        try:
            formatted_vacancy = await format_vacancy_deepseek(raw_vacancy)
            if not formatted_vacancy:
                print(f"Vacancy ID {current_id}: DeepSeek returned empty result. Skipping.")
                logger.info(f"Vacancy ID {current_id}: DeepSeek returned empty result. Skipping.")
                save_processed_id(current_id)
                current_id += 1
                continue
            print(f"Formatted vacancy for ID {current_id}:\n{formatted_vacancy}")
            logger.info("Formatted vacancy received from DeepSeek.")
        except Exception as e:
            print(f"Error formatting vacancy ID {current_id}: {e}")
            logger.error(f"Error formatting vacancy ID {current_id}: {e}")
            save_processed_id(current_id)
            current_id += 1
            continue

        topic_id = choose_topic(formatted_vacancy)
        if topic_id == 0:
            print(f"Vacancy ID {current_id}: Could not determine topic. Skipping.")
            logger.info(f"Vacancy ID {current_id}: Could not determine topic. Skipping.")
            save_processed_id(current_id)
            current_id += 1
            continue

        try:
            await context.bot.send_message(
                chat_id=TARGET_CHAT_ID,
                text=formatted_vacancy,
                message_thread_id=topic_id
            )
            print(f"Sent vacancy ID {current_id} to topic {topic_id}.")
            logger.info(f"Sent vacancy ID {current_id} to topic {topic_id}.")
        except Exception as e:
            print(f"Error sending vacancy ID {current_id}: {e}")
            logger.error(f"Error sending vacancy ID {current_id}: {e}")

        save_processed_id(current_id)
        new_count += 1
        current_id += 1

    print(f"Vacancy check complete. {new_count} new vacancies processed.")
    logger.info(f"Vacancy check complete. {new_count} new vacancies processed.")


### Команды для бота

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("Received /start command.")
    await update.message.reply_text("Vacancy scraper bot is running.")


async def scrape_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("Received /scrape command. Starting vacancy check manually.")
    await check_new_vacancies(context)
    await update.message.reply_text("Vacancy check completed.")


# Функция для автоматического запуска команды /scrape каждые 60 секунд
async def scheduled_scrape(context: ContextTypes.DEFAULT_TYPE):
    print("Scheduled scrape triggered.")
    await check_new_vacancies(context)


### Основной запуск бота

def main():
    create_table()
    print("Database is ready.")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("scrape", scrape_command))

    # Автоматическая проверка вакансий каждые 60 секунд, вызывая scheduled_scrape
    app.job_queue.run_repeating(scheduled_scrape, interval=CHECK_INTERVAL, first=0)

    print("Bot is running. Waiting for commands and scheduled vacancy checks...")
    logger.info("Bot is running. Waiting for commands and scheduled vacancy checks...")
    app.run_polling()


if __name__ == "__main__":
    main()
