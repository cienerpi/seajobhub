import sqlite3
import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()
DB_FILE = os.getenv("DB_FILE", "vacancies.db")

def clear_database():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM processed_vacancies")
        conn.commit()
        print("Database cleared successfully.")
    except Exception as e:
        print(f"Error clearing database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    clear_database()
