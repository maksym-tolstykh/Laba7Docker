
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv
import os
from tabulate import tabulate
# Завантаження змінних з файлу .env
load_dotenv()

# Отримання змінних з оточення
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

try:
    # Підключення до бази даних PostgreSQL
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    print("З'єднання з базою даних відкрите.")
    cursor = conn.cursor()

#Завдання 9 виводить всі таблиці(структура + дані, які в ній зберігаються) 
    print("\n\nЗавдання 9 Вивести всі таблиці(структура + дані, які в ній зберігаються)")
    # Вибірка таблиць та їх структури
    cursor.execute('''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    ''')
    tables = cursor.fetchall()

    # Виведення структури та данів кожної таблиці
    for table in tables:
        table_name = table[0]
        cursor.execute(f'SELECT * FROM {table_name};')
        rows = cursor.fetchall()
        print(f"Таблиця: {table_name}")
        print(tabulate(rows, headers=[desc[0] for desc in cursor.description], tablefmt='grid'))
        print()

    

    # Закриття курсору та з'єднання
    cursor.close()
    conn.close()
    print("З'єднання з базою даних закрите.")
except Error as e:
    print(f"Помилка: {e}")