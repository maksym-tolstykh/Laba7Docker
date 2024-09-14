import psycopg2
from psycopg2 import OperationalError, Error
from dotenv import load_dotenv
import os

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
    # Створення курсору для виконання SQL-команд
    cursor = conn.cursor()

    try:
        # Створення таблиці Постачальники
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Suppliers (
                SupplierCode SERIAL PRIMARY KEY,  -- Автоматичний лічильник для первинного ключа
                CompanyName VARCHAR(100) NOT NULL,  -- Назва компанії
                ContactPerson VARCHAR(100),  -- Контактна особа
                Phone VARCHAR(25) CHECK (Phone ~ '^\\+?[0-9\\s()-]+$'),  -- Маска вводу телефону
                BankAccount VARCHAR(20) NOT NULL  -- Розрахунковий рахунок
            );
        ''')
        
        # Створення таблиці Матеріали
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Materials (
                MaterialCode SERIAL PRIMARY KEY,  -- Автоматичний лічильник для первинного ключа
                MaterialName VARCHAR(100) NOT NULL,  -- Назва матеріалу
                Price REAL CHECK (Price >= 0)  -- Ціна матеріалу повинна бути більше або рівною 0
            );
        ''')

        # Створення таблиці Поставки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Deliveries (
                DeliveryNumber SERIAL PRIMARY KEY,  -- Автоматичний лічильник для первинного ключа
                DeliveryDate DATE NOT NULL,  -- Дата поставки
                SupplierCode INTEGER NOT NULL,  -- Код постачальника
                MaterialCode INTEGER NOT NULL,  -- Код матеріалу
                DeliveryDays INTEGER CHECK (DeliveryDays BETWEEN 1 AND 7),  -- Кількість днів поставки (від 1 до 7)
                Quantity INTEGER CHECK (Quantity > 0),  -- Кількість поставлених матеріалів повинна бути більше 0
                FOREIGN KEY (SupplierCode) REFERENCES Suppliers(SupplierCode) ON DELETE CASCADE,  -- Зв'язок з таблицею Suppliers
                FOREIGN KEY (MaterialCode) REFERENCES Materials(MaterialCode) ON DELETE CASCADE  -- Зв'язок з таблицею Materials
            );
        ''')
        
        # Застосування змін
        conn.commit()
        print("Успішно!")
    except Error as e:
        print(f"Помилка при виконанні SQL-запиту: {e}")
        conn.rollback()  # Відкочування змін у разі помилки

    finally:
        # Закриття курсору
        cursor.close()

except OperationalError as e:
    print(f"Помилка при підключенні до бази даних: {e}")

finally:
    if conn:
        conn.close()  # Закриття підключення до бази даних
        print("З'єднання з базою даних закрите.")
