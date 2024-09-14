
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
#Запит 1 Відобразити всі поставки, які здійснюються за 3 або менше днів. Відсортувати назви постачальників за алфавітом;
    # Вибірка всіх поставок, які здійснюються за 3 або менше днів
    print('Запит 1 Відобразити всі поставки, які здійснюються за 3 або менше днів. Відсортувати назви постачальників за алфавітом;')
    cursor.execute('''
        SELECT Suppliers.CompanyName, Deliveries.DeliveryDate,Deliveries.DeliveryDays
        FROM Suppliers
        JOIN Deliveries ON Suppliers.SupplierCode = Deliveries.SupplierCode
        WHERE Deliveries.DeliveryDays <= 3
        ORDER BY Suppliers.CompanyName;
    ''')
    rows = cursor.fetchall()
    
    # Форматування результатів як таблиця
    print(tabulate(rows, headers=['Назва компанії', 'Дата доставки','Дні поставки'], tablefmt='grid'))

#Запит 2 Порахувати суму, яку треба сплатити за кожну поставку (запит з обчислювальним полем);
    # Вибірка суми, яку треба сплатити за кожну поставку
    cursor.execute('''
        SELECT DeliveryNumber, Quantity * Price AS TotalPrice
        FROM Deliveries
        JOIN Materials ON Deliveries.MaterialCode = Materials.MaterialCode;
    ''')
    rows = cursor.fetchall()

    # Форматування результатів як таблиця
    print("Запит 2 Порахувати суму, яку треба сплатити за кожну поставку (запит з обчислювальним полем)")
    print(tabulate(rows, headers=['Номер поставки', 'Сума до сплати'], tablefmt='grid'))
    
#Запит 3 Відобразити всі поставки обраного матеріалу (запит з параметром);
     # Вибірка всіх поставок обраного матеріалу
    material_code = 1
    cursor.execute(''' 
            SELECT Deliveries.DeliveryNumber, Materials.MaterialName, Deliveries.Quantity, Deliveries.DeliveryDate
            FROM Deliveries
            JOIN Materials ON Deliveries.MaterialCode = Materials.MaterialCode
            WHERE Deliveries.MaterialCode = %s;
            ''', (material_code,))

    rows = cursor.fetchall()
    # Форматування результатів як таблиця
    print("Запит 3 Відобразити всі поставки обраного матеріалу (запит з параметром)")
    print(tabulate(rows, headers=["Номер доставки","Назва матеріалу", "Кількість","Дата доставки"], tablefmt='grid'))

#Запит 4 Порахувати кількість кожного матеріалу, що поставляється кожним постачальником (перехресний запит);
    # Вибірка кількості кожного матеріалу, що поставляється кожним постачальником
    cursor.execute('''
        SELECT Suppliers.CompanyName, Materials.MaterialName, SUM(Deliveries.Quantity) AS TotalQuantity
        FROM Suppliers
        JOIN Deliveries ON Suppliers.SupplierCode = Deliveries.SupplierCode
        JOIN Materials ON Deliveries.MaterialCode = Materials.MaterialCode
        GROUP BY Suppliers.CompanyName, Materials.MaterialName
        ORDER BY Suppliers.CompanyName;
    ''')
    rows = cursor.fetchall()

    # Форматування результатів як таблиця
    print("Запит 4 Порахувати кількість кожного матеріалу, що поставляється кожним постачальником (перехресний запит)")
    print(tabulate(rows, headers=['Назва компанії', 'Назва матеріалу', 'Загальна кількість'], tablefmt='grid'))

#Запи 5 Порахувати загальну кількість кожного матеріалу (підсумковий запит);
    # Вибірка загальної кількості кожного матеріалу
    cursor.execute('''
        SELECT Materials.MaterialName, SUM(Deliveries.Quantity) AS TotalQuantity
        FROM Deliveries
        JOIN Materials ON Deliveries.MaterialCode = Materials.MaterialCode
        GROUP BY Materials.MaterialName;
    ''')
    rows = cursor.fetchall()

    # Форматування результатів як таблиця
    print("Запит 5 Порахувати загальну кількість кожного матеріалу (підсумковий запит)")
    print(tabulate(rows, headers=['Назва матеріалу', 'Загальна кількість'], tablefmt='grid'))


#Запит 5 Порахувати кількість поставок від кожного постачальника (підсумковий запит).
    # Вибірка кількості поставок від кожного постачальника
    cursor.execute('''
        SELECT Suppliers.CompanyName, COUNT(Deliveries.DeliveryNumber) AS TotalDeliveries
        FROM Suppliers
        JOIN Deliveries ON Suppliers.SupplierCode = Deliveries.SupplierCode
        GROUP BY Suppliers.CompanyName;
    ''')
    rows = cursor.fetchall()

    # Форматування результатів як таблиця
    print("Запит 6 Порахувати кількість поставок від кожного постачальника (підсумковий запит)")
    print(tabulate(rows, headers=['Назва компанії', 'Загальна кількість поставок'], tablefmt='grid'))


    # Закриття курсору та з'єднання
    cursor.close()
    conn.close()
    print("З'єднання з базою даних закрите.")
except Error as e:
    print(f"Помилка: {e}")