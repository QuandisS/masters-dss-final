import pandas as pd
import hashlib
import psycopg
import os
from datetime import datetime
import sys
from dotenv import load_dotenv

load_dotenv()


def generate_hash(*args):
    """Генерация MD5 хэша из конкатенированных строк"""
    return hashlib.md5("".join(str(arg) for arg in args).encode()).hexdigest()


def get_db_connection():
    """Создание подключения к БД из переменных окружения"""
    try:
        conn = psycopg.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "greenplum_db"),
            user=os.getenv("DB_USER", "gpadmin"),
            password=os.getenv("DB_PASSWORD", ""),
            connect_timeout=30,
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)


def check_table_exists(cur, table_name):
    """Проверка существования таблицы"""
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = %s
        )
    """,
        (table_name,),
    )
    return cur.fetchone()[0]


def get_existing_keys(cur, table_name, key_column):
    """Получение существующих ключей из таблицы"""
    cur.execute(f"SELECT {key_column} FROM {table_name}")
    return set(row[0] for row in cur.fetchall())


def load_data_vault():
    """Основная функция загрузки данных в Data Vault"""

    file_path = os.getenv("FILE_PATH", "SampleSuperstore.csv")

    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        sys.exit(1)

    try:
        print(f"Загрузка данных из файла: {file_path}")
        df = pd.read_csv(file_path)
        print(f"Загружено {len(df)} строк")

        conn = get_db_connection()
        cur = conn.cursor()
        print("Подключение к БД установлено")

        required_tables = [
            "hub_customer",
            "hub_product",
            "hub_location",
            "hub_order",
            "link_order",
            "sat_customer",
            "sat_product",
            "sat_location",
            "sat_order",
        ]

        missing_tables = []
        for table in required_tables:
            if not check_table_exists(cur, table):
                missing_tables.append(table)

        if missing_tables:
            print(f"Ошибка: отсутствуют таблицы: {', '.join(missing_tables)}")
            print("Пожалуйста, сначала выполните DDL скрипт для создания таблиц")
            sys.exit(1)

        # Получение существующих ключей
        print("Получение существующих ключей из БД...")
        existing_customers = get_existing_keys(cur, "hub_customer", "customer_hash_key")
        existing_products = get_existing_keys(cur, "hub_product", "product_hash_key")
        existing_locations = get_existing_keys(cur, "hub_location", "location_hash_key")
        existing_orders = get_existing_keys(cur, "hub_order", "order_hash_key")
        existing_links = get_existing_keys(cur, "link_order", "order_hash_key")

        # Получение существующих записей в спутниках для избежания дубликатов
        cur.execute("SELECT order_hash_key, load_dts FROM sat_order")
        existing_sat_orders = set((row[0], row[1]) for row in cur.fetchall())

        print("Генерация хэш-ключей...")

        # Для хабов используем уникальные комбинации
        customers_unique = df[
            ["Segment", "City", "State", "Postal Code"]
        ].drop_duplicates()
        products_unique = df[["Category", "Sub-Category"]].drop_duplicates()
        locations_unique = df[
            ["City", "State", "Postal Code", "Region"]
        ].drop_duplicates()

        customers_unique["customer_hash_key"] = customers_unique.apply(
            lambda row: generate_hash(
                row["Segment"], row["City"], row["State"], row["Postal Code"]
            ),
            axis=1,
        )
        products_unique["product_hash_key"] = products_unique.apply(
            lambda row: generate_hash(row["Category"], row["Sub-Category"]), axis=1
        )
        locations_unique["location_hash_key"] = locations_unique.apply(
            lambda row: generate_hash(row["City"], row["State"], row["Postal Code"]),
            axis=1,
        )

        # Для заказов используем все строки, но удаляем дубликаты по хэш-ключу
        df["order_hash_key"] = df.apply(lambda row: generate_hash(*row), axis=1)
        orders_unique = df[["order_hash_key"]].drop_duplicates()

        # Текущая дата для load_dts с уникальными метками времени для каждой записи
        base_dts = datetime.now()
        record_source = os.path.basename(file_path)

        print("Загрузка в хабы...")

        # hub_customer
        new_customers = customers_unique[
            ~customers_unique["customer_hash_key"].isin(existing_customers)
        ]
        for _, row in new_customers.iterrows():
            cur.execute(
                """
                INSERT INTO hub_customer (customer_hash_key, load_dts, record_source) 
                VALUES (%s, %s, %s)
            """,
                (row["customer_hash_key"], base_dts, record_source),
            )
        print(f"  - Добавлено клиентов: {len(new_customers)}")

        # hub_product
        new_products = products_unique[
            ~products_unique["product_hash_key"].isin(existing_products)
        ]
        for _, row in new_products.iterrows():
            cur.execute(
                """
                INSERT INTO hub_product (product_hash_key, load_dts, record_source) 
                VALUES (%s, %s, %s)
            """,
                (row["product_hash_key"], base_dts, record_source),
            )
        print(f"  - Добавлено продуктов: {len(new_products)}")

        # hub_location
        new_locations = locations_unique[
            ~locations_unique["location_hash_key"].isin(existing_locations)
        ]
        for _, row in new_locations.iterrows():
            cur.execute(
                """
                INSERT INTO hub_location (location_hash_key, load_dts, record_source) 
                VALUES (%s, %s, %s)
            """,
                (row["location_hash_key"], base_dts, record_source),
            )
        print(f"  - Добавлено локаций: {len(new_locations)}")

        # hub_order
        new_orders = orders_unique[
            ~orders_unique["order_hash_key"].isin(existing_orders)
        ]
        for _, row in new_orders.iterrows():
            cur.execute(
                """
                INSERT INTO hub_order (order_hash_key, load_dts, record_source) 
                VALUES (%s, %s, %s)
            """,
                (row["order_hash_key"], base_dts, record_source),
            )
        print(f"  - Добавлено заказов: {len(new_orders)}")

        print("Загрузка в линки...")

        # Создаем DataFrame для линков с уникальными order_hash_key
        links_df = df[
            [
                "order_hash_key",
                "Segment",
                "City",
                "State",
                "Postal Code",
                "Category",
                "Sub-Category",
            ]
        ].drop_duplicates(subset=["order_hash_key"])

        new_links = links_df[~links_df["order_hash_key"].isin(existing_links)]
        for _, row in new_links.iterrows():
            customer_hash = generate_hash(
                row["Segment"], row["City"], row["State"], row["Postal Code"]
            )
            product_hash = generate_hash(row["Category"], row["Sub-Category"])
            location_hash = generate_hash(row["City"], row["State"], row["Postal Code"])

            cur.execute(
                """
                INSERT INTO link_order (order_hash_key, customer_hash_key, product_hash_key, location_hash_key, load_dts, record_source)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    row["order_hash_key"],
                    customer_hash,
                    product_hash,
                    location_hash,
                    base_dts,
                    record_source,
                ),
            )
        print(f"  - Добавлено связей: {len(new_links)}")

        # Вставка в спутники (SCD-2 - всегда вставляем новые версии)
        print("Загрузка в спутники...")

        # sat_customer - используем уникальные записи
        for _, row in customers_unique.iterrows():
            cur.execute(
                """
                INSERT INTO sat_customer (customer_hash_key, load_dts, segment, region, record_source)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (
                    row["customer_hash_key"],
                    base_dts,
                    row["Segment"],
                    row.get("Region", ""),
                    record_source,
                ),
            )
        print(f"  - Спутник клиентов: {len(customers_unique)} записей")

        # sat_product
        for _, row in products_unique.iterrows():
            cur.execute(
                """
                INSERT INTO sat_product (product_hash_key, load_dts, category, sub_category, record_source)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (
                    row["product_hash_key"],
                    base_dts,
                    row["Category"],
                    row["Sub-Category"],
                    record_source,
                ),
            )
        print(f"  - Спутник продуктов: {len(products_unique)} записей")

        # sat_location
        for _, row in locations_unique.iterrows():
            cur.execute(
                """
                INSERT INTO sat_location (location_hash_key, load_dts, city, state, postal_code, region, record_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    row["location_hash_key"],
                    base_dts,
                    row["City"],
                    row["State"],
                    row["Postal Code"],
                    row["Region"],
                    record_source,
                ),
            )
        print(f"  - Спутник локаций: {len(locations_unique)} записей")

        # sat_order - используем уникальные записи с разными временными метками
        print("Загрузка в спутник заказов...")

        # Создаем уникальный DataFrame для sat_order
        sat_order_df = df.drop_duplicates(subset=["order_hash_key"])

        inserted_count = 0
        for i, row in enumerate(sat_order_df.iterrows()):
            _, row_data = row
            # Создаем уникальную временную метку для каждой записи
            unique_dts = base_dts.replace(microsecond=base_dts.microsecond + i)

            # Проверяем, не существует ли уже такая запись
            if (row_data["order_hash_key"], unique_dts) not in existing_sat_orders:
                cur.execute(
                    """
                    INSERT INTO sat_order (order_hash_key, load_dts, ship_mode, sales, quantity, discount, profit, record_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        row_data["order_hash_key"],
                        unique_dts,
                        row_data["Ship Mode"],
                        row_data["Sales"],
                        row_data["Quantity"],
                        row_data["Discount"],
                        row_data["Profit"],
                        record_source,
                    ),
                )
                inserted_count += 1

        print(f"  - Спутник заказов: {inserted_count} записей")

        # Коммит изменений
        conn.commit()
        print("Данные успешно загружены!")

        # Итоговая статистика
        print("\nИтоговая статистика:")
        print(f"  - Всего клиентов: {len(customers_unique)}")
        print(f"  - Всего продуктов: {len(products_unique)}")
        print(f"  - Всего локаций: {len(locations_unique)}")
        print(f"  - Всего заказов: {len(orders_unique)}")
        print(f"  - Всего транзакций в исходных данных: {len(df)}")

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        if "conn" in locals():
            conn.rollback()  # type: ignore
        raise
    finally:
        # Закрытие соединения
        if "cur" in locals():
            cur.close()  # type: ignore
        if "conn" in locals():
            conn.close()  # type: ignore
        print("Соединение с БД закрыто")


if __name__ == "__main__":
    # Проверка обязательных переменных окружения
    required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(
            f"Ошибка: отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}"
        )
        sys.exit(1)

    load_data_vault()
