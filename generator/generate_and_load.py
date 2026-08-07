#!/usr/bin/env python3
"""Generate synthetic source data and load it into the test databases."""
import csv
import io
import os
import random
import sys
from datetime import date, timedelta, datetime
from pathlib import Path

import numpy as np
import psycopg2
import pymysql
from faker import Faker

SEED = int(os.environ.get("GEN_SEED", "42"))
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

OUT = Path(os.environ.get("GEN_OUTPUT_DIR", "/data/output"))
OUT.mkdir(parents=True, exist_ok=True)

START_DATE = date.fromisoformat(os.environ.get("GEN_START_DATE", "2024-01-01"))
END_DATE = date.fromisoformat(os.environ.get("GEN_END_DATE", "2025-12-31"))
TOTAL_DAYS = (END_DATE - START_DATE).days + 1

N_CUSTOMERS = int(os.environ.get("GEN_N_CUSTOMERS", "10000"))
N_PRODUCTS = int(os.environ.get("GEN_N_PRODUCTS", "2000"))
N_ORDERS = int(os.environ.get("GEN_N_ORDERS", "100000"))

# --- Postgres (orders) connection ---
PG_HOST = os.environ["ORDERS_DB_HOST"]
PG_PORT = int(os.environ.get("ORDERS_DB_PORT", "5432"))
PG_DB = os.environ["ORDERS_DB_NAME"]
PG_USER = os.environ["ORDERS_DB_USER"]
PG_PASSWORD = os.environ["ORDERS_DB_PASSWORD"]

# --- MySQL (customers) connection ---
MY_HOST = os.environ["CUSTOMERS_DB_HOST"]
MY_PORT = int(os.environ.get("CUSTOMERS_DB_PORT", "3306"))
MY_DB = os.environ["CUSTOMERS_DB_NAME"]
MY_USER = os.environ["CUSTOMERS_DB_USER"]
MY_PASSWORD = os.environ["CUSTOMERS_DB_PASSWORD"]

SEGMENTS = ["RETAIL", "VIP", "WHOLESALE"]
SEGMENT_WEIGHTS = [0.75, 0.15, 0.10]
SEGMENT_ORDER_WEIGHT = {"RETAIL": 1.0, "VIP": 3.0, "WHOLESALE": 1.5}

CATEGORIES = {
    "Electronics": ["Phones", "Laptops", "Audio", "Accessories"],
    "Apparel": ["Men", "Women", "Kids", "Footwear"],
    "Home & Garden": ["Furniture", "Decor", "Kitchen", "Outdoor"],
    "Sports": ["Fitness", "Team Sports", "Outdoor Rec"],
    "Beauty": ["Skincare", "Makeup", "Haircare"],
    "Toys": ["Educational", "Action Figures", "Board Games"],
    "Grocery": ["Snacks", "Beverages", "Pantry"],
    "Books": ["Fiction", "Non-Fiction", "Children"],
}
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Hooli", "Soylent"]
COUNTRIES = ["Egypt", "USA", "Germany", "UK", "UAE", "France", "India", "Nigeria"]
CITY_BY_COUNTRY = {
    "Egypt": ["Cairo", "Alexandria", "Giza"],
    "USA": ["New York", "Chicago", "Austin"],
    "Germany": ["Berlin", "Munich", "Hamburg"],
    "UK": ["London", "Manchester", "Leeds"],
    "UAE": ["Dubai", "Abu Dhabi"],
    "France": ["Paris", "Lyon"],
    "India": ["Mumbai", "Bangalore"],
    "Nigeria": ["Lagos", "Abuja"],
}
CURRENCIES = ["USD", "EUR", "GBP", "EGP"]
CURRENCY_WEIGHTS = [0.55, 0.20, 0.10, 0.15]
BLACK_FRIDAY = {2024: date(2024, 11, 29), 2025: date(2025, 11, 28)}


def log(msg):
    print(f"[generator] {msg}", flush=True)


def daily_order_weight(d: date) -> float:
    weight = 1.0
    bf = BLACK_FRIDAY.get(d.year, date(d.year, 11, 29))
    days_from_bf = (d - bf).days
    if 0 <= days_from_bf <= 3:
        weight *= 4.0
    elif d.month in (11, 12):
        weight *= 1.6
    if d.month in (7, 8):
        weight *= 0.75
    if d.weekday() >= 5:
        weight *= 1.2
    return weight


def is_post_black_friday_window(d: date) -> bool:
    bf = BLACK_FRIDAY.get(d.year, date(d.year, 11, 29))
    return 0 <= (d - bf).days <= 14


# -----------------------------------------------------------------------------
# Generation (in-memory, same logic/distributions as the standalone version)
# -----------------------------------------------------------------------------
def generate_customers():
    log(f"Generating {N_CUSTOMERS:,} customers...")
    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        country = random.choice(COUNTRIES)
        city = random.choice(CITY_BY_COUNTRY[country])
        signup_date = fake.date_between(start_date=date(2018, 1, 1), end_date=END_DATE)
        customers.append({
            "customer_id": cid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "country": country,
            "city": city,
            "signup_date": signup_date,
            "customer_segment": segment,
            "is_active": 0 if random.random() < 0.05 else 1,
        })
    return customers


def generate_products():
    log(f"Generating {N_PRODUCTS:,} products...")
    products = []
    for pid in range(1, N_PRODUCTS + 1):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        unit_cost = round(random.uniform(3, 100), 2)
        list_price = round(unit_cost * random.uniform(1.3, 2.0), 2)
        products.append({
            "product_id": pid,
            "product_name": f"{fake.word().capitalize()} {subcategory[:-1] if subcategory.endswith('s') else subcategory}",
            "category": category,
            "subcategory": subcategory,
            "brand": random.choice(BRANDS),
            "unit_cost": unit_cost,
            "list_price": list_price,
            "is_active": 1 if random.random() < 0.95 else 0,
        })
    return products


def generate_exchange_rates():
    log("Generating exchange rates...")
    FX_BASELINE = {"EUR": 0.92, "GBP": 0.79, "EGP": 48.5}
    fx_rows = []
    for i in range(TOTAL_DAYS):
        d = START_DATE + timedelta(days=i)
        for cur, base_rate in FX_BASELINE.items():
            rate = round(base_rate * (1 + (random.random() - 0.5) * 0.02), 6)
            fx_rows.append({
                "rate_date": d,
                "base_currency": "USD",
                "quote_currency": cur,
                "rate": rate,
            })
    return fx_rows


def generate_orders_and_items(customers, products):
    log(f"Generating {N_ORDERS:,} orders + order_items...")
    customer_ids = [c["customer_id"] for c in customers]
    customer_weights = [SEGMENT_ORDER_WEIGHT[c["customer_segment"]] for c in customers]
    customer_signup = {c["customer_id"]: c["signup_date"] for c in customers}
    customer_active = {c["customer_id"]: bool(c["is_active"]) for c in customers}
    product_ids = [p["product_id"] for p in products]
    product_price = {p["product_id"]: p["list_price"] for p in products}

    all_days = [START_DATE + timedelta(days=i) for i in range(TOTAL_DAYS)]
    day_weights = np.array([daily_order_weight(d) for d in all_days], dtype=float)
    day_weights /= day_weights.sum()
    order_days = np.random.choice(len(all_days), size=N_ORDERS, p=day_weights)

    orders, order_items = [], []
    order_item_seq = 1
    STATUSES_BASE = ["PLACED", "SHIPPED", "DELIVERED", "DELIVERED", "DELIVERED", "CANCELLED", "RETURNED"]

    for oid in range(1, N_ORDERS + 1):
        d = all_days[order_days[oid - 1]]

        cust_id = None
        for _ in range(5):
            candidate = random.choices(customer_ids, weights=customer_weights, k=1)[0]
            if customer_signup[candidate] <= d and customer_active[candidate]:
                cust_id = candidate
                break
        if cust_id is None:
            cust_id = random.choice([c for c in customer_ids if customer_signup[c] <= d])

        sales_channel = random.choices(["ONLINE", "STORE"], weights=[0.75, 0.25], k=1)[0]
        if sales_channel == "ONLINE":
            hour = int(np.clip(np.random.normal(19, 3), 0, 23))
        else:
            hour = int(np.clip(np.random.normal(13, 2.5), 9, 20))
        order_ts = datetime(d.year, d.month, d.day, hour, random.randint(0, 59), random.randint(0, 59))

        status_weights = [1, 1, 3, 3, 3, 1, 4] if is_post_black_friday_window(d) else [1, 1, 3, 3, 3, 1, 1]
        order_status = random.choices(STATUSES_BASE, weights=status_weights, k=1)[0]

        currency = random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0]
        shipping_cost = 0.0 if random.random() < 0.3 else round(random.uniform(3, 15), 2)

        n_items = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15], k=1)[0]
        chosen_products = random.sample(product_ids, k=min(n_items, len(product_ids)))
        order_total = 0.0
        for prod_id in chosen_products:
            qty = random.randint(1, 5)
            unit_price = product_price[prod_id]
            discount_pct = round(random.uniform(0, 0.3), 3) if random.random() < 0.2 else 0.0
            line_total = round(qty * unit_price * (1 - discount_pct), 2)
            order_items.append({
                "order_item_id": order_item_seq,
                "order_id": oid,
                "product_id": prod_id,
                "quantity": qty,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "line_total": line_total,
            })
            order_item_seq += 1
            order_total += line_total

        orders.append({
            "order_id": oid,
            "customer_id": cust_id,
            "order_date": d,
            "order_ts": order_ts,
            "order_status": order_status,
            "sales_channel": sales_channel,
            "currency_code": currency,
            "shipping_cost": shipping_cost,
            "order_total": round(order_total + shipping_cost, 2),
        })

        if oid % 20000 == 0:
            log(f"  {oid:,}/{N_ORDERS:,} orders...")

    return orders, order_items


# -----------------------------------------------------------------------------
# Loading
# -----------------------------------------------------------------------------
def load_postgres(orders, order_items, products):
    log(f"Connecting to Postgres at {PG_HOST}:{PG_PORT}/{PG_DB}...")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS order_items;
                DROP TABLE IF EXISTS orders;
                DROP TABLE IF EXISTS products;

                CREATE TABLE orders (
                    order_id BIGINT PRIMARY KEY, customer_id INTEGER NOT NULL,
                    order_date DATE NOT NULL, order_ts TIMESTAMP NOT NULL,
                    order_status VARCHAR(20) NOT NULL, sales_channel VARCHAR(10) NOT NULL,
                    currency_code CHAR(3) NOT NULL, shipping_cost NUMERIC(10,2) NOT NULL,
                    order_total NUMERIC(12,2) NOT NULL
                );
                CREATE TABLE order_items (
                    order_item_id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(order_id),
                    product_id INTEGER NOT NULL, quantity INTEGER NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL, discount_pct NUMERIC(4,3) NOT NULL,
                    line_total NUMERIC(12,2) NOT NULL
                );
                CREATE TABLE products (
                    product_id INT PRIMARY KEY, product_name VARCHAR(100), category VARCHAR(50),
                    subcategory VARCHAR(50), brand VARCHAR(50), unit_cost NUMERIC(10,2),
                    list_price NUMERIC(10,2), is_active BOOLEAN
                );
            """)
            conn.commit()

            def copy(table, rows, cols):
                buf = io.StringIO()
                w = csv.writer(buf)
                for r in rows:
                    w.writerow([r[c] for c in cols])
                buf.seek(0)
                cur.copy_expert(f"COPY {table} ({','.join(cols)}) FROM STDIN WITH (FORMAT csv)", buf)

            log(f"  COPYing {len(products):,} products...")
            copy("products", products, ["product_id", "product_name", "category", "subcategory", "brand", "unit_cost", "list_price", "is_active"])

            log(f"  COPYing {len(orders):,} orders...")
            copy("orders", orders, ["order_id", "customer_id", "order_date", "order_ts", "order_status", "sales_channel", "currency_code", "shipping_cost", "order_total"])

            log(f"  COPYing {len(order_items):,} order_items...")
            copy("order_items", order_items, ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct", "line_total"])

            cur.execute("""
                CREATE INDEX idx_orders_date ON orders(order_date);
                CREATE INDEX idx_orders_customer ON orders(customer_id);
                CREATE INDEX idx_items_order ON order_items(order_id);
                CREATE INDEX idx_items_product ON order_items(product_id);
            """)
            conn.commit()
        log("Postgres load complete.")
    finally:
        conn.close()


def load_mysql(customers):
    log(f"Connecting to MySQL at {MY_HOST}:{MY_PORT}/{MY_DB}...")
    conn = pymysql.connect(host=MY_HOST, port=MY_PORT, database=MY_DB, user=MY_USER, password=MY_PASSWORD)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS customers")
            cur.execute("""
                CREATE TABLE customers (
                    customer_id INT PRIMARY KEY, first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL, email VARCHAR(120) NOT NULL,
                    country VARCHAR(50) NOT NULL, city VARCHAR(50) NOT NULL,
                    signup_date DATE NOT NULL, customer_segment VARCHAR(20) NOT NULL,
                    is_active TINYINT(1) NOT NULL
                )
            """)
            conn.commit()

            log(f"  Inserting {len(customers):,} customers (batched executemany)...")
            rows = [
                (c["customer_id"], c["first_name"], c["last_name"], c["email"], c["country"],
                 c["city"], c["signup_date"], c["customer_segment"], c["is_active"])
                for c in customers
            ]
            BATCH = 2000
            for i in range(0, len(rows), BATCH):
                cur.executemany(
                    "INSERT INTO customers (customer_id, first_name, last_name, email, country, city, signup_date, customer_segment, is_active) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    rows[i:i + BATCH],
                )
            conn.commit()

            cur.execute("CREATE INDEX idx_customers_country ON customers(country)")
            cur.execute("CREATE INDEX idx_customers_signup ON customers(signup_date)")
            conn.commit()
        log("MySQL load complete.")
    finally:
        conn.close()


def write_flat_outputs(products, fx_rows):
    log(f"Writing products.csv and exchange_rates.csv to {OUT}...")
    with open(OUT / "products.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=products[0].keys())
        w.writeheader()
        w.writerows(products)
    with open(OUT / "exchange_rates.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fx_rows[0].keys())
        w.writeheader()
        w.writerows(fx_rows)


def main():
    customers = generate_customers()
    products = generate_products()
    fx_rows = generate_exchange_rates()
    orders, order_items = generate_orders_and_items(customers, products)

    write_flat_outputs(products, fx_rows)
    load_mysql(customers)
    load_postgres(orders, order_items, products)

    log("")
    log("Done:")
    log(f"  customers      {len(customers):,}  -> MySQL")
    log(f"  products       {len(products):,}  -> Postgres + products.csv")
    log(f"  exchange_rates {len(fx_rows):,}  -> exchange_rates.csv")
    log(f"  orders         {len(orders):,}  -> Postgres")
    log(f"  order_items    {len(order_items):,}  -> Postgres")


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        log(f"FATAL: missing required env var {e}. Check docker-compose environment: block.")
        sys.exit(1)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(1)
