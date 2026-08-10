#!/usr/bin/env python3
"""Seed a small batch of NEW rows into the already-running source systems,
to demonstrate that Bronze's watermark-based incremental extraction
actually skips old rows and picks up new ones on a second DAG run.

Why this exists: generate_and_load.py (the main generator) is a one-shot
job — DROP + full reload every time it runs, and its Compose service has
restart: "no". After the first Bronze run, every later scheduled/manual
DAG trigger finds zero new rows forever, because nothing ever adds new
data to Postgres/MySQL/the fx CSV after that first load. That makes it
impossible to actually show the incremental logic (common/incremental.py)
doing its job — get_watermark/set_watermark, and the "0 new rows, skip
write" path in each bronze/extract_*.py — in a live demo.

This script fixes that by appending a small, clearly-marked batch of new
rows past the current max watermark for each incremental source:
  - customers  (MySQL)      : a handful of new customer_ids
  - orders + order_items    (Postgres): a handful of new orders, dated
                               after the current max order_date, for a
                               mix of existing AND newly-seeded customers
  - exchange_rates (CSV)    : new rows appended for the same new date
                               range, since Bronze's fx extraction reads
                               the whole CSV but only keeps rows past its
                               watermark (see extract_fx_rates.py)

Usage (after the stack is up and the main generator has already run once):
    docker compose run --rm data-generator python seed_incremental.py

Then trigger the DAG again — extract_customers/extract_orders/
extract_fx_rates should report a small nonzero row count and a moved
watermark; extract_products should report 0 (products stays full-refresh
by design, not part of this demo).

Not meant to be run before the main generator, or repeatedly without
limit — each run adds another small batch on top of whatever's already
there. Fine to run a few times to demonstrate multiple incremental runs;
just re-run `generate_and_load.py` first if you want to reset to a clean
baseline.
"""
import csv
import os
import random
import sys
from datetime import date, datetime, timedelta

import psycopg2
import pymysql
from faker import Faker

fake = Faker()

OUT_DIR = os.environ.get("GEN_OUTPUT_DIR", "/data/output")
FX_CSV_PATH = os.path.join(OUT_DIR, "exchange_rates.csv")

N_NEW_CUSTOMERS = int(os.environ.get("SEED_N_NEW_CUSTOMERS", "20"))
N_NEW_ORDERS = int(os.environ.get("SEED_N_NEW_ORDERS", "100"))

PG_HOST = os.environ["ORDERS_DB_HOST"]
PG_PORT = int(os.environ.get("ORDERS_DB_PORT", "5432"))
PG_DB = os.environ["ORDERS_DB_NAME"]
PG_USER = os.environ["ORDERS_DB_USER"]
PG_PASSWORD = os.environ["ORDERS_DB_PASSWORD"]

MY_HOST = os.environ["CUSTOMERS_DB_HOST"]
MY_PORT = int(os.environ.get("CUSTOMERS_DB_PORT", "3306"))
MY_DB = os.environ["CUSTOMERS_DB_NAME"]
MY_USER = os.environ["CUSTOMERS_DB_USER"]
MY_PASSWORD = os.environ["CUSTOMERS_DB_PASSWORD"]

SEGMENTS = ["RETAIL", "VIP", "WHOLESALE"]
SEGMENT_WEIGHTS = [0.75, 0.15, 0.10]
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
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "DELIVERED", "CANCELLED", "RETURNED"]
CURRENCIES = ["USD", "EUR", "GBP", "EGP"]
CURRENCY_WEIGHTS = [0.55, 0.20, 0.10, 0.15]
FX_BASELINE = {"EUR": 0.92, "GBP": 0.79, "EGP": 48.5}


def log(msg):
    print(f"[seed_incremental] {msg}", flush=True)


def pg_connect():
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)


def my_connect():
    return pymysql.connect(host=MY_HOST, port=MY_PORT, database=MY_DB, user=MY_USER, password=MY_PASSWORD)


def seed_customers(conn) -> tuple:
    """Insert N_NEW_CUSTOMERS new rows past the current max customer_id.
    Returns (new_customer_ids, new_customer_ids_active_only)."""
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(customer_id), 0) FROM customers")
        max_id = cur.fetchone()[0]

    new_ids = list(range(max_id + 1, max_id + 1 + N_NEW_CUSTOMERS))
    rows = []
    for cid in new_ids:
        segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        country = random.choice(COUNTRIES)
        city = random.choice(CITY_BY_COUNTRY[country])
        rows.append((
            cid, fake.first_name(), fake.last_name(), fake.unique.email(),
            country, city, date.today(), segment, 1,
        ))

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO customers (customer_id, first_name, last_name, email, country, city, "
            "signup_date, customer_segment, is_active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows,
        )
    conn.commit()
    log(f"Inserted {len(new_ids)} new customers into MySQL (customer_id {new_ids[0]}..{new_ids[-1]})")
    return new_ids


def seed_orders(conn, seedable_customer_ids: list) -> date:
    """Insert N_NEW_ORDERS new orders + matching order_items, dated after
    the current max order_date. Returns the new max order_date used, so
    the caller can append matching exchange_rates rows for the same
    dates."""
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
        max_order_id = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(MAX(order_item_id), 0) FROM order_items")
        max_item_id = cur.fetchone()[0]
        cur.execute("SELECT MAX(order_date) FROM orders")
        max_order_date = cur.fetchone()[0]
        cur.execute("SELECT product_id, list_price FROM products")
        products = cur.fetchall()

    if max_order_date is None:
        log("FATAL: orders table is empty — run generate_and_load.py first.")
        sys.exit(1)

    new_start_date = max_order_date + timedelta(days=1)
    new_end_date = new_start_date + timedelta(days=4)  # a small 5-day new window
    date_span = (new_end_date - new_start_date).days + 1

    order_id = max_order_id
    item_id = max_item_id
    order_rows, item_rows = [], []

    for _ in range(N_NEW_ORDERS):
        order_id += 1
        d = new_start_date + timedelta(days=random.randint(0, date_span - 1))
        order_ts = datetime(d.year, d.month, d.day, random.randint(8, 21), random.randint(0, 59), random.randint(0, 59))
        cust_id = random.choice(seedable_customer_ids)
        sales_channel = random.choices(["ONLINE", "STORE"], weights=[0.75, 0.25], k=1)[0]
        currency = random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0]
        status = random.choice(STATUSES)
        shipping_cost = 0.0 if random.random() < 0.3 else round(random.uniform(3, 15), 2)

        n_items = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15], k=1)[0]
        chosen = random.sample(products, k=min(n_items, len(products)))
        order_total = 0.0
        for prod_id, list_price in chosen:
            item_id += 1
            qty = random.randint(1, 5)
            line_total = round(qty * float(list_price), 2)
            item_rows.append((item_id, order_id, prod_id, qty, list_price, 0.0, line_total))
            order_total += line_total

        order_rows.append((
            order_id, cust_id, d, order_ts, status, sales_channel, currency,
            shipping_cost, round(order_total + shipping_cost, 2),
        ))

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO orders (order_id, customer_id, order_date, order_ts, order_status, "
            "sales_channel, currency_code, shipping_cost, order_total) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            order_rows,
        )
        cur.executemany(
            "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, "
            "discount_pct, line_total) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            item_rows,
        )
    conn.commit()
    log(
        f"Inserted {len(order_rows)} new orders ({new_start_date}..{new_end_date}) "
        f"and {len(item_rows)} new order_items into Postgres"
    )
    return new_end_date


def append_fx_rates(new_end_date: date) -> None:
    """Append exchange_rates rows for the same new date window, since
    Bronze's extract_fx_rates.py reads the whole CSV but only keeps rows
    past its own watermark — the file just needs new dates present."""
    if not os.path.exists(FX_CSV_PATH):
        log(f"FATAL: {FX_CSV_PATH} not found — run generate_and_load.py first.")
        sys.exit(1)

    with open(FX_CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        log(f"FATAL: {FX_CSV_PATH} has no rows to infer the current date range from.")
        sys.exit(1)

    max_existing_date = max(date.fromisoformat(r["rate_date"]) for r in rows)
    start = max_existing_date + timedelta(days=1)
    end = new_end_date

    new_rows = []
    d = start
    while d <= end:
        for cur, base_rate in FX_BASELINE.items():
            rate = round(base_rate * (1 + (random.random() - 0.5) * 0.02), 6)
            new_rows.append({"rate_date": d.isoformat(), "base_currency": "USD", "quote_currency": cur, "rate": rate})
        d += timedelta(days=1)

    if not new_rows:
        log(f"exchange_rates.csv already covers through {max_existing_date}, nothing to append.")
        return

    with open(FX_CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["rate_date", "base_currency", "quote_currency", "rate"])
        writer.writerows(new_rows)
    log(f"Appended {len(new_rows)} exchange_rates rows ({start}..{end}) to {FX_CSV_PATH}")


def main():
    log(f"Seeding {N_NEW_CUSTOMERS} new customers and {N_NEW_ORDERS} new orders...")

    my_conn = my_connect()
    try:
        new_customer_ids = seed_customers(my_conn)
    finally:
        my_conn.close()

    pg_conn = pg_connect()
    try:
        # Mix new orders across both newly-seeded and pre-existing
        # customers so the customer_id_exists Silver check has realistic
        # coverage either way — pull a sample of existing ids too.
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT customer_id FROM (SELECT DISTINCT customer_id FROM orders) sub "
                "ORDER BY random() LIMIT 200"
            )
            existing_sample = [r[0] for r in cur.fetchall()]
        seedable = new_customer_ids + existing_sample
        new_end_date = seed_orders(pg_conn, seedable)
    finally:
        pg_conn.close()

    append_fx_rates(new_end_date)

    log("")
    log("Done. Re-trigger the Airflow DAG to see incremental extraction pick these up:")
    log(f"  extract_customers   ~{N_NEW_CUSTOMERS} new rows expected")
    log(f"  extract_orders      ~{N_NEW_ORDERS} new orders + their order_items expected")
    log("  extract_fx_rates    a few new rows expected")
    log("  extract_products    0 rows expected (products stays full-refresh by design)")


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        log(f"FATAL: missing required env var {e}. Run this via `docker compose run --rm data-generator ...` "
            f"so the same environment: block as generate_and_load.py is in effect.")
        sys.exit(1)
