import hashlib
import sqlite3
from datetime import datetime, timezone
from typing import Optional

DB_PATH = "trustkart.db"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_password(password: str) -> str:
    return hashlib.sha256(f"trustkart_salt::{password}".encode("utf-8")).hexdigest()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_login_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS stores (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            is_online INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS products (
            id TEXT PRIMARY KEY,
            store_id TEXT NOT NULL,
            name TEXT NOT NULL,
            brand TEXT NOT NULL,
            price REAL NOT NULL,
            total_stock INTEGER NOT NULL,
            reserved_stock INTEGER NOT NULL DEFAULT 0,
            image_url TEXT NOT NULL,
            FOREIGN KEY (store_id) REFERENCES stores(id)
        );

        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            customer_id TEXT,
            customer_name TEXT NOT NULL,
            customer_email TEXT,
            total_amount REAL NOT NULL,
            paid_amount REAL NOT NULL DEFAULT 0,
            refunded_amount REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sub_orders (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            store_id TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            needs_attention INTEGER NOT NULL DEFAULT 0,
            failure_reason TEXT,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (store_id) REFERENCES stores(id)
        );

        CREATE TABLE IF NOT EXISTS sub_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sub_order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            product_name TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            FOREIGN KEY (sub_order_id) REFERENCES sub_orders(id)
        );

        CREATE TABLE IF NOT EXISTS payment_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            order_id TEXT NOT NULL,
            sub_order_id TEXT,
            event_type TEXT NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS order_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            sub_order_id TEXT,
            actor TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrics (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    ensure_default_metrics(cur)
    conn.commit()
    conn.close()


def ensure_default_metrics(cur: sqlite3.Cursor):
    for key in (
        "oversells_blocked",
        "duplicates_blocked",
        "partial_refunds_safe",
        "abandoned_released",
    ):
        cur.execute(
            "INSERT OR IGNORE INTO metrics (key, value) VALUES (?, 0)",
            (key,),
        )


def increment_metric(conn: sqlite3.Connection, key: str, delta: int = 1):
    conn.execute(
        "INSERT OR IGNORE INTO metrics (key, value) VALUES (?, 0)",
        (key,),
    )
    conn.execute(
        "UPDATE metrics SET value = value + ? WHERE key = ?",
        (delta, key),
    )


def log_order_event(
    conn: sqlite3.Connection,
    order_id: str,
    actor: str,
    event_type: str,
    message: str,
    sub_order_id: Optional[str] = None,
):
    conn.execute(
        """
        INSERT INTO order_events (order_id, sub_order_id, actor, event_type, message, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (order_id, sub_order_id, actor, event_type, message, now_iso()),
    )


def seed_stores_and_products(cur: sqlite3.Cursor):
    stores = [
        ("STORE-A", "Electronics Hub", "Mobiles & Audio", 1),
        ("STORE-B", "Fashion & Clothing", "Apparel & Streetwear", 1),
        ("STORE-C", "General & Lifestyle Mart", "Footwear & Everyday Gear", 1),
    ]
    cur.executemany(
        "INSERT INTO stores (id, name, category, is_online) VALUES (?, ?, ?, ?)",
        stores,
    )

    products = [
        (
            "PROD-1",
            "STORE-A",
            "Apple iPhone 16 Pro (256GB)",
            "Apple",
            119900.0,
            3,
            0,
            "https://images.unsplash.com/photo-1592750475338-74b7b21085ab?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-2",
            "STORE-A",
            "Sony WH-1000XM5 Headphones",
            "Sony",
            26990.0,
            5,
            0,
            "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-3",
            "STORE-A",
            "Apple AirPods Pro (2nd Gen)",
            "Apple",
            21900.0,
            4,
            0,
            "https://images.unsplash.com/photo-1606220588913-b3aacb4d2f46?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-4",
            "STORE-A",
            "Samsung 25W Fast Charger",
            "Samsung",
            1499.0,
            6,
            0,
            "https://images.unsplash.com/photo-1583863788434-e58a36330cf0?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-5",
            "STORE-B",
            "Levi's Classic Denim Jacket",
            "Levi's",
            4599.0,
            2,
            0,
            "https://images.unsplash.com/photo-1551537482-f2075a1d41f2?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-6",
            "STORE-B",
            "Puma Essential Hoodie",
            "Puma",
            2999.0,
            4,
            0,
            "https://images.unsplash.com/photo-1556821840-3a63f95609a7?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-7",
            "STORE-B",
            "Allen Solly Casual Shirt",
            "Allen Solly",
            1899.0,
            5,
            0,
            "https://images.unsplash.com/photo-1596755094514-f87e34085b2c?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-8",
            "STORE-C",
            "Nike Air Jordan Sneakers",
            "Nike",
            10795.0,
            3,
            0,
            "https://images.unsplash.com/photo-1542291026-7eec264c27ff?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-9",
            "STORE-C",
            "Milton Thermosteel Bottle (1L)",
            "Milton",
            999.0,
            6,
            0,
            "https://images.unsplash.com/photo-1602143407151-7111542de6e8?auto=format&fit=crop&w=600&q=80",
        ),
        (
            "PROD-10",
            "STORE-C",
            "Casio G-Shock Digital Watch",
            "Casio",
            8995.0,
            3,
            0,
            "https://images.unsplash.com/photo-1523275335684-37898b6baf30?auto=format&fit=crop&w=600&q=80",
        ),
    ]
    cur.executemany(
        """
        INSERT INTO products (id, store_id, name, brand, price, total_stock, reserved_stock, image_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        products,
    )


def reset_stock_and_orders():
    """Resets stores, stock, orders and metrics while KEEPING registered customer accounts."""
    conn = get_connection()
    cur = conn.cursor()
    cur.executescript(
        """
        DELETE FROM order_events;
        DELETE FROM payment_events;
        DELETE FROM sub_order_items;
        DELETE FROM sub_orders;
        DELETE FROM orders;
        DELETE FROM products;
        DELETE FROM stores;
        UPDATE metrics SET value = 0;
        """
    )
    ensure_default_metrics(cur)
    seed_stores_and_products(cur)
    conn.commit()
    conn.close()


def full_reset_db():
    """Clears everything including registered customer accounts."""
    conn = get_connection()
    cur = conn.cursor()
    cur.executescript(
        """
        DELETE FROM order_events;
        DELETE FROM payment_events;
        DELETE FROM sub_order_items;
        DELETE FROM sub_orders;
        DELETE FROM orders;
        DELETE FROM products;
        DELETE FROM stores;
        DELETE FROM customers;
        UPDATE metrics SET value = 0;
        """
    )
    ensure_default_metrics(cur)
    seed_stores_and_products(cur)
    conn.commit()
    conn.close()