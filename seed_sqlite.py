"""
seed_sqlite.py
One-time script to create and populate the local SQLite product catalog.
Simulates an operational source database with ~500 products.
Run once before starting the pipeline: python seed_sqlite.py
"""

import sqlite3
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────
DB_PATH = Path("~/AI_Projects/de-ai-agent/data/products.db").expanduser()
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Mirrors Superstore product categories for join compatibility
CATEGORIES = {
    "Furniture": {
        "sub_categories": ["Bookcases", "Chairs", "Furnishings", "Tables"],
        "price_range": (50, 2000),
        "suppliers": ["OfficeMax Supply Co", "Herman Miller", "Steelcase", "HON Industries"],
    },
    "Office Supplies": {
        "sub_categories": ["Appliances", "Art", "Binders", "Envelopes",
                           "Fasteners", "Labels", "Paper", "Storage"],
        "price_range": (2, 300),
        "suppliers": ["Staples Direct", "3M Business", "Avery Products", "Smead Manufacturing"],
    },
    "Technology": {
        "sub_categories": ["Accessories", "Copiers", "Machines", "Phones"],
        "price_range": (20, 5000),
        "suppliers": ["Tech Distributors Inc", "Ingram Micro", "D&H Distributing", "ScanSource"],
    },
}

PRODUCT_ADJECTIVES = [
    "Premium", "Standard", "Deluxe", "Professional", "Essential",
    "Advanced", "Classic", "Modern", "Compact", "Heavy-Duty",
]

PRODUCT_NOUNS = {
    "Furniture": {
        "Bookcases": ["Bookcase", "Shelf Unit", "Display Cabinet", "Storage Tower"],
        "Chairs": ["Office Chair", "Task Chair", "Executive Chair", "Guest Chair"],
        "Furnishings": ["Lamp", "Mat", "Picture Frame", "Partition Panel"],
        "Tables": ["Desk", "Conference Table", "Side Table", "Workstation"],
    },
    "Office Supplies": {
        "Appliances": ["Paper Shredder", "Pencil Sharpener", "Tape Dispenser", "Calculator"],
        "Art": ["Marker Set", "Colored Pencils", "Sketch Pad", "Drawing Kit"],
        "Binders": ["3-Ring Binder", "Report Cover", "View Binder", "Presentation Folder"],
        "Envelopes": ["Business Envelope", "Clasp Envelope", "Bubble Mailer", "Catalog Envelope"],
        "Fasteners": ["Stapler", "Paper Clips", "Binder Clips", "Rubber Bands"],
        "Labels": ["Address Labels", "File Labels", "Name Badge", "Shipping Labels"],
        "Paper": ["Copy Paper", "Card Stock", "Graph Paper", "Recycled Paper"],
        "Storage": ["Filing Cabinet", "Storage Box", "Drawer Organizer", "Desktop Tray"],
    },
    "Technology": {
        "Accessories": ["USB Hub", "Cable Management", "Monitor Stand", "Keyboard Tray"],
        "Copiers": ["Laser Printer", "Multifunction Copier", "Scanner", "Fax Machine"],
        "Machines": ["Desktop Computer", "Laptop Stand", "Projector", "Digital Camera"],
        "Phones": ["Desk Phone", "Cordless Phone", "Conference Speaker", "Headset"],
    },
}


def generate_product_id(category: str, index: int) -> str:
    prefix = category[:3].upper()
    return f"{prefix}-{index:05d}"


def random_date(start_days_ago: int = 730, end_days_ago: int = 0) -> str:
    base = datetime.now()
    delta = random.randint(end_days_ago, start_days_ago)
    return (base - timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")


def seed():
    if DB_PATH.exists():
        print(f"Database already exists at {DB_PATH}. Delete it to re-seed.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id   TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category     TEXT NOT NULL,
            sub_category TEXT NOT NULL,
            unit_cost    REAL NOT NULL,
            supplier     TEXT NOT NULL,
            in_stock     INTEGER NOT NULL DEFAULT 1,  -- 1=True, 0=False
            last_updated TEXT NOT NULL
        )
    """)

    # Watermark support: metadata table tracks last extraction time
    cur.execute("""
        CREATE TABLE IF NOT EXISTS extraction_metadata (
            table_name       TEXT PRIMARY KEY,
            last_extracted_at TEXT
        )
    """)
    cur.execute("""
        INSERT OR IGNORE INTO extraction_metadata (table_name, last_extracted_at)
        VALUES ('products', NULL)
    """)

    rows = []
    index = 1
    for category, config in CATEGORIES.items():
        for sub_category in config["sub_categories"]:
            nouns = PRODUCT_NOUNS[category][sub_category]
            count = random.randint(12, 22)
            for _ in range(count):
                adj = random.choice(PRODUCT_ADJECTIVES)
                noun = random.choice(nouns)
                product_name = f"{adj} {noun}"
                lo, hi = config["price_range"]
                unit_cost = round(random.uniform(lo, hi), 2)
                supplier = random.choice(config["suppliers"])
                in_stock = 1 if random.random() > 0.12 else 0
                last_updated = random_date()

                rows.append((
                    generate_product_id(category, index),
                    product_name,
                    category,
                    sub_category,
                    unit_cost,
                    supplier,
                    in_stock,
                    last_updated,
                ))
                index += 1

    cur.executemany("""
        INSERT OR IGNORE INTO products
        (product_id, product_name, category, sub_category, unit_cost, supplier, in_stock, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()

    row_count = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    print(f"Seeded {row_count} products into {DB_PATH}")
    print(f"Categories: {list(CATEGORIES.keys())}")

    conn.close()


if __name__ == "__main__":
    seed()
