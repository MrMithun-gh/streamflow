"""Generates fake transaction events. Pure functions, no I/O, easy to test."""
import random
import uuid
from datetime import datetime, timezone

# (product_id, product_name, category, typical unit price in INR)
PRODUCTS = [
    ("P101", "Wireless Mouse", "Electronics", 799),
    ("P102", "Bluetooth Headphones", "Electronics", 2499),
    ("P103", "USB-C Cable", "Electronics", 299),
    ("P201", "Running Shoes", "Fashion", 3199),
    ("P202", "Cotton T-Shirt", "Fashion", 599),
    ("P203", "Denim Jacket", "Fashion", 2799),
    ("P301", "Notebook Pack", "Stationery", 249),
    ("P302", "Gel Pen Set", "Stationery", 149),
    ("P401", "Protein Bar Box", "Grocery", 899),
    ("P402", "Green Tea Pack", "Grocery", 349),
    ("P501", "Yoga Mat", "Sports", 1299),
    ("P502", "Water Bottle", "Sports", 499),
]

PAYMENT_METHODS = ["upi", "credit_card", "debit_card", "net_banking", "cash_on_delivery"]

NUM_CUSTOMERS = 200


def generate_transaction() -> dict:
    """Return ONE valid transaction as a dictionary."""
    product_id, product_name, category, unit_price = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    price = unit_price * random.uniform(0.9, 1.1)  # small price variation

    return {
        "transaction_id": "T" + uuid.uuid4().hex[:12],
        "customer_id": f"C{random.randint(1, NUM_CUSTOMERS):04d}",
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "amount": round(price * quantity, 2),
        "quantity": quantity,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payment_method": random.choice(PAYMENT_METHODS),
    }


def corrupt_transaction(txn: dict) -> dict:
    """Return a copy of txn with ONE deliberate defect (for data-quality testing)."""
    bad = dict(txn)
    defect = random.choice(
        ["null_id", "negative_amount", "zero_quantity", "bad_timestamp", "missing_customer"]
    )
    if defect == "null_id":
        bad["transaction_id"] = None
    elif defect == "negative_amount":
        bad["amount"] = -abs(bad["amount"])
    elif defect == "zero_quantity":
        bad["quantity"] = 0
    elif defect == "bad_timestamp":
        bad["timestamp"] = "not-a-timestamp"
    elif defect == "missing_customer":
        bad["customer_id"] = None
    return bad


def next_event(bad_rate: float, previous: dict | None = None) -> dict:
    """Return the next event: usually valid, occasionally a duplicate or corrupted."""
    if random.random() >= bad_rate:
        return generate_transaction()
    # Bad event: 20% of the time a duplicate of the previous event, else a corrupted one
    if previous is not None and random.random() < 0.2:
        return dict(previous)
    return corrupt_transaction(generate_transaction())