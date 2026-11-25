"""Synthetic marketplace order data and a persistent refund simulator."""

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5


@dataclass(frozen=True)
class Order:
    order_id: str
    status: str
    refundable: bool


ORDERS = {
    "RF-1001": Order("RF-1001", "delivered", True),
    "RF-1002": Order("RF-1002", "in_transit", False),
    "RF-1003": Order("RF-1003", "delivered", False),
}


class RefundLedger:
    def __init__(self, path: Path):
        self.path = path
        connection = sqlite3.connect(path)
        try:
            with connection:
                connection.execute("""
                    CREATE TABLE IF NOT EXISTS refunds (
                        case_id TEXT PRIMARY KEY,
                        order_id TEXT UNIQUE NOT NULL,
                        refund_id TEXT UNIQUE NOT NULL
                    )
                """)
        finally:
            connection.close()

    def execute(self, case_id: str, order_id: str) -> str | None:
        refund_id = str(uuid5(NAMESPACE_URL, f"resolveflow:refund:{case_id}"))
        connection = sqlite3.connect(self.path)
        try:
            with connection:
                connection.execute(
                    "INSERT INTO refunds VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                    (case_id, order_id, refund_id),
                )
                row = connection.execute(
                    "SELECT refund_id FROM refunds WHERE case_id = ? AND order_id = ?",
                    (case_id, order_id),
                ).fetchone()
        finally:
            connection.close()
        return row[0] if row else None
