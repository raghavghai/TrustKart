import os
import socket
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel

from database import (
    DB_PATH,
    full_reset_db,
    hash_password,
    increment_metric,
    init_db,
    log_order_event,
    now_iso,
    reset_stock_and_orders,
)

ADMIN_PIN = "admin123"
DB_WRITE_LOCK = threading.RLock()
CACHED_LAN_IP: Optional[str] = None
LAST_EXPIRE_CHECK_TS = 0.0


def get_local_ip() -> str:
    global CACHED_LAN_IP
    if CACHED_LAN_IP:
        return CACHED_LAN_IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    CACHED_LAN_IP = ip
    return ip


def get_tx_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(
        DB_PATH, timeout=10.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    conn = get_tx_conn()
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        count = conn.execute("SELECT COUNT(*) AS c FROM stores").fetchone()["c"]
    finally:
        conn.close()
    if count == 0:
        reset_stock_and_orders()

    lan_ip = get_local_ip()
    print("\n" + "=" * 62)
    print("  TRUSTKART SERVER IS LIVE!")
    print(f"  LAPTOP ADMIN LINK    : http://127.0.0.1:8000/admin  (PIN: {ADMIN_PIN})")
    print(f"  LAPTOP CUSTOMER LINK : http://127.0.0.1:8000/")
    print(f"  LOCAL LAN LINK       : http://{lan_ip}:8000/")
    print("  DEVTUNNELS READY     : Forward Port 8000 (Public) in VS Code")
    print("=" * 62 + "\n")
    yield


app = FastAPI(title="TrustKart — Reliable Multi-Store Payments & Fulfilment", lifespan=lifespan)


# -----------------------------
# Request Schemas
# -----------------------------
class SignupRequest(BaseModel):
    name: str
    email: str
    phone: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class AdminLoginRequest(BaseModel):
    pin: str


class CartItem(BaseModel):
    product_id: str
    qty: int


class CreateOrderRequest(BaseModel):
    customer_id: Optional[str] = None
    customer_name: str = "Guest Shopper"
    customer_email: Optional[str] = None
    items: List[CartItem]
    reservation_seconds: int = 60


class SimulatePaymentRequest(BaseModel):
    order_id: str
    event_id: str
    event_type: str  # "PAYMENT_SUCCESS" or "REFUND"
    sub_order_id: Optional[str] = None
    amount: Optional[float] = None
    delay_seconds: float = 0.0


class ToggleStoreRequest(BaseModel):
    is_online: bool
    fail_active_orders: bool = True


# -----------------------------
# Core Consistency Helpers
# -----------------------------
def recalculate_order_status(conn: sqlite3.Connection, order_id: str) -> str:
    subs = conn.execute(
        "SELECT status, needs_attention FROM sub_orders WHERE order_id = ?",
        (order_id,),
    ).fetchall()
    statuses = [s["status"] for s in subs]
    any_attention = any(s["needs_attention"] == 1 for s in subs)

    if any_attention or "FAILED_NEEDS_REFUND" in statuses:
        new_status = "PARTIAL_FAILURE_ATTENTION"
    elif all(s == "DISPATCHED" for s in statuses):
        new_status = "COMPLETED"
    elif all(s == "EXPIRED" for s in statuses):
        new_status = "EXPIRED"
    elif all(s == "CANCELLED" for s in statuses):
        new_status = "CANCELLED"
    elif all(s == "REFUNDED" for s in statuses):
        new_status = "REFUNDED"
    elif all(s == "RESERVED" for s in statuses):
        new_status = "RESERVED"
    elif any(s in ("REFUNDED", "CANCELLED") for s in statuses):
        new_status = "PARTIALLY_FULFILLED"
    elif any(s == "DISPATCHED" for s in statuses):
        new_status = "PARTIALLY_DISPATCHED"
    else:
        new_status = "PAID"

    conn.execute("UPDATE orders SET status = ? WHERE id = ?", (new_status, order_id))
    return new_status


def release_sub_order_stock(conn: sqlite3.Connection, sub_order_id: str):
    items = conn.execute(
        "SELECT product_id, qty FROM sub_order_items WHERE sub_order_id = ?",
        (sub_order_id,),
    ).fetchall()
    for item in items:
        conn.execute(
            """
            UPDATE products
            SET reserved_stock = MAX(0, reserved_stock - ?)
            WHERE id = ?
            """,
            (item["qty"], item["product_id"]),
        )


def release_expired_reservations(force: bool = False):
    """Fast read-only check first; acquires write lock ONLY if an order actually expired."""
    global LAST_EXPIRE_CHECK_TS
    now_ts = time.monotonic()
    if not force and (now_ts - LAST_EXPIRE_CHECK_TS) < 2.0:
        return
    LAST_EXPIRE_CHECK_TS = now_ts

    conn = get_tx_conn()
    try:
        current_time = now_iso()
        has_expired = conn.execute(
            "SELECT 1 FROM orders WHERE status = 'RESERVED' AND expires_at <= ? LIMIT 1",
            (current_time,),
        ).fetchone()
        if not has_expired:
            return
    finally:
        conn.close()

    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            current_time = now_iso()
            expired_orders = conn.execute(
                """
                SELECT id FROM orders
                WHERE status = 'RESERVED' AND expires_at <= ?
                """,
                (current_time,),
            ).fetchall()

            for row in expired_orders:
                order_id = row["id"]
                subs = conn.execute(
                    "SELECT id, status FROM sub_orders WHERE order_id = ?",
                    (order_id,),
                ).fetchall()
                released_any = False
                for sub in subs:
                    if sub["status"] == "RESERVED":
                        release_sub_order_stock(conn, sub["id"])
                        conn.execute(
                            "UPDATE sub_orders SET status = 'EXPIRED' WHERE id = ?",
                            (sub["id"],),
                        )
                        released_any = True
                conn.execute("UPDATE orders SET status = 'EXPIRED' WHERE id = ?", (order_id,))
                if released_any:
                    increment_metric(conn, "abandoned_released", 1)
                log_order_event(
                    conn,
                    order_id,
                    "SYSTEM",
                    "RESERVATION_EXPIRED",
                    "60-second checkout timer expired. Locked stock automatically released back to stores.",
                )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


def create_order_tx(req: CreateOrderRequest) -> dict:
    if not req.items:
        raise HTTPException(status_code=400, detail="Your cart is empty.")

    merged_qty: dict[str, int] = {}
    for item in req.items:
        if item.qty <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be at least 1.")
        merged_qty[item.product_id] = merged_qty.get(item.product_id, 0) + item.qty

    release_expired_reservations(force=True)

    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")

            store_buckets: dict[str, list[dict]] = {}
            total_amount = 0.0

            for prod_id, qty in merged_qty.items():
                row = conn.execute(
                    """
                    SELECT p.*, s.name AS store_name, s.is_online
                    FROM products p
                    JOIN stores s ON s.id = p.store_id
                    WHERE p.id = ?
                    """,
                    (prod_id,),
                ).fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Product {prod_id} not found.")
                if row["is_online"] == 0:
                    raise HTTPException(
                        status_code=409,
                        detail=f"{row['store_name']} is currently offline and cannot accept orders.",
                    )

                available = row["total_stock"] - row["reserved_stock"]
                if available < qty:
                    increment_metric(conn, "oversells_blocked", 1)
                    conn.execute("COMMIT")
                    raise HTTPException(
                        status_code=409,
                        detail=f"Out of Stock / Order Blocked: {row['name']} has only {available} left (you requested {qty}).",
                    )

                line_total = round(row["price"] * qty, 2)
                total_amount = round(total_amount + line_total, 2)
                store_buckets.setdefault(row["store_id"], []).append(
                    {
                        "product_id": row["id"],
                        "product_name": row["name"],
                        "qty": qty,
                        "unit_price": row["price"],
                        "line_total": line_total,
                        "store_name": row["store_name"],
                    }
                )

            for prod_id, qty in merged_qty.items():
                conn.execute(
                    "UPDATE products SET reserved_stock = reserved_stock + ? WHERE id = ?",
                    (qty, prod_id),
                )

            order_id = f"TK-{uuid.uuid4().hex[:6].upper()}"
            created = datetime.now(timezone.utc)
            expires = created + timedelta(seconds=max(10, req.reservation_seconds))

            conn.execute(
                """
                INSERT INTO orders (id, customer_id, customer_name, customer_email, total_amount, paid_amount, refunded_amount, status, expires_at, created_at)
                VALUES (?, ?, ?, ?, ?, 0, 0, 'RESERVED', ?, ?)
                """,
                (
                    order_id,
                    req.customer_id,
                    req.customer_name.strip() or "Guest Shopper",
                    req.customer_email,
                    total_amount,
                    expires.isoformat(),
                    created.isoformat(),
                ),
            )

            created_subs = []
            for idx, (store_id, items) in enumerate(store_buckets.items(), start=1):
                sub_id = f"{order_id}-S{idx}"
                sub_amount = round(sum(i["line_total"] for i in items), 2)
                conn.execute(
                    """
                    INSERT INTO sub_orders (id, order_id, store_id, amount, status, needs_attention, failure_reason)
                    VALUES (?, ?, ?, ?, 'RESERVED', 0, NULL)
                    """,
                    (sub_id, order_id, store_id, sub_amount),
                )
                for i in items:
                    conn.execute(
                        """
                        INSERT INTO sub_order_items (sub_order_id, product_id, product_name, qty, unit_price)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (sub_id, i["product_id"], i["product_name"], i["qty"], i["unit_price"]),
                    )
                created_subs.append({"sub_order_id": sub_id, "store_id": store_id, "amount": sub_amount})
                log_order_event(
                    conn,
                    order_id,
                    "STORE",
                    "SUB_ORDER_RESERVED",
                    f"Locked stock at {items[0]['store_name']} (Rs. {sub_amount:,.2f}).",
                    sub_order_id=sub_id,
                )

            log_order_event(
                conn,
                order_id,
                "CHECKOUT",
                "ORDER_CREATED",
                f"Order placed by {req.customer_name} across {len(store_buckets)} store(s). Total Rs. {total_amount:,.2f}. 60s payment timer started.",
            )

            conn.execute("COMMIT")
            print(f"[CHECKOUT OK] {order_id} by {req.customer_name} | Rs. {total_amount:,.2f}")
            return {
                "order_id": order_id,
                "total_amount": total_amount,
                "status": "RESERVED",
                "expires_at": expires.isoformat(),
                "sub_orders": created_subs,
                "payment_event_id": f"PAY-{order_id}",
            }
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        except Exception as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            conn.close()


def process_payment_tx(req: SimulatePaymentRequest) -> dict:
    # Sleep BEFORE acquiring any DB lock so delayed webhooks never block other phones
    if req.delay_seconds > 0:
        time.sleep(min(req.delay_seconds, 5.0))

    release_expired_reservations(force=True)

    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")

            order = conn.execute("SELECT * FROM orders WHERE id = ?", (req.order_id,)).fetchone()
            if not order:
                raise HTTPException(status_code=404, detail=f"Order {req.order_id} not found.")

            existing = conn.execute(
                "SELECT * FROM payment_events WHERE event_id = ?", (req.event_id,)
            ).fetchone()
            if existing:
                increment_metric(conn, "duplicates_blocked", 1)
                log_order_event(
                    conn,
                    req.order_id,
                    "PAYMENT_GATEWAY",
                    "DUPLICATE_EVENT_BLOCKED",
                    f"Duplicate {req.event_type} notification ({req.event_id}) safely ignored. No double charge or double refund.",
                    sub_order_id=req.sub_order_id,
                )
                conn.execute("COMMIT")
                print(f"[DUPLICATE BLOCKED] {req.event_id} for {req.order_id}")
                return {
                    "duplicate": True,
                    "applied": False,
                    "event_id": req.event_id,
                    "message": f"Duplicate event {req.event_id} blocked! Customer was NOT charged/refunded twice.",
                }

            event_type = req.event_type.upper()

            if event_type == "PAYMENT_SUCCESS":
                pay_amount = round(req.amount if req.amount is not None else order["total_amount"], 2)
                if round(order["paid_amount"] + pay_amount, 2) > round(order["total_amount"] + 0.01, 2):
                    increment_metric(conn, "duplicates_blocked", 1)
                    conn.execute("COMMIT")
                    raise HTTPException(
                        status_code=409,
                        detail="Order is already fully paid. Duplicate payment rejected.",
                    )

                conn.execute(
                    """
                    INSERT INTO payment_events (event_id, order_id, sub_order_id, event_type, amount, status, note, created_at)
                    VALUES (?, ?, NULL, 'PAYMENT_SUCCESS', ?, 'APPLIED', ?, ?)
                    """,
                    (req.event_id, req.order_id, pay_amount, "Customer checkout payment", now_iso()),
                )
                conn.execute(
                    "UPDATE orders SET paid_amount = ROUND(paid_amount + ?, 2) WHERE id = ?",
                    (pay_amount, req.order_id),
                )

                subs = conn.execute(
                    """
                    SELECT so.*, s.name AS store_name, s.is_online
                    FROM sub_orders so
                    JOIN stores s ON s.id = so.store_id
                    WHERE so.order_id = ?
                    """,
                    (req.order_id,),
                ).fetchall()

                if order["status"] in ("EXPIRED", "CANCELLED"):
                    for sub in subs:
                        conn.execute(
                            """
                            UPDATE sub_orders
                            SET status = 'FAILED_NEEDS_REFUND',
                                needs_attention = 1,
                                failure_reason = 'Late payment arrived after checkout expired/cancelled'
                            WHERE id = ?
                            """,
                            (sub["id"],),
                        )
                    log_order_event(
                        conn,
                        req.order_id,
                        "PAYMENT_GATEWAY",
                        "LATE_PAYMENT_FLAGGED",
                        f"Late payment ({req.event_id}) of Rs. {pay_amount:,.2f} arrived after stock was released. Flagged for staff refund.",
                    )
                else:
                    for sub in subs:
                        if sub["status"] != "RESERVED":
                            continue
                        if sub["is_online"] == 0:
                            release_sub_order_stock(conn, sub["id"])
                            conn.execute(
                                """
                                UPDATE sub_orders
                                SET status = 'FAILED_NEEDS_REFUND',
                                    needs_attention = 1,
                                    failure_reason = 'Store went offline before payment settled'
                                WHERE id = ?
                                """,
                                (sub["id"],),
                            )
                            log_order_event(
                                conn,
                                req.order_id,
                                "RECOVERY",
                                "PARTIAL_FAILURE_DETECTED",
                                f"{sub['store_name']} went offline before payment settled. Stock released and Rs. {sub['amount']:,.2f} queued for partial refund.",
                                sub_order_id=sub["id"],
                            )
                        else:
                            conn.execute(
                                "UPDATE sub_orders SET status = 'PAID' WHERE id = ?",
                                (sub["id"],),
                            )
                    log_order_event(
                        conn,
                        req.order_id,
                        "PAYMENT_GATEWAY",
                        "PAYMENT_APPLIED",
                        f"Payment {req.event_id} confirmed once (Rs. {pay_amount:,.2f}).",
                    )

                final_status = recalculate_order_status(conn, req.order_id)
                conn.execute("COMMIT")
                print(f"[PAY NOW OK] {req.order_id} | {req.event_id} | Status: {final_status}")
                return {
                    "duplicate": False,
                    "applied": True,
                    "event_id": req.event_id,
                    "order_status": final_status,
                    "message": f"Payment {req.event_id} successful (Rs. {pay_amount:,.2f}).",
                }

            elif event_type == "REFUND":
                if not req.sub_order_id:
                    raise HTTPException(
                        status_code=400, detail="sub_order_id is required for a partial refund."
                    )

                sub = conn.execute(
                    """
                    SELECT so.*, s.name AS store_name
                    FROM sub_orders so
                    JOIN stores s ON s.id = so.store_id
                    WHERE so.id = ? AND so.order_id = ?
                    """,
                    (req.sub_order_id, req.order_id),
                ).fetchone()
                if not sub:
                    raise HTTPException(status_code=404, detail="Sub-order not found.")
                if sub["status"] == "REFUNDED":
                    increment_metric(conn, "duplicates_blocked", 1)
                    log_order_event(
                        conn,
                        req.order_id,
                        "PAYMENT_GATEWAY",
                        "DOUBLE_REFUND_BLOCKED",
                        f"Blocked duplicate refund attempt ({req.event_id}) for already-refunded part {sub['id']}.",
                        sub_order_id=sub["id"],
                    )
                    conn.execute("COMMIT")
                    return {
                        "duplicate": True,
                        "applied": False,
                        "event_id": req.event_id,
                        "message": "Already refunded! Duplicate refund safely blocked.",
                    }
                if sub["status"] == "DISPATCHED":
                    raise HTTPException(
                        status_code=409,
                        detail="Cannot refund or erase completed fulfilment: this store already DISPATCHED the order.",
                    )

                refund_amount = round(req.amount if req.amount is not None else sub["amount"], 2)
                if round(order["refunded_amount"] + refund_amount, 2) > round(order["paid_amount"] + 0.01, 2):
                    raise HTTPException(
                        status_code=409,
                        detail="Refund rejected: total refunds cannot exceed paid amount.",
                    )

                if sub["status"] in ("RESERVED", "PAID"):
                    release_sub_order_stock(conn, sub["id"])

                conn.execute(
                    """
                    INSERT INTO payment_events (event_id, order_id, sub_order_id, event_type, amount, status, note, created_at)
                    VALUES (?, ?, ?, 'REFUND', ?, 'APPLIED', ?, ?)
                    """,
                    (
                        req.event_id,
                        req.order_id,
                        sub["id"],
                        refund_amount,
                        f"Safe partial refund for {sub['store_name']}",
                        now_iso(),
                    ),
                )
                conn.execute(
                    "UPDATE orders SET refunded_amount = ROUND(refunded_amount + ?, 2) WHERE id = ?",
                    (refund_amount, req.order_id),
                )
                conn.execute(
                    "UPDATE sub_orders SET status = 'REFUNDED', needs_attention = 0 WHERE id = ?",
                    (sub["id"],),
                )
                increment_metric(conn, "partial_refunds_safe", 1)
                log_order_event(
                    conn,
                    req.order_id,
                    "PAYMENT_GATEWAY",
                    "REFUND_APPLIED",
                    f"Refund {req.event_id} issued once for {sub['store_name']} (Rs. {refund_amount:,.2f}). Healthy store shipments preserved.",
                    sub_order_id=sub["id"],
                )

                final_status = recalculate_order_status(conn, req.order_id)
                conn.execute("COMMIT")
                print(f"[REFUND OK] {req.order_id} / {sub['id']} | Rs. {refund_amount:,.2f}")
                return {
                    "duplicate": False,
                    "applied": True,
                    "event_id": req.event_id,
                    "order_status": final_status,
                    "message": f"Partial refund {req.event_id} completed once (Rs. {refund_amount:,.2f}).",
                }
            else:
                raise HTTPException(status_code=400, detail="event_type must be PAYMENT_SUCCESS or REFUND.")

        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        except Exception as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise HTTPException(status_code=500, detail=str(exc))
        finally:
            conn.close()


def run_consistency_audit(conn: sqlite3.Connection) -> dict:
    issues = []

    products = conn.execute("SELECT * FROM products").fetchall()
    for p in products:
        expected_reserved = conn.execute(
            """
            SELECT COALESCE(SUM(soi.qty), 0) AS qty
            FROM sub_order_items soi
            JOIN sub_orders so ON so.id = soi.sub_order_id
            WHERE soi.product_id = ? AND so.status IN ('RESERVED', 'PAID')
            """,
            (p["id"],),
        ).fetchone()["qty"]

        if p["total_stock"] < 0 or p["reserved_stock"] < 0 or p["reserved_stock"] > p["total_stock"]:
            issues.append(f"Oversell/negative stock detected on {p['name']}.")
        if p["reserved_stock"] != expected_reserved:
            issues.append(
                f"Reserved stock mismatch on {p['name']}: db={p['reserved_stock']} vs active={expected_reserved}."
            )

    orders = conn.execute("SELECT * FROM orders").fetchall()
    for o in orders:
        ledger_paid = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS amt
            FROM payment_events
            WHERE order_id = ? AND event_type = 'PAYMENT_SUCCESS' AND status = 'APPLIED'
            """,
            (o["id"],),
        ).fetchone()["amt"]
        ledger_refunded = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS amt
            FROM payment_events
            WHERE order_id = ? AND event_type = 'REFUND' AND status = 'APPLIED'
            """,
            (o["id"],),
        ).fetchone()["amt"]

        if abs(o["paid_amount"] - ledger_paid) > 0.01:
            issues.append(f"Paid mismatch on {o['id']}.")
        if abs(o["refunded_amount"] - ledger_refunded) > 0.01:
            issues.append(f"Refund mismatch on {o['id']}.")
        if o["refunded_amount"] > o["paid_amount"] + 0.01:
            issues.append(f"Over-refund detected on {o['id']}.")

    return {
        "consistent": len(issues) == 0,
        "issue_count": len(issues),
        "issues": issues,
        "checked_products": len(products),
        "checked_orders": len(orders),
    }


def fetch_orders_enriched(conn: sqlite3.Connection, customer_id: Optional[str] = None):
    if customer_id:
        orders_rows = conn.execute(
            "SELECT * FROM orders WHERE customer_id = ? ORDER BY created_at DESC",
            (customer_id,),
        ).fetchall()
    else:
        orders_rows = conn.execute("SELECT * FROM orders ORDER BY created_at DESC").fetchall()

    orders = []
    unresolved_work = []
    for o in orders_rows:
        od = dict(o)
        subs = []
        for s in conn.execute(
            """
            SELECT so.*, st.name AS store_name, st.is_online
            FROM sub_orders so
            JOIN stores st ON st.id = so.store_id
            WHERE so.order_id = ?
            ORDER BY so.id
            """,
            (o["id"],),
        ).fetchall():
            sd = dict(s)
            sd["items"] = [
                dict(i)
                for i in conn.execute(
                    "SELECT * FROM sub_order_items WHERE sub_order_id = ?", (s["id"],)
                ).fetchall()
            ]
            subs.append(sd)
            if sd["needs_attention"] == 1:
                unresolved_work.append(
                    {
                        "order_id": o["id"],
                        "customer_name": o["customer_name"],
                        "sub_order_id": sd["id"],
                        "store_name": sd["store_name"],
                        "amount": sd["amount"],
                        "status": sd["status"],
                        "reason": sd["failure_reason"] or "Requires staff partial refund",
                    }
                )

        od["sub_orders"] = subs
        od["payment_events"] = [
            dict(pe)
            for pe in conn.execute(
                "SELECT * FROM payment_events WHERE order_id = ? ORDER BY id ASC",
                (o["id"],),
            ).fetchall()
        ]
        od["timeline"] = [
            dict(ev)
            for ev in conn.execute(
                "SELECT * FROM order_events WHERE order_id = ? ORDER BY id ASC",
                (o["id"],),
            ).fetchall()
        ]
        orders.append(od)
    return orders, unresolved_work


# -----------------------------
# Page Routes (No-Cache Headers so Mobile Always Loads Latest Code)
# -----------------------------
@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


@app.get("/")
def serve_customer_site():
    path = os.path.join("static", "customer.html")
    if os.path.exists(path):
        return FileResponse(
            path,
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    return {"status": "TRUSTKART_BACKEND_LIVE", "next_step": "Create static/customer.html"}


@app.get("/admin")
def serve_admin_site():
    path = os.path.join("static", "admin.html")
    if os.path.exists(path):
        return FileResponse(
            path,
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )
    return {"status": "ADMIN_PENDING_HTML", "message": "Next step: create static/admin.html"}


# -----------------------------
# Customer Auth Endpoints
# -----------------------------
@app.post("/api/auth/signup")
def api_signup(req: SignupRequest):
    name = req.name.strip()
    email = req.email.strip().lower()
    phone = req.phone.strip()
    password = req.password.strip()

    if len(name) < 2:
        raise HTTPException(status_code=400, detail="Please enter your full name.")
    if "@" not in email or "." not in email:
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")
    if len(phone) < 7:
        raise HTTPException(status_code=400, detail="Please enter a valid mobile number.")
    if len(password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters.")

    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            exists = conn.execute("SELECT id FROM customers WHERE email = ?", (email,)).fetchone()
            if exists:
                raise HTTPException(
                    status_code=409,
                    detail="An account with this email already exists. Please switch to Login.",
                )

            cust_id = f"CUST-{uuid.uuid4().hex[:6].upper()}"
            ts = now_iso()
            conn.execute(
                """
                INSERT INTO customers (id, name, email, phone, password_hash, created_at, last_login_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (cust_id, name, email, phone, hash_password(password), ts, ts),
            )
            conn.execute("COMMIT")
            return {
                "customer": {"id": cust_id, "name": name, "email": email, "phone": phone},
                "message": f"Welcome to TrustKart, {name}!",
            }
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@app.post("/api/auth/login")
def api_login(req: LoginRequest):
    email = req.email.strip().lower()
    password = req.password.strip()

    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            user = conn.execute("SELECT * FROM customers WHERE email = ?", (email,)).fetchone()
            if not user or user["password_hash"] != hash_password(password):
                raise HTTPException(status_code=401, detail="Invalid email or password.")

            conn.execute(
                "UPDATE customers SET last_login_at = ? WHERE id = ?",
                (now_iso(), user["id"]),
            )
            conn.execute("COMMIT")
            return {
                "customer": {
                    "id": user["id"],
                    "name": user["name"],
                    "email": user["email"],
                    "phone": user["phone"],
                },
                "message": f"Welcome back, {user['name']}!",
            }
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@app.post("/api/admin/login")
def api_admin_login(req: AdminLoginRequest):
    if req.pin.strip() != ADMIN_PIN:
        raise HTTPException(status_code=401, detail="Wrong Admin PIN. Hint: admin123")
    return {"authenticated": True}


# -----------------------------
# State & Order Endpoints
# -----------------------------
@app.get("/api/customer/state")
def api_customer_state(customer_id: Optional[str] = None):
    release_expired_reservations(force=False)
    conn = get_tx_conn()
    try:
        stores = [dict(r) for r in conn.execute("SELECT * FROM stores ORDER BY id").fetchall()]
        products = []
        for r in conn.execute(
            """
            SELECT p.*, s.name AS store_name, s.category AS store_category, s.is_online
            FROM products p
            JOIN stores s ON s.id = p.store_id
            ORDER BY p.store_id, p.id
            """
        ).fetchall():
            d = dict(r)
            d["available_stock"] = d["total_stock"] - d["reserved_stock"]
            products.append(d)

        my_orders = []
        if customer_id:
            my_orders, _ = fetch_orders_enriched(conn, customer_id=customer_id)

        return {
            "stores": stores,
            "products": products,
            "my_orders": my_orders,
            "server_time": now_iso(),
        }
    finally:
        conn.close()


@app.get("/api/admin/state")
def api_admin_state():
    release_expired_reservations(force=False)
    conn = get_tx_conn()
    try:
        stores = [dict(r) for r in conn.execute("SELECT * FROM stores ORDER BY id").fetchall()]
        products = []
        for r in conn.execute(
            """
            SELECT p.*, s.name AS store_name, s.category AS store_category, s.is_online
            FROM products p
            JOIN stores s ON s.id = p.store_id
            ORDER BY p.store_id, p.id
            """
        ).fetchall():
            d = dict(r)
            d["available_stock"] = d["total_stock"] - d["reserved_stock"]
            products.append(d)

        orders, unresolved_work = fetch_orders_enriched(conn, customer_id=None)

        customers = [
            dict(r)
            for r in conn.execute(
                """
                SELECT
                    c.id, c.name, c.email, c.phone, c.created_at, c.last_login_at,
                    COUNT(o.id) AS order_count,
                    COALESCE(SUM(o.paid_amount - o.refunded_amount), 0) AS net_spent
                FROM customers c
                LEFT JOIN orders o ON o.customer_id = c.id
                GROUP BY c.id
                ORDER BY c.last_login_at DESC
                """
            ).fetchall()
        ]

        metrics_rows = conn.execute("SELECT key, value FROM metrics").fetchall()
        metrics = {r["key"]: r["value"] for r in metrics_rows}
        audit = run_consistency_audit(conn)
        lan_ip = get_local_ip()

        return {
            "stores": stores,
            "products": products,
            "orders": orders,
            "unresolved_work": unresolved_work,
            "customers": customers,
            "metrics": metrics,
            "audit": audit,
            "network": {
                "lan_ip": lan_ip,
                "mobile_url": f"http://{lan_ip}:8000/",
                "admin_url": "http://127.0.0.1:8000/admin",
            },
            "server_time": now_iso(),
        }
    finally:
        conn.close()


@app.post("/api/orders")
def api_create_order(req: CreateOrderRequest):
    return create_order_tx(req)


@app.post("/api/orders/{order_id}/cancel")
def api_cancel_unpaid_order(order_id: str):
    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
            if not order:
                raise HTTPException(status_code=404, detail="Order not found.")
            if order["status"] == "CANCELLED":
                conn.execute("COMMIT")
                return {"status": "CANCELLED", "order_id": order_id}
            if order["status"] != "RESERVED":
                raise HTTPException(
                    status_code=409,
                    detail=f"Cannot cancel directly: order status is already {order['status']}.",
                )

            subs = conn.execute("SELECT id, status FROM sub_orders WHERE order_id = ?", (order_id,)).fetchall()
            for sub in subs:
                if sub["status"] == "RESERVED":
                    release_sub_order_stock(conn, sub["id"])
                    conn.execute("UPDATE sub_orders SET status = 'CANCELLED' WHERE id = ?", (sub["id"],))
            conn.execute("UPDATE orders SET status = 'CANCELLED' WHERE id = ?", (order_id,))
            increment_metric(conn, "abandoned_released", 1)
            log_order_event(
                conn,
                order_id,
                "CUSTOMER",
                "ORDER_CANCELLED_BY_CUSTOMER",
                "Customer cancelled checkout before payment. Locked stock released immediately.",
            )
            conn.execute("COMMIT")
            return {"status": "CANCELLED", "order_id": order_id}
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@app.post("/api/orders/{order_id}/expire-now")
def api_expire_now(order_id: str):
    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
            conn.execute(
                "UPDATE orders SET expires_at = ? WHERE id = ? AND status = 'RESERVED'",
                (past, order_id),
            )
            conn.execute("COMMIT")
        finally:
            conn.close()
    release_expired_reservations(force=True)
    return {"status": "expired_checked", "order_id": order_id}


@app.post("/api/payments/simulate")
def api_simulate_payment(req: SimulatePaymentRequest):
    return process_payment_tx(req)


@app.post("/api/stores/{store_id}/toggle")
def api_toggle_store(store_id: str, req: ToggleStoreRequest):
    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            store = conn.execute("SELECT * FROM stores WHERE id = ?", (store_id,)).fetchone()
            if not store:
                raise HTTPException(status_code=404, detail="Store not found.")

            conn.execute(
                "UPDATE stores SET is_online = ? WHERE id = ?",
                (1 if req.is_online else 0, store_id),
            )

            impacted = []
            if not req.is_online and req.fail_active_orders:
                active_subs = conn.execute(
                    """
                    SELECT * FROM sub_orders
                    WHERE store_id = ? AND status IN ('RESERVED', 'PAID')
                    """,
                    (store_id,),
                ).fetchall()

                for sub in active_subs:
                    release_sub_order_stock(conn, sub["id"])
                    if sub["status"] == "PAID":
                        conn.execute(
                            """
                            UPDATE sub_orders
                            SET status = 'FAILED_NEEDS_REFUND',
                                needs_attention = 1,
                                failure_reason = ?
                            WHERE id = ?
                            """,
                            (f"{store['name']} went offline midway through fulfilment", sub["id"]),
                        )
                        log_order_event(
                            conn,
                            sub["order_id"],
                            "OUTAGE_MONITOR",
                            "STORE_FAILED_MID_ORDER",
                            f"{store['name']} went unavailable after payment! Stock reconciled; Rs. {sub['amount']:,.2f} queued for single partial refund. Healthy stores untouched.",
                            sub_order_id=sub["id"],
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE sub_orders
                            SET status = 'CANCELLED',
                                failure_reason = ?
                            WHERE id = ?
                            """,
                            (f"{store['name']} went offline before payment", sub["id"]),
                        )
                        log_order_event(
                            conn,
                            sub["order_id"],
                            "OUTAGE_MONITOR",
                            "STORE_UNAVAILABLE_CANCELLED",
                            f"{store['name']} went offline before payment. Reservation cancelled and stock released.",
                            sub_order_id=sub["id"],
                        )
                    recalculate_order_status(conn, sub["order_id"])
                    impacted.append(sub["id"])

            conn.execute("COMMIT")
            return {
                "store_id": store_id,
                "is_online": req.is_online,
                "impacted_sub_orders": impacted,
            }
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@app.post("/api/sub-orders/{sub_order_id}/dispatch")
def api_dispatch_sub_order(sub_order_id: str):
    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            sub = conn.execute(
                """
                SELECT so.*, s.name AS store_name, s.is_online
                FROM sub_orders so
                JOIN stores s ON s.id = so.store_id
                WHERE so.id = ?
                """,
                (sub_order_id,),
            ).fetchone()
            if not sub:
                raise HTTPException(status_code=404, detail="Sub-order not found.")
            if sub["is_online"] == 0:
                raise HTTPException(status_code=409, detail=f"{sub['store_name']} is currently offline.")
            if sub["status"] != "PAID":
                raise HTTPException(
                    status_code=409,
                    detail=f"Only PAID sub-orders can be dispatched (current status: {sub['status']}).",
                )

            items = conn.execute(
                "SELECT product_id, qty FROM sub_order_items WHERE sub_order_id = ?",
                (sub_order_id,),
            ).fetchall()
            for item in items:
                conn.execute(
                    """
                    UPDATE products
                    SET total_stock = total_stock - ?, reserved_stock = reserved_stock - ?
                    WHERE id = ?
                    """,
                    (item["qty"], item["qty"], item["product_id"]),
                )

            conn.execute("UPDATE sub_orders SET status = 'DISPATCHED' WHERE id = ?", (sub_order_id,))
            log_order_event(
                conn,
                sub["order_id"],
                "FULFILMENT",
                "SUB_ORDER_DISPATCHED",
                f"{sub['store_name']} packed and dispatched items. Stock permanently finalized.",
                sub_order_id=sub_order_id,
            )
            order_status = recalculate_order_status(conn, sub["order_id"])
            conn.execute("COMMIT")
            return {"sub_order_id": sub_order_id, "status": "DISPATCHED", "order_status": order_status}
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@app.post("/api/sub-orders/{sub_order_id}/cancel")
def api_cancel_sub_order(sub_order_id: str):
    with DB_WRITE_LOCK:
        conn = get_tx_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            sub = conn.execute(
                """
                SELECT so.*, s.name AS store_name
                FROM sub_orders so
                JOIN stores s ON s.id = so.store_id
                WHERE so.id = ?
                """,
                (sub_order_id,),
            ).fetchone()
            if not sub:
                raise HTTPException(status_code=404, detail="Sub-order not found.")
            if sub["status"] == "DISPATCHED":
                raise HTTPException(status_code=409, detail="Already dispatched; cannot cancel.")
            if sub["status"] in ("CANCELLED", "REFUNDED", "EXPIRED"):
                raise HTTPException(status_code=409, detail=f"Already {sub['status']}.")

            release_sub_order_stock(conn, sub_order_id)
            if sub["status"] == "PAID":
                conn.execute(
                    """
                    UPDATE sub_orders
                    SET status = 'FAILED_NEEDS_REFUND',
                        needs_attention = 1,
                        failure_reason = 'Cancelled by store after payment — pending single partial refund'
                    WHERE id = ?
                    """,
                    (sub_order_id,),
                )
                log_order_event(
                    conn,
                    sub["order_id"],
                    "ADMIN",
                    "SUB_ORDER_CANCELLED_POST_PAY",
                    f"{sub['store_name']} part cancelled after payment. Stock restored; queued for partial refund.",
                    sub_order_id=sub_order_id,
                )
            else:
                conn.execute("UPDATE sub_orders SET status = 'CANCELLED' WHERE id = ?", (sub_order_id,))
                log_order_event(
                    conn,
                    sub["order_id"],
                    "ADMIN",
                    "SUB_ORDER_CANCELLED",
                    f"{sub['store_name']} unpaid reservation cancelled. Stock released.",
                    sub_order_id=sub_order_id,
                )
            order_status = recalculate_order_status(conn, sub["order_id"])
            conn.execute("COMMIT")
            return {"sub_order_id": sub_order_id, "order_status": order_status}
        except HTTPException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@app.post("/api/admin/reset-orders")
def api_reset_orders_only():
    with DB_WRITE_LOCK:
        reset_stock_and_orders()
    return {"status": "orders_and_stock_reset", "users_kept": True}


@app.post("/api/admin/reset-full")
def api_reset_full():
    with DB_WRITE_LOCK:
        full_reset_db()
    return {"status": "full_reset_complete", "users_kept": False}


# -----------------------------
# Admin Stress-Test Lab Endpoints
# -----------------------------
@app.post("/api/demo/concurrent-rush")
def demo_concurrent_rush():
    with DB_WRITE_LOCK:
        reset_stock_and_orders()

    def attempt_buy(buyer_num: int):
        try:
            res = create_order_tx(
                CreateOrderRequest(
                    customer_name=f"Rush Buyer #{buyer_num}",
                    items=[CartItem(product_id="PROD-1", qty=1)],
                    reservation_seconds=60,
                )
            )
            return {"buyer": f"Rush Buyer #{buyer_num}", "success": True, "order_id": res["order_id"]}
        except HTTPException as e:
            return {"buyer": f"Rush Buyer #{buyer_num}", "success": False, "reason": e.detail}

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(attempt_buy, [1, 2, 3, 4, 5]))

    succeeded = [r for r in results if r["success"]]
    rejected = [r for r in results if not r["success"]]

    conn = get_tx_conn()
    try:
        prod = dict(conn.execute("SELECT * FROM products WHERE id = 'PROD-1'").fetchone())
        audit = run_consistency_audit(conn)
    finally:
        conn.close()

    return {
        "scenario": "5 Simultaneous Buyers vs. 3 Apple iPhone 16 Pros",
        "succeeded_count": len(succeeded),
        "rejected_count": len(rejected),
        "results": results,
        "final_stock": {
            "product": prod["name"],
            "total_stock": prod["total_stock"],
            "reserved_stock": prod["reserved_stock"],
            "available_stock": prod["total_stock"] - prod["reserved_stock"],
        },
        "audit": audit,
    }


@app.post("/api/demo/duplicate-payment")
def demo_duplicate_payment():
    order = create_order_tx(
        CreateOrderRequest(
            customer_name="Priya (Duplicate Webhook Test)",
            items=[
                CartItem(product_id="PROD-2", qty=1),
                CartItem(product_id="PROD-9", qty=1),
            ],
            reservation_seconds=60,
        )
    )
    event_id = f"PAY-DUP-{uuid.uuid4().hex[:5].upper()}"
    attempts = []
    for i in range(1, 4):
        res = process_payment_tx(
            SimulatePaymentRequest(
                order_id=order["order_id"],
                event_id=event_id,
                event_type="PAYMENT_SUCCESS",
            )
        )
        attempts.append({"attempt": i, **res})

    conn = get_tx_conn()
    try:
        o = dict(conn.execute("SELECT * FROM orders WHERE id = ?", (order["order_id"],)).fetchone())
        audit = run_consistency_audit(conn)
    finally:
        conn.close()

    return {
        "scenario": "Same Payment Notification Delivered 3 Times",
        "order_id": order["order_id"],
        "event_id": event_id,
        "order_total": o["total_amount"],
        "actual_paid_amount": o["paid_amount"],
        "attempts": attempts,
        "audit": audit,
    }


@app.post("/api/demo/partial-failure")
def demo_partial_failure():
    with DB_WRITE_LOCK:
        reset_stock_and_orders()

    order = create_order_tx(
        CreateOrderRequest(
            customer_name="Rohan (Split Outage Test)",
            items=[
                CartItem(product_id="PROD-1", qty=1),
                CartItem(product_id="PROD-8", qty=1),
            ],
            reservation_seconds=60,
        )
    )
    order_id = order["order_id"]
    sub_a = next(s for s in order["sub_orders"] if s["store_id"] == "STORE-A")
    sub_c = next(s for s in order["sub_orders"] if s["store_id"] == "STORE-C")

    pay_res = process_payment_tx(
        SimulatePaymentRequest(
            order_id=order_id,
            event_id=f"PAY-{order_id}",
            event_type="PAYMENT_SUCCESS",
        )
    )
    dispatch_res = api_dispatch_sub_order(sub_a["sub_order_id"])
    outage_res = api_toggle_store(
        "STORE-C", ToggleStoreRequest(is_online=False, fail_active_orders=True)
    )

    refund_event_id = f"REF-{sub_c['sub_order_id']}"
    refund_1 = process_payment_tx(
        SimulatePaymentRequest(
            order_id=order_id,
            sub_order_id=sub_c["sub_order_id"],
            event_id=refund_event_id,
            event_type="REFUND",
        )
    )
    refund_2 = process_payment_tx(
        SimulatePaymentRequest(
            order_id=order_id,
            sub_order_id=sub_c["sub_order_id"],
            event_id=refund_event_id,
            event_type="REFUND",
        )
    )

    conn = get_tx_conn()
    try:
        o = dict(conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone())
        audit = run_consistency_audit(conn)
    finally:
        conn.close()

    return {
        "scenario": "Split Order: Electronics Hub Dispatched + General Mart Outage + 2x Refund",
        "order_id": order_id,
        "summary": {
            "total_amount": o["total_amount"],
            "paid_amount": o["paid_amount"],
            "refunded_amount": o["refunded_amount"],
            "net_retained_for_store_a": round(o["paid_amount"] - o["refunded_amount"], 2),
            "final_order_status": o["status"],
        },
        "steps": {
            "payment": pay_res,
            "store_a_dispatch": dispatch_res,
            "store_c_outage": outage_res,
            "refund_attempt_1": refund_1,
            "refund_attempt_2_duplicate": refund_2,
        },
        "audit": audit,
    }