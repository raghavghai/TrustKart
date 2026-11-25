# TrustKart — Reliable Multi-Store Payments & Order Fulfilment Hub

Built for **Problem Statement 04 (FinTech): Reliable Multi-Store Payments and Order Fulfilment**.

## Overview
In multi-seller e-commerce, a single customer checkout can span multiple independent stores, each with its own stock and delivery status. Under high demand, network retries, or store outages, systems can accidentally oversell limited stock, double-charge/double-refund customers, or lose track of half-finished orders.

**TrustKart** solves this with a dual-interface web application:
- **Customer Storefront (`/`)** — Mobile-friendly shopping website with real Sign-Up/Login, 3 independent stores, 10 products, live 60-second checkout stock timers, and live split-order tracking.
- **Admin & Operations Hub (`/admin`)** — PIN-protected (`admin123`) laptop dashboard with 2-second live auto-sync, a *Disasters Prevented* scoreboard, registered customer database, store outage controls, chronological order timelines, and a 1-click Stress-Test Lab.

---

## Key Features Mapped to Problem Statement 04

1. **Reserve Without Overselling:** Atomic SQLite `BEGIN IMMEDIATE` write-locked transactions check `total_stock - reserved_stock` and lock inventory in one indivisible step. Abandoned checkouts automatically expire after 60 seconds and release locked stock back to stores.
2. **Split & Track Fulfilment:** Every multi-store cart splits automatically into independent per-store sub-orders (`Electronics Hub`, `Fashion & Clothing`, `General & Lifestyle Mart`) tracking `RESERVED`, `PAID`, `DISPATCHED`, `FAILED_NEEDS_REFUND`, `CANCELLED`, `EXPIRED`, and `REFUNDED`.
3. **Simulated Payment Events & Idempotency:** Every payment and refund carries a unique `event_id` enforced by a database-level `UNIQUE` constraint in `payment_events`. Delayed or duplicate webhook notifications are safely ignored and logged—preventing double charges and double refunds.
4. **Partial Failure Recovery:** Taking a store offline mid-order preserves completed (`DISPATCHED`) shipments from healthy stores, reconciles failed store stock, and queues a single partial refund for the impacted sub-order.
5. **Unresolved Work & Chronological Timelines:** Failed or late-paid sub-orders immediately appear in the Admin **Unresolved Work** queue alongside a complete event-by-event audit timeline (`order_events`).
6. **Live Consistency Verification:** Built-in Stress-Test Lab runs multi-threaded concurrent rushes, 3x duplicate webhooks, and mid-order store crashes, then runs an automated consistency audit across all products and ledgers.

---

## Stores & Product Catalog (3 Stores, 10 Products)

| Store | Products |
| :--- | :--- |
| **Electronics Hub (`STORE-A`)** | Apple iPhone 16 Pro (Stock: 3), Sony WH-1000XM5, Apple AirPods Pro, Samsung 25W Charger |
| **Fashion & Clothing (`STORE-B`)** | Levi's Classic Denim Jacket (Stock: 2), Puma Essential Hoodie, Allen Solly Casual Shirt |
| **General & Lifestyle Mart (`STORE-C`)** | Nike Air Jordan Sneakers (Stock: 3), Milton Thermosteel Bottle, Casio G-Shock Watch |

---

## Tech Stack
- **Backend:** Python 3, FastAPI, Uvicorn, Pydantic
- **Database:** SQLite (`WAL` mode + `BEGIN IMMEDIATE` atomic locking)
- **Frontend:** Responsive Light-Theme HTML5, CSS3, Vanilla JavaScript (zero build step, mobile-ready, offline icon fallbacks)

---

## How to Run Locally (Laptop + Multiple Mobile Phones)

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt