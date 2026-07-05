"""The individual behaviors the fictional app performs against Postgres.

Each function is one "thing the app does": sign up a customer, place an
order, move orders through their lifecycle, change a price, erase a
customer. They are all plain SQL inside a transaction — exactly what an
app backend would do — so everything downstream (CDC, snapshots, SCD2)
sees realistic write patterns.
"""

import logging
import random

from faker import Faker
from psycopg import Connection

log = logging.getLogger(__name__)
fake = Faker()

COUNTRIES = ["US", "US", "US", "GB", "DE", "FR", "IL", "NL", "CA", "AU"]
PAYMENT_METHODS = ["card", "card", "card", "paypal", "giftcard"]  # weighted


def create_customer(conn: Connection) -> int:
    """A new user signs up."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO customers (email, full_name, country_code, marketing_opt_in)
            VALUES (%s, %s, %s, %s)
            RETURNING customer_id
            """,
            (
                # uuid fragment guarantees uniqueness; Faker alone collides eventually
                f"{fake.user_name()}.{random.getrandbits(24):x}@example.com",
                fake.name(),
                random.choice(COUNTRIES),
                random.random() < 0.4,
            ),
        )
        return cur.fetchone()[0]


def create_order(
    conn: Connection,
    cfg,
    customer_id: int | None = None,
    product_ids: list[int] | None = None,
) -> int | None:
    """A customer checks out a cart of products.

    Called two ways: directly by the tick loop (random customer, random
    cart — "orders from channels we don't track events for"), and by a
    browsing session that reached purchase (its customer, its cart).

    Prices are read from the products table *inside the same transaction*,
    and copied onto the order item (unit_price_at_purchase) — the classic
    "denormalize the price at time of sale" pattern.
    """
    with conn.transaction(), conn.cursor() as cur:
        if customer_id is None:
            # ORDER BY random() is fine at learning scale; at production
            # scale you'd sample differently (e.g. TABLESAMPLE).
            cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return None
            customer_id = row[0]

        cur.execute(
            "INSERT INTO orders (customer_id) VALUES (%s) RETURNING order_id",
            (customer_id,),
        )
        order_id = cur.fetchone()[0]

        if product_ids:
            cur.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price_at_purchase)
                SELECT %s, p.product_id, 1, p.unit_price
                FROM products p
                WHERE p.product_id = ANY(%s)
                """,
                (order_id, product_ids),
            )
        else:
            # Pick 1-4 distinct products, skewed toward a "popular" subset by
            # sorting on product_id modulo — cheap zipf-ish popularity.
            n_items = random.choices([1, 2, 3, 4], weights=[55, 25, 13, 7])[0]
            cur.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price_at_purchase)
                SELECT %s, p.product_id, 1 + floor(random() * 3)::int, p.unit_price
                FROM products p
                WHERE p.is_active
                ORDER BY (p.product_id %% 7), random()
                LIMIT %s
                """,
                (order_id, n_items),
            )

        # Decrement stock (floor at zero — the app oversells rather than errors;
        # noticing that in the data is a future lesson).
        cur.execute(
            """
            UPDATE inventory i
            SET quantity_on_hand = greatest(i.quantity_on_hand - oi.quantity, 0)
            FROM order_items oi
            WHERE oi.order_id = %s AND oi.product_id = i.product_id
            """,
            (order_id,),
        )

        cur.execute(
            """
            UPDATE orders o
            SET total_amount = t.total
            FROM (
                SELECT sum(quantity * unit_price_at_purchase) AS total
                FROM order_items WHERE order_id = %s
            ) t
            WHERE o.order_id = %s
            """,
            (order_id, order_id),
        )

        # Payment attempt at checkout
        roll = random.random()
        if roll < cfg.payment_captured_pct:
            payment_status, order_status = "captured", "paid"
        elif roll < cfg.payment_captured_pct + cfg.payment_failed_pct:
            payment_status, order_status = "failed", None  # stays pending
        else:
            payment_status, order_status = "authorized", None  # resolves later

        cur.execute(
            """
            INSERT INTO payments (order_id, method, status, amount)
            SELECT order_id, %s, %s, total_amount FROM orders WHERE order_id = %s
            """,
            (random.choice(PAYMENT_METHODS), payment_status, order_id),
        )
        if order_status:
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (order_status, order_id),
            )
        return order_id


def advance_order_lifecycles(conn: Connection, cfg) -> dict[str, int]:
    """Move orders through pending -> paid -> shipped -> delivered / cancelled.

    These UPDATEs are the heart of the phase-2 CDC lesson: a daily snapshot
    of `orders` never sees the intermediate states these writes create.
    """
    moved: dict[str, int] = {}
    min_age = f"{cfg.lifecycle_min_age_seconds} seconds"
    transitions = [
        # (from, to, probability per tick once old enough)
        ("paid", "shipped", 0.20),
        ("shipped", "delivered", 0.15),
    ]
    with conn.transaction(), conn.cursor() as cur:
        for src, dst, prob in transitions:
            cur.execute(
                f"""
                UPDATE orders
                SET status = %s
                WHERE status = %s
                  AND updated_at < now() - interval '{min_age}'
                  AND random() < %s
                """,
                (dst, src, prob),
            )
            if cur.rowcount:
                moved[f"{src}->{dst}"] = cur.rowcount

        # Authorized payments eventually capture -> order becomes paid
        cur.execute(
            """
            WITH captured AS (
                UPDATE payments p
                SET status = 'captured'
                FROM orders o
                WHERE p.order_id = o.order_id
                  AND p.status = 'authorized'
                  AND o.status = 'pending'
                  AND random() < 0.3
                RETURNING p.order_id
            )
            UPDATE orders SET status = 'paid'
            WHERE order_id IN (SELECT order_id FROM captured)
            """
        )
        if cur.rowcount:
            moved["pending->paid"] = cur.rowcount

        # Stale pending orders (failed/abandoned payments) get cancelled
        cur.execute(
            f"""
            UPDATE orders
            SET status = 'cancelled'
            WHERE status = 'pending'
              AND updated_at < now() - interval '{min_age}' * 4
              AND random() < 0.5
            """
        )
        if cur.rowcount:
            moved["pending->cancelled"] = cur.rowcount

        # Restock anything running low, like a merchandising job would
        cur.execute(
            """
            UPDATE inventory
            SET quantity_on_hand = quantity_on_hand + 500
            WHERE quantity_on_hand < 20
            """
        )
        if cur.rowcount:
            moved["restocked"] = cur.rowcount
    return moved


def change_product_price(conn: Connection) -> int | None:
    """Merchandising tweaks a price by ±10%.

    This is the seed of the SCD Type 2 lesson: revenue reports that join
    to the *current* price will silently misstate history.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            UPDATE products
            SET unit_price = greatest(round(unit_price * (0.90 + random() * 0.20), 2), 1.00)
            WHERE product_id = (
                SELECT product_id FROM products WHERE is_active
                ORDER BY random() LIMIT 1
            )
            RETURNING product_id, unit_price
            """
        )
        row = cur.fetchone()
        if row:
            log.info("price change: product_id=%s new_price=%s", row[0], row[1])
            return row[0]
        return None


def gdpr_delete_customer(conn: Connection) -> int | None:
    """A customer invokes their right to erasure: hard DELETE.

    orders.customer_id is ON DELETE SET NULL, so their order history remains
    but is orphaned — downstream pipelines must handle the tombstone.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM customers
            WHERE customer_id = (
                SELECT customer_id FROM customers
                WHERE created_at < now() - interval '5 minutes'
                ORDER BY random() LIMIT 1
            )
            RETURNING customer_id
            """
        )
        row = cur.fetchone()
        if row:
            log.info("GDPR erasure: customer_id=%s deleted", row[0])
            return row[0]
        return None
