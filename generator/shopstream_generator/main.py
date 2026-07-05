"""Main loop: ticks once a second (by default) and rolls dice for each
kind of app activity, so traffic looks organic rather than metronomic.
"""

import logging
import math
import random
import signal
import sys
import time

import psycopg

from . import actions
from .config import Config, load_config

log = logging.getLogger("generator")


def poisson(lam: float) -> int:
    """Draw from Poisson(lam) — Knuth's algorithm, fine for small lambdas."""
    if lam <= 0:
        return 0
    threshold = math.exp(-lam)
    k, p = 0, 1.0
    while True:
        p *= random.random()
        if p <= threshold:
            return k
        k += 1


def connect_with_retry(cfg: Config, attempts: int = 30) -> psycopg.Connection:
    """Postgres may still be booting when the container starts."""
    for attempt in range(1, attempts + 1):
        try:
            conn = psycopg.connect(cfg.database_url, autocommit=True)
            log.info("connected to Postgres")
            return conn
        except psycopg.OperationalError as exc:
            log.warning("Postgres not ready (attempt %d/%d): %s", attempt, attempts, exc)
            time.sleep(2)
    raise SystemExit("could not connect to Postgres, giving up")


def run(cfg: Config) -> None:
    conn = connect_with_retry(cfg)
    per_tick = cfg.tick_seconds / 60.0  # rate/minute -> rate/tick
    per_tick_hourly = cfg.tick_seconds / 3600.0

    # Clickstream mode (v2) switches on when a Kafka bootstrap is configured
    producer = None
    sessions: list = []
    product_ids: list[int] = []
    if cfg.kafka_bootstrap:
        from .events import ClickstreamProducer

        producer = ClickstreamProducer(cfg)
        log.info("clickstream enabled -> %s (topic %s)",
                 cfg.kafka_bootstrap, cfg.clickstream_topic)

    stats = {"orders": 0, "customers": 0, "price_changes": 0, "gdpr_deletes": 0,
             "events": 0, "sessions": 0}
    last_stats_at = time.monotonic()

    running = True

    def stop(signum, _frame):
        nonlocal running
        log.info("received signal %s, shutting down", signum)
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info(
        "generating traffic: %.1f orders/min, %.1f signups/min, tick=%.1fs",
        cfg.orders_per_minute, cfg.new_customers_per_minute, cfg.tick_seconds,
    )

    while running:
        tick_started = time.monotonic()
        try:
            for _ in range(poisson(cfg.new_customers_per_minute * per_tick)):
                actions.create_customer(conn)
                stats["customers"] += 1

            for _ in range(poisson(cfg.orders_per_minute * per_tick)):
                if actions.create_order(conn, cfg):
                    stats["orders"] += 1

            for _ in range(poisson(cfg.price_changes_per_hour * per_tick_hourly)):
                if actions.change_product_price(conn):
                    stats["price_changes"] += 1

            for _ in range(poisson(cfg.customer_deletes_per_hour * per_tick_hourly)):
                if actions.gdpr_delete_customer(conn):
                    stats["gdpr_deletes"] += 1

            moved = actions.advance_order_lifecycles(conn, cfg)
            if moved:
                log.debug("lifecycle: %s", moved)

            if producer:
                from .sessions import Session

                now = time.monotonic()
                if not product_ids:  # cached once; catalog is static enough
                    with conn.cursor() as cur:
                        cur.execute("SELECT product_id FROM products WHERE is_active")
                        product_ids = [r[0] for r in cur.fetchall()]

                for _ in range(poisson(cfg.sessions_per_minute * per_tick)):
                    customer_id = None
                    if random.random() >= cfg.anon_session_pct:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT customer_id FROM customers ORDER BY random() LIMIT 1"
                            )
                            row = cur.fetchone()
                            customer_id = row[0] if row else None
                    sessions.append(Session(customer_id, product_ids, now))
                    stats["sessions"] += 1

                for s in sessions:
                    if not s.due(now):
                        continue
                    event = s.step(now)
                    if event is None:
                        continue
                    if event["event_type"] == "purchase":
                        # Anonymous shoppers create an account at checkout
                        if s.customer_id is None:
                            s.customer_id = actions.create_customer(conn)
                            event["customer_id"] = s.customer_id
                            stats["customers"] += 1
                        order_id = actions.create_order(
                            conn, cfg, customer_id=s.customer_id,
                            product_ids=s.cart,
                        )
                        event["properties"]["order_id"] = str(order_id)
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT total_amount FROM orders WHERE order_id = %s",
                                (order_id,),
                            )
                            event["properties"]["order_value"] = str(cur.fetchone()[0])
                        stats["orders"] += 1
                    producer.emit(event)
                    stats["events"] += 1
                sessions = [s for s in sessions if not s.done]

        except psycopg.OperationalError as exc:
            log.error("lost Postgres connection (%s), reconnecting", exc)
            conn = connect_with_retry(cfg)

        if time.monotonic() - last_stats_at >= cfg.log_stats_every_seconds:
            log.info(
                "last %ds: %d orders, %d signups, %d price changes, "
                "%d GDPR deletes, %d sessions started, %d events emitted",
                cfg.log_stats_every_seconds,
                stats["orders"], stats["customers"],
                stats["price_changes"], stats["gdpr_deletes"],
                stats["sessions"], stats["events"],
            )
            stats = dict.fromkeys(stats, 0)
            last_stats_at = time.monotonic()

        # Sleep whatever remains of the tick
        elapsed = time.monotonic() - tick_started
        time.sleep(max(cfg.tick_seconds - elapsed, 0))

    if producer:
        producer.flush()
    conn.close()
    log.info("generator stopped cleanly")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    cfg = load_config()
    if cfg.random_seed:
        random.seed(cfg.random_seed)
    run(cfg)


if __name__ == "__main__":
    main()
