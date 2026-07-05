"""Generator configuration, read from environment variables.

Every knob has a sane default so `docker compose up` just works, and every
knob is a lever for creating learning scenarios later (traffic spikes,
GDPR deletes, price churn, ...). Override any field with its UPPER_CASE
env var, e.g. ORDERS_PER_MINUTE=120.
"""

import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Config:
    database_url: str = "postgresql://shopstream:shopstream@localhost:5432/shopstream"

    # Main loop cadence
    tick_seconds: float = 1.0

    # Traffic rates (averages; each tick draws from a Poisson distribution)
    orders_per_minute: float = 12.0
    new_customers_per_minute: float = 2.0

    # Mutation rates — these are what make the CDC/SCD2 lessons possible later
    price_changes_per_hour: float = 6.0
    customer_deletes_per_hour: float = 0.5  # GDPR erasure requests

    # Order lifecycle pacing. Real orders take days to deliver; we compress
    # to minutes so status transitions are observable within one sitting.
    lifecycle_min_age_seconds: int = 90

    # Payment outcome mix at checkout
    payment_captured_pct: float = 0.88  # order -> paid immediately
    payment_failed_pct: float = 0.07    # order stays pending (cancelled later)
    # remainder: payment still authorizing; resolved by the lifecycle step

    log_stats_every_seconds: int = 30
    random_seed: int = 0  # 0 = non-deterministic; set >0 for reproducible runs

    # --- Clickstream (generator v2, phase 2+) ---
    # Empty bootstrap = transactions-only mode (how phases 0-1 run it).
    kafka_bootstrap: str = ""
    schema_registry_url: str = "http://schema-registry:8081"
    clickstream_topic: str = "shopstream.events.v1"
    sessions_per_minute: float = 6.0
    anon_session_pct: float = 0.35   # sessions browsing without an account
    bad_event_pct: float = 0.005     # malformed (non-Avro) poison messages
    late_event_pct: float = 0.02     # events stamped up to 5 min in the past


def load_config() -> Config:
    """Build a Config, overriding defaults from UPPER_CASE env vars."""
    overrides = {}
    for f in fields(Config):
        raw = os.environ.get(f.name.upper())
        if raw is None:
            continue
        default = getattr(Config, f.name)
        if isinstance(default, float):
            overrides[f.name] = float(raw)
        elif isinstance(default, int):
            overrides[f.name] = int(raw)
        else:
            overrides[f.name] = raw
    return Config(**overrides)
