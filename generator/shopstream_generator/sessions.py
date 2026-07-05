"""Browsing sessions: little state machines that walk the shopping funnel.

    browse (page/product views, searches)
      -> add_to_cart        (some sessions)
        -> begin_checkout   (some carts)
          -> purchase       (most checkouts; creates a REAL order in Postgres)

Sessions step every few seconds — not every tick — so per-session event
gaps look human. A purchase emits the order_id it created, which later
lets us reconcile the event stream against the OLTP source of truth
(a classic data-quality exercise).
"""

import random
import uuid

DEVICES = [
    {"type": "mobile", "os": "iOS", "browser": "Safari"},
    {"type": "mobile", "os": "Android", "browser": "Chrome"},
    {"type": "desktop", "os": "Windows", "browser": "Chrome"},
    {"type": "desktop", "os": "macOS", "browser": "Safari"},
    {"type": "desktop", "os": "Linux", "browser": "Firefox"},
    {"type": "tablet", "os": "iPadOS", "browser": "Safari"},
]
REFERRERS = [None, None, None, "https://google.com", "https://instagram.com",
             "https://newsletter.shopstream.example"]
SEARCH_TERMS = ["headphones", "lamp", "yoga", "hoodie", "coffee", "backpack",
                "gift", "speaker", "monitor", "wallet"]

# Funnel odds per step while browsing
P_ADD_TO_CART = 0.18
P_REMOVE_FROM_CART = 0.05
P_CHECKOUT_WHEN_CARTED = 0.45
P_PURCHASE_AT_CHECKOUT = 0.75


class Session:
    def __init__(self, customer_id: int | None, product_ids: list[int], now: float):
        self.session_id = str(uuid.uuid4())
        self.customer_id = customer_id  # None = anonymous
        self.device = random.choice(DEVICES)
        self.referrer = random.choice(REFERRERS)
        self.product_ids = product_ids
        self.cart: list[int] = []
        self.state = "browsing"
        self.steps_left = random.randint(3, 12)
        self.next_step_at = now + random.uniform(1, 5)
        self.done = False

    def due(self, now: float) -> bool:
        return not self.done and now >= self.next_step_at

    def step(self, now: float) -> dict | None:
        """Advance one step; returns an event spec (or None when finished).

        A spec of type 'purchase' is a *request* — main.py creates the order
        in Postgres first, then emits the event with the real order_id.
        """
        from .events import base_event  # local import to avoid cycle

        self.next_step_at = now + random.uniform(2, 15)
        self.steps_left -= 1
        if self.steps_left <= 0:
            self.done = True  # wandered off; the eternal abandoned cart
            return None

        if self.state == "browsing":
            roll = random.random()
            if self.cart and roll < P_CHECKOUT_WHEN_CARTED / 3:
                self.state = "checkout"
                return base_event(self, "begin_checkout", "/checkout",
                                  {"cart_size": len(self.cart)})
            if roll < P_ADD_TO_CART and self.product_ids:
                pid = random.choice(self.product_ids)
                self.cart.append(pid)
                return base_event(self, "add_to_cart", f"/product/{pid}",
                                  {"product_id": pid, "cart_size": len(self.cart)})
            if self.cart and roll < P_ADD_TO_CART + P_REMOVE_FROM_CART:
                pid = self.cart.pop()
                return base_event(self, "remove_from_cart", f"/product/{pid}",
                                  {"product_id": pid, "cart_size": len(self.cart)})
            if roll < 0.5:
                pid = random.choice(self.product_ids)
                return base_event(self, "product_view", f"/product/{pid}",
                                  {"product_id": pid})
            if roll < 0.65:
                q = random.choice(SEARCH_TERMS)
                return base_event(self, "search", f"/search?q={q}",
                                  {"search_query": q})
            return base_event(self, "page_view",
                              random.choice(["/", "/deals", "/category/electronics",
                                             "/category/home", "/about"]))

        if self.state == "checkout":
            self.done = True
            if random.random() < P_PURCHASE_AT_CHECKOUT:
                return base_event(self, "purchase", "/checkout/complete",
                                  {"cart_size": len(self.cart)})
            return base_event(self, "page_view", "/cart")  # got cold feet

        return None
