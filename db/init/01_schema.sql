-- ShopStream OLTP schema (PostgreSQL)
--
-- Modeled the way a backend team would model it: normalized, optimized for
-- the app's reads/writes — NOT for analytics. The analytics-unfriendly bits
-- are deliberate learning material:
--   * orders.status mutates in place  -> naive daily extracts miss states (needs CDC)
--   * products.unit_price mutates     -> historical revenue needs SCD2 in the warehouse
--   * customers can be deleted (GDPR) -> downstream must handle tombstones

-- ---------------------------------------------------------------------------
-- updated_at maintenance: classic Postgres trigger pattern
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------
CREATE TABLE customers (
    customer_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email            text        NOT NULL UNIQUE,
    full_name        text        NOT NULL,
    country_code     char(2)     NOT NULL,
    marketing_opt_in boolean     NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku         text          NOT NULL UNIQUE,
    name        text          NOT NULL,
    category    text          NOT NULL,
    subcategory text          NOT NULL,
    unit_price  numeric(10,2) NOT NULL CHECK (unit_price >= 0),
    is_active   boolean       NOT NULL DEFAULT true,
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    product_id       bigint      PRIMARY KEY REFERENCES products (product_id),
    quantity_on_hand integer     NOT NULL CHECK (quantity_on_hand >= 0),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TYPE order_status AS ENUM
    ('pending', 'paid', 'shipped', 'delivered', 'cancelled');

CREATE TABLE orders (
    order_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- SET NULL: a GDPR-erased customer leaves orphaned orders behind.
    -- Downstream pipelines have to decide what an order without a customer means.
    customer_id  bigint        REFERENCES customers (customer_id) ON DELETE SET NULL,
    status       order_status  NOT NULL DEFAULT 'pending',
    currency     char(3)       NOT NULL DEFAULT 'USD',
    total_amount numeric(12,2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    created_at   timestamptz   NOT NULL DEFAULT now(),
    updated_at   timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id               bigint        NOT NULL REFERENCES orders (order_id),
    product_id             bigint        NOT NULL REFERENCES products (product_id),
    quantity               integer       NOT NULL CHECK (quantity > 0),
    -- Denormalized on purpose: the price the customer actually paid,
    -- immune to later price changes on the product row.
    unit_price_at_purchase numeric(10,2) NOT NULL
);

CREATE TYPE payment_method AS ENUM ('card', 'paypal', 'giftcard');
CREATE TYPE payment_status AS ENUM ('authorized', 'captured', 'failed', 'refunded');

CREATE TABLE payments (
    payment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id   bigint         NOT NULL REFERENCES orders (order_id),
    method     payment_method NOT NULL,
    status     payment_status NOT NULL,
    amount     numeric(12,2)  NOT NULL,
    created_at timestamptz    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Indexes the app would need (FK lookups & status scans)
-- ---------------------------------------------------------------------------
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_status      ON orders (status) WHERE status NOT IN ('delivered', 'cancelled');
CREATE INDEX idx_order_items_order  ON order_items (order_id);
CREATE INDEX idx_payments_order     ON payments (order_id);

-- ---------------------------------------------------------------------------
-- updated_at triggers
-- ---------------------------------------------------------------------------
CREATE TRIGGER trg_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_inventory_updated_at BEFORE UPDATE ON inventory
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
