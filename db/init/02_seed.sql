-- Seed data: a reproducible product catalog + a starter set of customers.
-- setseed() makes random() deterministic so every fresh environment gets
-- the same catalog — handy when comparing query results while learning.
SELECT setseed(0.42);

-- ---------------------------------------------------------------------------
-- ~120 products built from category/adjective/noun word banks
-- ---------------------------------------------------------------------------
WITH categories(category, subcategory, noun, base_price) AS (
    VALUES
        ('electronics', 'audio',       'Headphones',     79.00),
        ('electronics', 'audio',       'Speaker',        59.00),
        ('electronics', 'computing',   'Keyboard',       49.00),
        ('electronics', 'computing',   'Mouse',          29.00),
        ('electronics', 'computing',   'Monitor',       199.00),
        ('electronics', 'mobile',      'Phone Case',     19.00),
        ('electronics', 'mobile',      'Charger',        24.00),
        ('home',        'kitchen',     'Coffee Maker',   89.00),
        ('home',        'kitchen',     'Knife Set',      69.00),
        ('home',        'decor',       'Table Lamp',     39.00),
        ('home',        'decor',       'Wall Clock',     27.00),
        ('sports',      'fitness',     'Yoga Mat',       25.00),
        ('sports',      'fitness',     'Dumbbell Set',   79.00),
        ('sports',      'outdoor',     'Water Bottle',   15.00),
        ('sports',      'outdoor',     'Backpack',       55.00),
        ('fashion',     'accessories', 'Sunglasses',     35.00),
        ('fashion',     'accessories', 'Wallet',         30.00),
        ('fashion',     'apparel',     'T-Shirt',        18.00),
        ('fashion',     'apparel',     'Hoodie',         45.00),
        ('books',       'nonfiction',  'Cookbook',       22.00)
),
adjectives(adjective, ord) AS (
    VALUES ('Aurora', 1), ('Titan', 2), ('Nimbus', 3),
           ('Ember', 4), ('Vertex', 5), ('Solstice', 6)
)
INSERT INTO products (sku, name, category, subcategory, unit_price)
SELECT
    upper(left(c.category, 3)) || '-' ||
        upper(left(a.adjective, 3)) || '-' ||
        lpad((row_number() OVER ())::text, 4, '0')          AS sku,
    a.adjective || ' ' || c.noun                            AS name,
    c.category,
    c.subcategory,
    round((c.base_price * (0.8 + random() * 0.5))::numeric, 2) AS unit_price
FROM categories c
CROSS JOIN adjectives a;

-- Every product starts with stock on the shelf
INSERT INTO inventory (product_id, quantity_on_hand)
SELECT product_id, 200 + floor(random() * 800)::int
FROM products;

-- ---------------------------------------------------------------------------
-- 25 starter customers (the generator creates the rest over time)
-- ---------------------------------------------------------------------------
INSERT INTO customers (email, full_name, country_code, marketing_opt_in)
SELECT
    'seed.customer.' || i || '@example.com',
    'Seed Customer ' || i,
    (ARRAY['US','US','US','GB','DE','FR','IL','NL','CA','AU'])[1 + floor(random() * 10)::int],
    random() < 0.4
FROM generate_series(1, 25) AS i;
