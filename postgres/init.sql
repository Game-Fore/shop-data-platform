CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    full_name   TEXT NOT NULL,
    city        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    price       NUMERIC(10,2) NOT NULL
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    status      TEXT NOT NULL DEFAULT 'new',  -- new -> paid -> shipped -> delivered | cancelled
    total       NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id  INT NOT NULL REFERENCES products(id),
    quantity    INT NOT NULL,
    price       NUMERIC(10,2) NOT NULL
);

ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Назначения клиентов в группы A/B-экспериментов (exposure log).
-- Пишется в момент назначения и больше не меняется; группа клиента в эксперименте одна.
CREATE TABLE experiment_assignments (
    experiment_id TEXT NOT NULL,
    customer_id   INT  NOT NULL REFERENCES customers(id),
    variant       TEXT NOT NULL CHECK (variant IN ('control', 'treatment')),
    assigned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (experiment_id, customer_id)
);
ALTER TABLE experiment_assignments REPLICA IDENTITY FULL;

INSERT INTO products (name, category, price) VALUES
  ('Laptop', 'electronics', 1200.00),
  ('Headphones', 'electronics', 150.00),
  ('Coffee Maker', 'home', 89.90),
  ('Desk Lamp', 'home', 35.50),
  ('Running Shoes', 'sport', 110.00),
  ('Yoga Mat', 'sport', 25.00),
  ('Novel', 'books', 15.99),
  ('Cookbook', 'books', 29.99);