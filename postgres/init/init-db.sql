
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.users (
    user_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    signup_date DATE NOT NULL,
    device VARCHAR(50),
    loyalty_tier VARCHAR(50),
    location VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    category VARCHAR(100),
    inventory INT CHECK (inventory >= 0),
    popularity_score DECIMAL(4,2) CHECK (popularity_score >= 0 AND popularity_score <= 1)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id VARCHAR(20) PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    total DECIMAL(10,2) NOT NULL CHECK (total >= 0),
    status VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50),
    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id) REFERENCES raw.users(user_id)
);
