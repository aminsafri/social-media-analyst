-- Date Dimension
CREATE TABLE dim.date_dim (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
) DISTSTYLE ALL;

-- Content Dimension (Reddit)
CREATE TABLE dim.content_dim (
    content_key BIGINT PRIMARY KEY,
    post_id VARCHAR(50),
    title VARCHAR(500),
    subreddit VARCHAR(100),
    author VARCHAR(100),
    content_type VARCHAR(50),
    created_at TIMESTAMP
) DISTSTYLE KEY DISTKEY(content_key);

-- News Dimension
CREATE TABLE dim.news_dim (
    article_key BIGINT PRIMARY KEY,
    title VARCHAR(500),
    description VARCHAR(1000),
    source_name VARCHAR(200),
    author VARCHAR(200),
    category VARCHAR(50),
    country VARCHAR(10),
    published_at TIMESTAMP
) DISTSTYLE KEY DISTKEY(article_key);

-- Crypto Dimension
CREATE TABLE dim.crypto_dim (
    crypto_key INTEGER PRIMARY KEY,
    coin_id VARCHAR(50),
    symbol VARCHAR(10),
    name VARCHAR(100),
    market_cap_rank INTEGER
) DISTSTYLE ALL;
