CREATE TABLE fact.engagement_fact (
    engagement_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    content_key BIGINT,
    date_key INTEGER,
    upvotes INTEGER,
    comments_count INTEGER,
    engagement_score DOUBLE PRECISION,
    upvote_ratio DOUBLE PRECISION,
    FOREIGN KEY (content_key) REFERENCES dim.content_dim(content_key),
    FOREIGN KEY (date_key) REFERENCES dim.date_dim(date_key)
)
DISTSTYLE KEY DISTKEY(content_key)
SORTKEY(date_key);


-- News Fact Table
CREATE TABLE fact.news_fact (
    news_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    article_key BIGINT,
    date_key INTEGER,
    description_length INTEGER,
    sentiment_score INTEGER,
    FOREIGN KEY (article_key) REFERENCES dim.news_dim(article_key),
    FOREIGN KEY (date_key) REFERENCES dim.date_dim(date_key)
) DISTSTYLE KEY DISTKEY(article_key)
SORTKEY(date_key);

-- Crypto Price Fact Table
CREATE TABLE fact.crypto_price_fact (
    crypto_price_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    crypto_key BIGINT,
    date_key INTEGER,
    current_price DECIMAL(19,2),
    market_cap BIGINT,
    total_volume BIGINT,
    price_change_24h DECIMAL(19,2),
    price_change_percentage_24h DECIMAL(11,2),
    FOREIGN KEY (crypto_key) REFERENCES dim.crypto_dim(crypto_key),
    FOREIGN KEY (date_key) REFERENCES dim.date_dim(date_key)
) DISTSTYLE KEY DISTKEY(crypto_key)
SORTKEY(date_key);