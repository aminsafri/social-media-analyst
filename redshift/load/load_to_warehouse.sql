-- Load Date Dimension
COPY dim.date_dim
FROM 's3://<BUCKET NAME>/processed-data/dim_date/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;

-- Load Content Dimension
COPY dim.content_dim
FROM 's3://<BUCKET NAME>/processed-data/dim_content/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;

-- Load News Dimension
COPY dim.news_dim
FROM 's3://<BUCKET NAME>/processed-data/dim_news/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;

-- Load Crypto Dimension
COPY dim.crypto_dim
FROM 's3://<BUCKET NAME>/processed-data/dim_crypto/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;

-- Load Engagement Fact
COPY fact.engagement_fact (content_key, date_key, upvotes, comments_count, engagement_score, upvote_ratio)
FROM 's3://<BUCKET NAME>/processed-data/fact_engagement/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;

-- Load News Fact
COPY fact.news_fact (article_key, date_key, description_length, sentiment_score)
FROM 's3://<BUCKET NAME>/processed-data/fact_news/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;

-- Load Crypto Price Fact
COPY fact.crypto_price_fact (crypto_key, date_key, current_price, market_cap, total_volume, price_change_24h, price_change_percentage_24h)
FROM 's3://<BUCKET NAME>/processed-data/fact_crypto_prices/'
IAM_ROLE 'arn:aws:iam::<USERID>:role/RedshiftS3AccessRole'
FORMAT AS PARQUET;