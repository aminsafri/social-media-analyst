-- Create views for easy querying
CREATE VIEW analytics.reddit_engagement_summary AS
SELECT 
    cd.subreddit,
    dd.year,
    dd.month,
    COUNT(*) as post_count,
    AVG(ef.engagement_score) as avg_engagement,
    SUM(ef.upvotes) as total_upvotes,
    AVG(ef.comments_count) as avg_comments
FROM fact.engagement_fact ef
JOIN dim.content_dim cd ON ef.content_key = cd.content_key
JOIN dim.date_dim dd ON ef.date_key = dd.date_key
GROUP BY cd.subreddit, dd.year, dd.month;

-- Crypto performance view
CREATE VIEW analytics.crypto_daily_performance AS
SELECT 
    cd.name,
    cd.symbol,
    dd.full_date,
    cf.current_price,
    cf.market_cap,
    cf.price_change_percentage_24h
FROM fact.crypto_price_fact cf
JOIN dim.crypto_dim cd ON cf.crypto_key = cd.crypto_key
JOIN dim.date_dim dd ON cf.date_key = dd.date_key
ORDER BY dd.full_date DESC, cd.market_cap_rank;

-- News sentiment by category
CREATE VIEW analytics.news_sentiment_by_category AS
SELECT 
    nd.category,
    dd.year,
    dd.month,
    COUNT(*) as article_count,
    AVG(nf.sentiment_score) as avg_sentiment
FROM fact.news_fact nf
JOIN dim.news_dim nd ON nf.article_key = nd.article_key
JOIN dim.date_dim dd ON nf.date_key = dd.date_key
GROUP BY nd.category, dd.year, dd.month;