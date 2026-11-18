-- Run with: python -m src.report  (registers the parquet tables as views, then runs these)

-- 1. Top streamers by gift revenue
SELECT f.streamer_id, s.streamer_name,
       SUM(f.gift_value_coins) AS gift_coins,
       COUNT(*)                AS gifts
FROM fact_live_events f
JOIN dim_streamer s USING (streamer_id)
WHERE f.event_type = 'gift_sent'
GROUP BY f.streamer_id, s.streamer_name
ORDER BY gift_coins DESC
LIMIT 5;

-- 2. Engagement mix by country (share of each event type)
SELECT country,
       COUNT(*) AS events,
       ROUND(100.0 * SUM(CASE WHEN event_type = 'like' THEN 1 ELSE 0 END) / COUNT(*), 1) AS like_pct,
       ROUND(100.0 * SUM(CASE WHEN event_type = 'comment' THEN 1 ELSE 0 END) / COUNT(*), 1) AS comment_pct,
       ROUND(100.0 * SUM(CASE WHEN event_type = 'gift_sent' THEN 1 ELSE 0 END) / COUNT(*), 1) AS gift_pct
FROM fact_live_events
GROUP BY country
ORDER BY events DESC;

-- 3. Busiest minute per streamer (window function over the streaming aggregate)
SELECT streamer_id, window_start, views, gift_coins
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY streamer_id ORDER BY views DESC, window_start) AS rn
  FROM agg_streamer_minute
)
WHERE rn = 1
ORDER BY views DESC
LIMIT 5;

-- 4. Data-quality summary: what the pipeline kept out of the warehouse
SELECT dq_reason, COUNT(*) AS records
FROM quarantine
GROUP BY dq_reason
ORDER BY records DESC;
