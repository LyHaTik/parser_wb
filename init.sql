-- Таблица остатков
CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    nmId BIGINT NOT NULL,
    stocks INT NOT NULL
);

-- Материализованная витрина динамики продаж
CREATE MATERIALIZED VIEW IF NOT EXISTS sales_mv AS
WITH diffs AS (
    SELECT
        s.nmId,
        s.date::date AS date,
        LAG(s.stocks) OVER (PARTITION BY s.nmId ORDER BY s.date) - s.stocks AS orders
    FROM stocks s
)
SELECT date, nmId, COALESCE(orders, 0) AS orders
FROM diffs
WHERE orders > 0;
