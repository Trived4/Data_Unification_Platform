-- =========================================
-- QUERIES USED BEHIND EACH BUSINESS METRIC
-- =========================================

-- =========================================
-- REVENUE BY LOCATION (simple)
-- =========================================

SELECT
    dl.location_code,
    dl.location_name,
    SUM(fh.total_amount) AS total_revenue
FROM fact_transaction_header fh
JOIN dim_location dl
  ON fh.location_sk = dl.location_sk
GROUP BY dl.location_code, dl.location_name
ORDER BY total_revenue DESC;


-- =========================================
-- REVENUE BY LOCATION (detailed: performance metric)
-- =========================================

WITH dedup_tx AS (
    SELECT DISTINCT
           pos_system,
           pos_transaction_id,
           location_sk,
           customer_sk,
           total_amount
    FROM fact_transaction_header
)
SELECT
    d.location_name,
    d.location_code,
    SUM(f.total_amount)                    AS total_revenue,
    COUNT(DISTINCT f.customer_sk)          AS unique_customers,
    CASE WHEN COUNT(DISTINCT f.customer_sk) = 0
         THEN NULL
         ELSE SUM(f.total_amount) * 1.0 / COUNT(DISTINCT f.customer_sk)
    END                                    AS revenue_per_customer
FROM dedup_tx f
JOIN dim_location d ON f.location_sk = d.location_sk
GROUP BY d.location_name, d.location_code
ORDER BY revenue_per_customer DESC
-- Optional: used by /analysis/revenue_by_location to return top N locations
LIMIT ?;


-- =========================================
-- SERVICE POPULARITY
-- =========================================

-- Overall
SELECT
    ds.service_name,
    SUM(fli.quantity) AS total_quantity
FROM fact_transaction_line_item fli
JOIN dim_service ds
  ON fli.service_sk = ds.service_sk
GROUP BY ds.service_name
ORDER BY total_quantity DESC;

-- By location
SELECT
    dl.location_code,
    ds.service_name,
    SUM(fli.quantity) AS total_quantity
FROM fact_transaction_line_item fli
JOIN fact_transaction_header fh
  ON fli.transaction_header_sk = fh.transaction_header_sk
JOIN dim_location dl
  ON fh.location_sk = dl.location_sk
JOIN dim_service ds
  ON fli.service_sk = ds.service_sk
GROUP BY dl.location_code, ds.service_name
ORDER BY dl.location_code, total_quantity DESC;


-- =========================================
-- TOP SERVICES BY LOCATION
-- =========================================

WITH svc_agg AS (
    SELECT
        d.location_name,
        s.service_name,
        SUM(li.quantity)     AS total_quantity,
        SUM(li.line_amount)  AS total_revenue
    FROM fact_transaction_line_item li
    JOIN fact_transaction_header h
      ON li.transaction_header_sk = h.transaction_header_sk
    JOIN dim_location d
      ON h.location_sk = d.location_sk
    JOIN dim_service s
      ON li.service_sk = s.service_sk
    GROUP BY d.location_name, s.service_name
),
ranked AS (
    SELECT
        location_name,
        service_name,
        total_quantity,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY location_name
            ORDER BY total_quantity DESC
        ) AS rn
    FROM svc_agg
)
SELECT
    location_name,
    service_name,
    total_quantity,
    total_revenue,
    rn AS rank_within_location
FROM ranked
WHERE rn <= ?
ORDER BY location_name, rn;


-- =========================================
-- CUSTOMER ENGAGEMENT BY MEMBERSHIP TIER
-- =========================================

SELECT
    dmt.tier_name,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT fh.customer_sk) AS distinct_customers,
    AVG(fh.total_amount) AS avg_transaction_value
FROM fact_transaction_header fh
JOIN dim_membership_tier dmt
  ON fh.membership_tier_sk = dmt.membership_tier_sk
WHERE fh.customer_sk IS NOT NULL
GROUP BY dmt.tier_name
ORDER BY transaction_count DESC;


-- =========================================
-- CUSTOMER FREQUENCY BY MEMBERSHIP
-- =========================================

SELECT
    COALESCE(mt.tier_name, 'None')                       AS membership_tier,
    COUNT(DISTINCT h.customer_sk)                        AS unique_customers,
    COUNT(*)                                             AS transactions,
    CAST(COUNT(*) AS REAL) /
      NULLIF(COUNT(DISTINCT h.customer_sk), 0)           AS avg_visits_per_customer
FROM fact_transaction_header h
LEFT JOIN dim_membership_tier mt
  ON h.membership_tier_sk = mt.membership_tier_sk
GROUP BY membership_tier
ORDER BY transactions DESC;


-- =========================================
-- MEMBERSHIP SERVICE MIX
-- =========================================

SELECT
    COALESCE(mt.tier_name, 'None')                       AS membership_tier,
    s.service_name,
    SUM(li.quantity)                                     AS total_quantity,
    SUM(li.line_amount)                                  AS total_spend
FROM fact_transaction_line_item li
JOIN fact_transaction_header h
  ON li.transaction_header_sk = h.transaction_header_sk
JOIN dim_service s
  ON li.service_sk = s.service_sk
LEFT JOIN dim_membership_tier mt
  ON h.membership_tier_sk = mt.membership_tier_sk
GROUP BY membership_tier, s.service_name
ORDER BY membership_tier, total_spend DESC;
