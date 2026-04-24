-- State-level spend with national rank and percentile
WITH state_totals AS (
    SELECT
        state,
        SUM(total_claims)    AS claims,
        SUM(total_drug_cost) AS spend,
        SUM(total_benes)     AS benes
    FROM claims.part_d_prescribers
    GROUP BY state
),
ranked AS (
    SELECT
        state,
        claims,
        spend,
        benes,
        RANK()         OVER (ORDER BY spend DESC)             AS spend_rank,
        PERCENT_RANK() OVER (ORDER BY spend DESC)             AS spend_percentile,
        spend / NULLIF(SUM(spend) OVER (), 0) * 100           AS pct_of_national_spend
    FROM state_totals
)
SELECT
    state,
    claims,
    ROUND(spend, 2)             AS spend,
    spend_rank,
    ROUND(spend_percentile, 4)  AS spend_percentile,
    ROUND(pct_of_national_spend, 3) AS pct_national
FROM ranked
ORDER BY spend_rank;