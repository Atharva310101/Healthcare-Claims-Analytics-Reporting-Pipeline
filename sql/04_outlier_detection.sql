-- Flag providers with statistically aberrant cost-per-claim
WITH provider_stats AS (
    SELECT
        npi,
        provider_name,
        state,
        specialty,
        SUM(total_claims)    AS claims,
        SUM(total_drug_cost) AS spend,
        ROUND(SUM(total_drug_cost) / NULLIF(SUM(total_claims), 0), 2)
                             AS cost_per_claim
    FROM claims.part_d_prescribers
    GROUP BY npi, provider_name, state, specialty
    HAVING SUM(total_claims) > 100  -- exclude low-volume noise
),
distribution AS (
    SELECT
        AVG(cost_per_claim)    AS mean_cpc,
        STDDEV(cost_per_claim) AS std_cpc
    FROM provider_stats
)
SELECT
    p.npi,
    p.provider_name,
    p.state,
    p.specialty,
    p.cost_per_claim,
    d.mean_cpc,
    d.std_cpc,
    ROUND((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0), 3)
                             AS z_score,
    CASE
        WHEN ABS((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0)) > 3
            THEN 'HIGH OUTLIER'
        WHEN ABS((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0)) > 2
            THEN 'MODERATE OUTLIER'
        ELSE 'NORMAL'
    END AS aberrant_flag
FROM provider_stats p, distribution d
WHERE ABS((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0)) > 2
ORDER BY ABS((p.cost_per_claim - d.mean_cpc) / NULLIF(d.std_cpc, 0)) DESC
LIMIT 100;;