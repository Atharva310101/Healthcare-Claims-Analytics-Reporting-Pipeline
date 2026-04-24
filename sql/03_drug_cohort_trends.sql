-- Ad hoc: top drugs by specialty for a given state
-- Usage: replace :state_param and :specialty_param before running
WITH drug_spend AS (
    SELECT
        generic_name,
        specialty,
        state,
        SUM(total_claims)    AS claims,
        SUM(total_drug_cost) AS spend,
        COUNT(DISTINCT npi)  AS prescriber_count
    FROM claims.part_d_prescribers
    WHERE state     = 'SC'      -- e.g. 'CA'
      AND specialty = 'Internal Medicine'  -- e.g. 'Internal Medicine'
    GROUP BY generic_name, specialty, state
),
drug_ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY specialty ORDER BY spend DESC) AS drug_rank
    FROM drug_spend
)
SELECT * FROM drug_ranked
WHERE drug_rank <= 10
ORDER BY drug_rank;