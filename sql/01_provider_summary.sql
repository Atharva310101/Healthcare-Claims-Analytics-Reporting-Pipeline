-- Provider-level claim and cost summary
SELECT
    npi,
    provider_name,
    state,
    specialty,
    COUNT(DISTINCT generic_name)                    AS unique_drugs,
    SUM(total_claims)                               AS total_claims,
    SUM(total_drug_cost)                            AS total_drug_cost,
    SUM(total_benes)                                AS total_beneficiaries,
    ROUND(SUM(total_drug_cost) / NULLIF(SUM(total_claims), 0), 2)
                                                    AS avg_cost_per_claim
FROM claims.part_d_prescribers
GROUP BY npi, provider_name, state, specialty
ORDER BY total_drug_cost DESC
LIMIT 100;