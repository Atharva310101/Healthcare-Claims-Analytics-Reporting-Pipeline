-- schema.sql
CREATE SCHEMA IF NOT EXISTS claims;

CREATE TABLE claims.part_d_prescribers (
    row_id          SERIAL PRIMARY KEY,
    npi             VARCHAR(10),
    provider_name   VARCHAR(100),
    city            VARCHAR(50),
    state           CHAR(2),
    specialty       VARCHAR(100),
    brand_name      VARCHAR(100),
    generic_name    VARCHAR(100),
    total_claims    NUMERIC(12,2),
    total_drug_cost NUMERIC(14,2),
    total_benes     NUMERIC(12,2),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_npi       ON claims.part_d_prescribers(npi);
CREATE INDEX idx_state     ON claims.part_d_prescribers(state);
CREATE INDEX idx_specialty ON claims.part_d_prescribers(specialty);
CREATE INDEX idx_generic   ON claims.part_d_prescribers(generic_name);