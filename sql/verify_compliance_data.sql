SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT asset_id) AS unique_id
    FROM compliance_enriched
    WHERE dt='2026-10-06'

    