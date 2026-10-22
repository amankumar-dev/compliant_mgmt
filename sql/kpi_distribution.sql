SELECT
    kpi_status,
    outcome,
    COUNT(*) AS total_records
FROM compliance_enriched
WHERE dt='2026-10-06'
GROUP BY kpi_status,outcome
ORDER BY kpi_status,outcome DESC