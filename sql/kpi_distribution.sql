SELECT
    outcome,
    kpi_status,
    COUNT(*) AS total_records
FROM compliance_analytics.compliance_enriched
WHERE outcome = 'STALE_REVALIDATE'
GROUP BY outcome, kpi_status
ORDER BY kpi_status;
