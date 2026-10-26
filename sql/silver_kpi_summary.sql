SELECT
    kpi_status,
    COUNT(*) AS total_records,
    ROUND(
        COUNT(*) * 100.0/
        SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM compliance_analytics.compliance_enriched
GROUP BY kpi_status
ORDER BY kpi_status