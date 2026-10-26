CREATE TABLE compliance_analytics.gold_kpi_summary WITH(
    format='PARQUET',
    external_location='s3://amandataeng-compliance-de-2026/gold/kpi_summary/',
    write_compression='SNAPPY'
) AS
SELECT
    kpi_status,
    COUNT(*) AS total_device,
    ROUND(
        COUNT(*) * 100.0/
        SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM compliance_analytics.compliance_enriched
GROUP BY kpi_status;