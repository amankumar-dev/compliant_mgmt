CREATE TABLE compliance_analytics.gold_summary WITH(
    format='PARQUET',
    external_location='s3://amandataeng-compliance-de-2026/gold/control_compliance_summary/',
    write_compression='SNAPPY'
) AS

WITH control_table AS(
    SELECT 'Bitlocker' AS control_name,
        bitlocker_status AS control_status,
        COUNT(*) AS total_devices
    FROM compliance_analytics.compliance_enriched
    GROUP BY bitlocker_status

    UNION ALL

    SELECT 'Antivirus',antivirus_status,COUNT(*)
    FROM compliance_analytics.compliance_enriched
    GROUP BY antivirus_status

    UNION ALL

    SELECT 'Patch',patch_status,COUNT(*)
    FROM compliance_analytics.compliance_enriched
    GROUP BY patch_status

    UNION ALL

    SELECT 'Dlp',dlp_status,COUNT(*)
    FROM compliance_analytics.compliance_enriched
    GROUP BY dlp_status

    UNION ALL

    SELECT 'Proxy',proxy_status,COUNT(*)
    FROM compliance_analytics.compliance_enriched
    GROUP BY proxy_status
)

SELECT 
    control_name,
    control_status,
    total_devices,
    ROUND(
        100.0*total_devices/SUM(total_devices)
        OVER (PARTITION BY control_name),
        2
    ) AS percentage
FROM control_table;
