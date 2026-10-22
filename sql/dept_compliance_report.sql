SELECT
    department,
    COUNT(*) AS total_records,
    SUM(
        CASE
            WHEN kpi_status='Non-Compliant' THEN 1
            ELSE 0
        END
    ) AS non_compliant_devices,
    SUM(
        CASE
            WHEN kpi_status='Excluded' THEN 1
            ELSE 0
        END
    ) AS excluded_records,
    SUM(
        CASE
            WHEN kpi_status='Compliant' THEN 1
            ELSE 0
        END
    ) AS compliant_devices
FROM compliance_enriched
WHERE dt='2026-10-06'
GROUP BY department
ORDER BY total_records DESC;

