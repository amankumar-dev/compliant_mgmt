SELECT
    overall_compliance,
    recomputed_compliance,
    flag_mismatch,
    COUNT(*) AS total_records
FROM compliance_enriched
WHERE dt='2026-10-06'
GROUP BY overall_compliance,recomputed_compliance,flag_mismatch
ORDER BY total_records DESC