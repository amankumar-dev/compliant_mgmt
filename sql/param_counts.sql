SELECT 'BitLocker' AS control_name,
       bitlocker_status AS control_status,
       COUNT(*) AS total_devices
FROM compliance_analytics.compliance_enriched
GROUP BY bitlocker_status

UNION ALL

SELECT 'Antivirus', antivirus_status, COUNT(*)
FROM compliance_analytics.compliance_enriched
GROUP BY antivirus_status

UNION ALL

SELECT 'Patch', patch_status, COUNT(*)
FROM compliance_analytics.compliance_enriched
GROUP BY patch_status

UNION ALL

SELECT 'DLP', dlp_status, COUNT(*)
FROM compliance_analytics.compliance_enriched
GROUP BY dlp_status

UNION ALL

SELECT 'Proxy', proxy_status, COUNT(*)
FROM compliance_analytics.compliance_enriched
GROUP BY proxy_status

ORDER BY control_name, control_status;