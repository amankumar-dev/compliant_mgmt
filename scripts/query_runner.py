from scripts.athena_query import run_query

COMPLIANCE_ENRICHED='sql/compliance_enriched.sql'
VERIFY_COMPLIANCE='sql/verify_compliance_data.sql'
DEPT_COMP_REPORT='sql/dept_compliance_report.sql'
KPI_DIST='sql/kpi_distribution.sql'
COMPLIANCE_VALUE='sql/compliance_value.sql'

# Compliance enriched schema
run_query(COMPLIANCE_ENRICHED)

