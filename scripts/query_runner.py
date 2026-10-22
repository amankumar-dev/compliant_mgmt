from scripts.athena_query import run_query

COMPLIANCE_ENRICHED='sql/compliance_enriched.sql'

# Compliance enriched schema
run_query(COMPLIANCE_ENRICHED)

