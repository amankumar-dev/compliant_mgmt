
import sys
import time
import boto3

AWS_REGION = "us-east-1"
DATABASE = "compliance_analytics"
WORKGROUP = "primary"
ATHENA_OUTPUT = (
    "s3://amandataeng-compliance-de-2026/athena-results/"
)

athena = boto3.client("athena", region_name=AWS_REGION)


def execute_query(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(
            QueryExecutionId=query_id
        )
        status = result["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            return

        if status in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(
                f"Athena query {status}: {reason}"
            )

        time.sleep(2)


def refresh_gold_views():
    print("Creating/updating live Gold KPI view...")

    execute_query("""
        CREATE OR REPLACE VIEW
        compliance_analytics.gold_kpi_summary_live AS
        SELECT
            kpi_status,
            COUNT(*) AS total_device,
            ROUND(
                COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER (),
                2
            ) AS percentage
        FROM compliance_analytics.compliance_enriched
        GROUP BY kpi_status
    """)

    print("Creating/updating live Gold control view...")

    execute_query("""
        CREATE OR REPLACE VIEW
        compliance_analytics.gold_summary_live AS

        WITH control_table AS (
            SELECT
                'Bitlocker' AS control_name,
                bitlocker_status AS control_status,
                COUNT(*) AS total_devices
            FROM compliance_analytics.compliance_enriched
            GROUP BY bitlocker_status

            UNION ALL

            SELECT
                'Antivirus',
                antivirus_status,
                COUNT(*)
            FROM compliance_analytics.compliance_enriched
            GROUP BY antivirus_status

            UNION ALL

            SELECT
                'Patch',
                patch_status,
                COUNT(*)
            FROM compliance_analytics.compliance_enriched
            GROUP BY patch_status

            UNION ALL

            SELECT
                'Dlp',
                dlp_status,
                COUNT(*)
            FROM compliance_analytics.compliance_enriched
            GROUP BY dlp_status

            UNION ALL

            SELECT
                'Proxy',
                proxy_status,
                COUNT(*)
            FROM compliance_analytics.compliance_enriched
            GROUP BY proxy_status
        )

        SELECT
            control_name,
            control_status,
            total_devices,
            ROUND(
                100.0 * total_devices /
                SUM(total_devices) OVER (
                    PARTITION BY control_name
                ),
                2
            ) AS percentage
        FROM control_table
    """)

    print("GOLD VIEW REFRESH SUCCESSFUL")
    print("Live views are ready for validation and reporting.")


if __name__ == "__main__":
    try:
        refresh_gold_views()
    except Exception as exc:
        print(f"GOLD REFRESH FAILED: {exc}")
        sys.exit(1)
