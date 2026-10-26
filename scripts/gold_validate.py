
import boto3
import time
import pandas as pd

DATABASE = "compliance_analytics"
REGION = "us-east-1"
WORKGROUP = "primary"

KPI_TABLE = "gold_kpi_summary"
CONTROL_TABLE = "gold_summary"

EXPECTED_DEVICES = 1200

athena = boto3.client("athena", region_name=REGION)


def run_query(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )

    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        status = result["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break

        if status in ["FAILED", "CANCELLED"]:
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(
                f"Athena query {status}: {reason}"
            )

        time.sleep(2)

    rows = []
    next_token = None

    while True:
        args = {"QueryExecutionId": query_id}

        if next_token:
            args["NextToken"] = next_token

        page = athena.get_query_results(**args)
        rows.extend(page["ResultSet"]["Rows"])

        next_token = page.get("NextToken")

        if not next_token:
            break

    if not rows:
        return pd.DataFrame()

    headers = [cell.get("VarCharValue", "") for cell in rows[0]["Data"]]

    data = [
        [cell.get("VarCharValue") for cell in row["Data"]]
        for row in rows[1:]
    ]

    return pd.DataFrame(data, columns=headers)


def gold_validate():
    print("=" * 55)
    print("GOLD DATA INTEGRITY VALIDATION")
    print("=" * 55)

    print("\n1. KPI summary validation")

    kpi = run_query(
        f"SELECT * FROM {KPI_TABLE}"
    )

    if kpi.empty:
        print("FAIL: KPI table returned no rows.")
    else:
        kpi["total_device"] = pd.to_numeric(
            kpi["total_device"], errors="coerce"
        )

        total = kpi["total_device"].sum()

        print("KPI records:", len(kpi))
        print("Total devices:", total)
        print("Expected devices:", EXPECTED_DEVICES)
        print(
            "Result:",
            "PASS" if total == EXPECTED_DEVICES else "FAIL"
        )

        print("\nKPI distribution:")
        print(kpi.to_string(index=False))

    print("\n2. Control-wise summary validation")

    controls = run_query(
        f"SELECT * FROM {CONTROL_TABLE}"
    )

    if controls.empty:
        print("FAIL: Control summary table returned no rows.")
        return

    controls["total_devices"] = pd.to_numeric(
        controls["total_devices"], errors="coerce"
    )

    controls["percentage"] = pd.to_numeric(
        controls["percentage"], errors="coerce"
    )

    expected_controls = {
        "bitlocker",
        "antivirus",
        "patch",
        "dlp",
        "proxy",
    }

    actual_controls = set(
        controls["control_name"].str.lower().unique()
    )

    print("Controls found:", sorted(actual_controls))
    print(
        "All expected controls present:",
        expected_controls.issubset(actual_controls)
    )

    print("\nDevice count by control:")

    count_check = controls.groupby(
        controls["control_name"].str.lower()
    )["total_devices"].sum()

    for control, total in count_check.items():
        result = "PASS" if total == EXPECTED_DEVICES else "FAIL"

        print(
            f"{control}: {total} / {EXPECTED_DEVICES} - {result}"
        )

    print("\nPercentage total by control:")

    percentage_check = controls.groupby(
        controls["control_name"].str.lower()
    )["percentage"].sum()

    for control, total in percentage_check.items():
        result = (
            "PASS"
            if 99.99 <= total <= 100.01
            else "CHECK"
        )

        print(f"{control}: {total:.2f}% - {result}")

    print("\n3. Duplicate control-status combinations")

    duplicates = controls.duplicated(
        subset=["control_name", "control_status"]
    )

    print("Duplicate combinations:", duplicates.sum())

    if duplicates.any():
        print(
            controls.loc[
                duplicates,
                ["control_name", "control_status", "total_devices"],
            ].to_string(index=False)
        )

    print("\n4. Missing values")

    print(
        controls[
            ["control_name", "control_status",
             "total_devices", "percentage"]
        ].isna().sum().to_string()
    )

    print("\n5. Final control summary")
    print(
        controls.sort_values(
            ["control_name", "control_status"]
        ).to_string(index=False)
    )

    print("\nGOLD VALIDATION FINISHED")


gold_validate()
