import sys
import time

import boto3
import pandas as pd

DATABASE = "compliance_analytics"
REGION = "us-east-1"
WORKGROUP = "primary"

KPI_TABLE = "gold_kpi_summary_live"
CONTROL_TABLE = "gold_summary_live"

EXPECTED_DEVICES = 1200
TOLERANCE = 0.02

athena = boto3.client("athena", region_name=REGION)


def run_query(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
        ResultConfiguration={
            "OutputLocation": (
                "s3://amandataeng-compliance-de-2026/athena-results/"
            )
        },
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

    headers = [
        cell.get("VarCharValue", "")
        for cell in rows[0]["Data"]
    ]

    data = [
        [cell.get("VarCharValue") for cell in row["Data"]]
        for row in rows[1:]
    ]

    return pd.DataFrame(data, columns=headers)


def gold_validate():
    failures = []

    print("=" * 55)
    print("GOLD DATA INTEGRITY VALIDATION")
    print("=" * 55)

    # 1. KPI summary
    print("\n1. KPI summary validation")

    kpi = run_query(f"SELECT * FROM {KPI_TABLE}")

    if kpi.empty:
        failures.append("KPI table returned no rows.")
        print("FAIL: KPI table returned no rows.")
    else:
        required_kpi_columns = {
            "kpi_status", "total_device", "percentage"
        }

        if not required_kpi_columns.issubset(kpi.columns):
            failures.append("KPI table is missing required columns.")
            print("FAIL: Missing KPI columns.")
        else:
            kpi["total_device"] = pd.to_numeric(
                kpi["total_device"], errors="coerce"
            )
            kpi["percentage"] = pd.to_numeric(
                kpi["percentage"], errors="coerce"
            )

            if kpi["total_device"].isna().any():
                failures.append("KPI device counts contain invalid values.")

            if (kpi["total_device"] < 0).any():
                failures.append("KPI device counts contain negative values.")

            total = kpi["total_device"].sum()

            print("KPI records:", len(kpi))
            print("Total devices:", total)
            print("Expected devices:", EXPECTED_DEVICES)
            print(kpi.to_string(index=False))

            if total != EXPECTED_DEVICES:
                failures.append(
                    f"KPI total is {total}, expected {EXPECTED_DEVICES}."
                )

            if kpi["kpi_status"].isna().any():
                failures.append("KPI status contains missing values.")

            if kpi["kpi_status"].duplicated().any():
                failures.append("KPI table has duplicate status rows.")

            if kpi["percentage"].isna().any():
                failures.append("KPI percentages contain invalid values.")
            elif not (kpi["percentage"].between(0, 100).all()):
                failures.append("KPI percentages fall outside 0-100.")

            if not kpi["percentage"].isna().any():
                pct_total = kpi["percentage"].sum()
                print(f"KPI percentage total: {pct_total:.2f}%")

                if abs(pct_total - 100.0) > TOLERANCE:
                    failures.append(
                        f"KPI percentages sum to {pct_total:.2f}%, not 100%."
                    )

    # 2. Control-wise summary
    print("\n2. Control-wise summary validation")

    controls = run_query(f"SELECT * FROM {CONTROL_TABLE}")

    if controls.empty:
        failures.append("Control summary table returned no rows.")
        print("FAIL: Control summary table returned no rows.")
    else:
        required_control_columns = {
            "control_name",
            "control_status",
            "total_devices",
            "percentage",
        }

        if not required_control_columns.issubset(controls.columns):
            failures.append(
                "Control summary table is missing required columns."
            )
            print("FAIL: Missing control summary columns.")
        else:
            controls["total_devices"] = pd.to_numeric(
                controls["total_devices"], errors="coerce"
            )
            controls["percentage"] = pd.to_numeric(
                controls["percentage"], errors="coerce"
            )

            if controls[
                ["control_name", "control_status",
                 "total_devices", "percentage"]
            ].isna().any().any():
                failures.append(
                    "Control summary contains missing or invalid values."
                )

            if (controls["total_devices"] < 0).any():
                failures.append(
                    "Control summary contains negative device counts."
                )

            expected_controls = {
                "bitlocker", "antivirus", "patch", "dlp", "proxy"
            }

            actual_controls = set(
                controls["control_name"].dropna().str.lower().unique()
            )

            print("Controls found:", sorted(actual_controls))

            missing_controls = expected_controls - actual_controls

            if missing_controls:
                failures.append(
                    f"Missing controls: {sorted(missing_controls)}"
                )

            unexpected_controls = actual_controls - expected_controls

            if unexpected_controls:
                failures.append(
                    f"Unexpected controls: {sorted(unexpected_controls)}"
                )

            duplicates = controls.duplicated(
                subset=["control_name", "control_status"]
            )

            if duplicates.any():
                failures.append(
                    "Duplicate control-name/status combinations found."
                )
                print(
                    controls.loc[
                        duplicates,
                        ["control_name", "control_status", "total_devices"],
                    ].to_string(index=False)
                )

            print("\nDevice count and percentage by control:")

            for control, group in controls.groupby(
                controls["control_name"].str.lower()
            ):
                total = group["total_devices"].sum()
                pct_total = group["percentage"].sum()

                count_ok = total == EXPECTED_DEVICES
                pct_ok = (
                    pd.notna(pct_total)
                    and abs(pct_total - 100.0) <= TOLERANCE
                )

                print(
                    f"{control}: devices={total}/{EXPECTED_DEVICES}, "
                    f"percentage={pct_total:.2f}%"
                )

                if not count_ok:
                    failures.append(
                        f"{control}: device count is {total}, "
                        f"expected {EXPECTED_DEVICES}."
                    )

                if not pct_ok:
                    failures.append(
                        f"{control}: percentages sum to "
                        f"{pct_total:.2f}%, expected 100%."
                    )

            print("\nFinal control summary:")
            print(
                controls.sort_values(
                    ["control_name", "control_status"]
                ).to_string(index=False)
            )

    print("\n" + "=" * 55)

    if failures:
        print("GOLD VALIDATION FAILED")
        for number, failure in enumerate(failures, start=1):
            print(f"{number}. {failure}")

        return False

    print("GOLD VALIDATION SUCCESSFUL")
    print("All Gold integrity checks passed.")
    return True


if __name__ == "__main__":
    try:
        success = gold_validate()
        sys.exit(0 if success else 1)
    except Exception as exc:
        print(f"GOLD VALIDATION ERROR: {exc}")
        sys.exit(1)
