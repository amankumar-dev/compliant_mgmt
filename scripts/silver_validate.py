
import io
import os
import boto3
import pandas as pd

BUCKET = "amandataeng-compliance-de-2026"
REGION = "us-east-1"
DT = "2026-10-06"

S3_KEY = (
    f"silver/compliance_enriched/dt={DT}/"
    "compliance_enriched.parquet"
)

FILE = "data/compliance_enriched.parquet"

os.makedirs("data", exist_ok=True)

s3 = boto3.client("s3", region_name=REGION)

response = s3.get_object(
    Bucket=BUCKET,
    Key=S3_KEY
)

df = pd.read_parquet(
    io.BytesIO(response["Body"].read())
)

df.to_parquet(FILE, index=False)

print(f"Loaded current Silver data: s3://{BUCKET}/{S3_KEY}")
print(f"Rows loaded: {len(df):,}")

print("=" * 55)
print("SILVER DATA INTEGRITY VALIDATION")
print("=" * 55)

print("\n1. Total records:", len(df))

print("\n2. Duplicate asset IDs:")
print("Duplicate rows:", df["asset_id"].duplicated().sum())

print("\n3. Missing asset IDs:")
print(df["asset_id"].isna().sum())

print("\nActual columns in enriched Parquet:")
print(df.columns.tolist())

print("\n5. Missing values in important columns:")
columns = [
    "asset_id",
    "employee_id",
    "recomputed_compliance",
    "failed_checks",
    "unknown_checks",
    "outcome",
    "kpi_status",
]

for col in columns:
    if col in df.columns:
        print(f"{col}: {df[col].isna().sum()}")

print("\n6. Negative check counts:")
for col in ["failed_checks", "unknown_checks"]:
    if col in df.columns:
        print(f"{col}: {(df[col] < 0).sum()}")

print("\n7. Compliance classification:")
print(df["recomputed_compliance"].value_counts(dropna=False))

print("\n8. Outcome distribution:")
print(df["outcome"].value_counts(dropna=False))

print("\n9. KPI distribution:")
print(df["kpi_status"].value_counts(dropna=False))

print("\n10. Duplicate rows by asset ID (sample):")
duplicates = df[df["asset_id"].duplicated(keep=False)]

if duplicates.empty:
    print("No duplicate asset IDs found.")
else:
    cols = [
        c for c in [
            "asset_id",
            "employee_id",
            "outcome",
            "kpi_status",
        ] if c in df.columns
    ]
    print(duplicates[cols].head(20).to_string(index=False))

print("\nVALIDATION CHECK FINISHED")


print("\n11. Local KPI vs outcome check:")

print(
    pd.crosstab(
        df["outcome"],
        df["kpi_status"],
        dropna=False,
    )
)

print("\n12. Employee identifier null counts:")

for col in ["snow_employee_id", "intune_employee_id"]:
    print(f"{col}: {df[col].isna().sum()}")

print("\n13. Local file metadata:")

print("File:", FILE)
print("Rows:", len(df))
print("Columns:", len(df.columns))


print("\n14. Records with missing ServiceNow employee ID:")

missing_employee = df[df["snow_employee_id"].isna()]

cols = [
    "asset_id",
    "snow_employee_id",
    "intune_employee_id",
    "employee_name",
    "employee_status",
    "asset_status",
    "outcome",
    "kpi_status",
]

cols = [col for col in cols if col in df.columns]

if missing_employee.empty:
    print("No missing ServiceNow employee IDs.")
else:
    print(missing_employee[cols].to_string(index=False))


print("\n15. Inactive asset rule validation:")

inactive_statuses = {
    "detagged",
    "returned",
    "decommissioned",
}

inactive = df[
    df["asset_status"]
    .astype("string")
    .str.strip()
    .str.lower()
    .isin(inactive_statuses)
]

print("Inactive asset records:", len(inactive))

print("\nInactive asset status distribution:")
print(inactive["asset_status"].value_counts(dropna=False))

print("\nInactive asset outcomes:")
print(
    pd.crosstab(
        inactive["asset_status"],
        inactive["outcome"],
        dropna=False,
    )
)

print("\nInactive assets incorrectly marked for remediation:")
bad_inactive = inactive[inactive["outcome"].eq("REMEDIATE")]

print("Count:", len(bad_inactive))

if not bad_inactive.empty:
    print(
        bad_inactive[
            ["asset_id", "asset_status", "outcome", "kpi_status"]
        ].head(20).to_string(index=False)
    )


print("\n16. Asset ID mismatch validation:")

mismatch = df[df["outcome"].eq("ASSET_ID_MISMATCH")]

print("Mismatch records:", len(mismatch))

cols = [
    "asset_id",
    "snow_employee_id",
    "intune_employee_id",
    "employee_status",
    "asset_status",
    "outcome",
    "kpi_status",
]

cols = [col for col in cols if col in df.columns]

if mismatch.empty:
    print("No asset mismatches found.")
else:
    print(mismatch[cols].to_string(index=False))

print("\nMismatch KPI distribution:")
print(mismatch["kpi_status"].value_counts(dropna=False))

print("\n17. Check whether mismatches are excluded from KPI:")

incorrect = mismatch[mismatch["kpi_status"].ne("Excluded")]

print("Mismatches not excluded:", len(incorrect))


print("\n18. Exited employee recovery validation:")

exited = df[
    df["outcome"].eq("EXITED_LAPTOP_RECOVERY")
]

print("Recovery records:", len(exited))

cols = [
    "asset_id",
    "snow_employee_id",
    "employee_status",
    "asset_status",
    "return_date",
    "exit_date",
    "outcome",
    "kpi_status",
]

cols = [col for col in cols if col in df.columns]

if exited.empty:
    print("No exited employee recovery records found.")
else:
    print(exited[cols].to_string(index=False))

print("\nRecovery KPI distribution:")
print(exited["kpi_status"].value_counts(dropna=False))

print("\nRecovery records not excluded:")
print((exited["kpi_status"] != "Excluded").sum())


print("\n19. Stale device validation:")

stale = df[df["outcome"].eq("STALE_REVALIDATE")]

print("Stale records:", len(stale))

if "days_since_check" in df.columns:
    print(
        "Records with age greater than 30 days:",
        (stale["days_since_check"] > 30).sum()
    )

print("\nStale KPI distribution:")
print(stale["kpi_status"].value_counts(dropna=False))

print("\nStale compliance distribution:")
print(stale["recomputed_compliance"].value_counts(dropna=False))

print("\nStale records not marked Compliant in KPI:")
print((stale["kpi_status"] != "Compliant").sum())


# =======================================================
# STRICT VALIDATION GATE
# =======================================================

print("\n" + "=" * 55)
print("STRICT SILVER VALIDATION GATE")
print("=" * 55)

errors = []

# 1. Dataset must not be empty
if df.empty:
    errors.append("Silver dataset is empty.")

# 2. Asset IDs must exist and be unique
if df["asset_id"].isna().any():
    errors.append("Missing asset_id values found.")

if df["asset_id"].duplicated().any():
    errors.append("Duplicate asset_id values found.")

# 3. Critical columns must not contain null values
required_columns = [
    "recomputed_compliance",
    "failed_checks",
    "unknown_checks",
    "outcome",
    "kpi_status",
]

for column in required_columns:
    if column not in df.columns:
        errors.append(f"Required column missing: {column}")
    elif df[column].isna().any():
        errors.append(f"Null values found in {column}.")

# 4. Check counts cannot be negative
for column in ["failed_checks", "unknown_checks"]:
    if column in df.columns:
        if (df[column] < 0).any():
            errors.append(f"Negative values found in {column}.")

# 5. KPI statuses must be valid
allowed_kpi_statuses = {
    "Compliant",
    "Non-Compliant",
    "Excluded",
}

if "kpi_status" in df.columns:
    invalid = set(df["kpi_status"].dropna().unique()) - allowed_kpi_statuses

    if invalid:
        errors.append(f"Invalid KPI statuses: {sorted(invalid)}")

# 6. Every asset must have a valid outcome
allowed_outcomes = {
    "REMEDIATE",
    "STALE_REVALIDATE",
    "REVIEW_UNKNOWN_CONTROLS",
    "INACTIVE_DEVICE",
    "ASSET_ID_MISMATCH",
    "EXITED_LAPTOP_RECOVERY",
    "REVIEW_NO_OWNER",
    "UNMANAGED_REVIEW",
}

if "outcome" in df.columns:
    invalid = set(df["outcome"].dropna().unique()) - allowed_outcomes

    if invalid:
        errors.append(f"Invalid outcomes: {sorted(invalid)}")

# 7. Excluded business outcomes must not enter compliance KPIs
excluded_outcomes = {
    "INACTIVE_DEVICE",
    "ASSET_ID_MISMATCH",
    "EXITED_LAPTOP_RECOVERY",
    "REVIEW_UNKNOWN_CONTROLS",
    "REVIEW_NO_OWNER",
    "UNMANAGED_REVIEW",
}

if {"outcome", "kpi_status"}.issubset(df.columns):
    bad_exclusions = df[
        df["outcome"].isin(excluded_outcomes)
        & (df["kpi_status"] != "Excluded")
    ]

    if not bad_exclusions.empty:
        errors.append(
            f"{len(bad_exclusions)} excluded-outcome records "
            "have incorrect KPI status."
        )

# Final result
if errors:
    print("\nVALIDATION FAILED")

    for number, error in enumerate(errors, start=1):
        print(f"{number}. {error}")

    raise SystemExit(1)

print("PASS: All strict validation checks passed.")
print(f"Validated records: {len(df):,}")
print("VALIDATION SUCCESSFUL")
