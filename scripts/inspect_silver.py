
import io
import boto3
import pandas as pd

BUCKET = "amandataeng-compliance-de-2026"
DT = "2026-10-06"

s3 = boto3.client("s3", region_name="us-east-1")


def read_parquet(key):
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(response["Body"].read()))


# 1. Inspect enriched compliance data
key = f"silver/compliance_enriched/dt={DT}/compliance_enriched.parquet"
df = read_parquet(key)

print("\n=== COMPLIANCE ENRICHED ===")
print("Rows:", len(df))
print("Columns:", df.columns.tolist())

# 2. Inspect available business outcomes
for column in ["outcome", "kpi_status", "priority"]:
    if column in df.columns:
        print(f"\n=== {column.upper()} COUNTS ===")
        print(df[column].value_counts(dropna=False).to_string())

# 3. Validate our known test assets
if "asset_id" in df.columns:
    print("\n=== TEST ASSET RECORDS ===")
    test_assets = df[df["asset_id"].isin(
        ["AST-1003", "AST-1004", "AST-1005"]
    )]
    print(test_assets.to_string(index=False))

# 4. Check the exceptions dataset
key = f"silver/exceptions/dt={DT}/exceptions.parquet"
exceptions = read_parquet(key)

print("\n=== EXCEPTIONS DATASET ===")
print("Rows:", len(exceptions))
print("Columns:", exceptions.columns.tolist())

print("\nSilver inspection completed.")