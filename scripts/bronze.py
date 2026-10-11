import io
from datetime import datetime, timezone
import boto3
import pandas as pd

BUCKET='amandataeng-compliance-de-2026'

REGION='us-east-1'

DT='2026-10-06'

SOURCES={
    "asset_details":'raw/asset_details/asset_details.csv',
    "compliance_details":"raw/compliance_details/compliance_status.csv"
}

s3=boto3.client('s3',region_name=REGION)
now=datetime.now(timezone.utc)
batch_id=now.strftime("%Y%m%d%H%M%S")

for table,raw_key in SOURCES.items():
    body=s3.get_object(Bucket=BUCKET,Key=raw_key)['Body'].read()
    df=pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False)
    
    df['Batch_id']=batch_id
    df['Ingestion_ts']=now.isoformat()
    df['Source_file']=raw_key
    
    buffer=io.BytesIO()
    df.to_parquet(buffer,index=False)
    bronze_key=f"bronze/{table}/dt={DT}/{table}.parquet"
    s3.put_object(Bucket=BUCKET, Key=bronze_key, Body=buffer.getvalue())
    print(table, len(df), "rows ->", bronze_key)
    
