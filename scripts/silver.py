import io
import uuid
from datetime import datetime, timezone
import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

# Configuration
BUCKET="amandataeng-compliance-de-2026"

REGION='us-east-1'

DT='2026-10-06'

STALE_DAYS=30

s3=boto3.client('s3',region_name=REGION)

PASS={
    "bitlocker_status":"Enabled",
    "antivirus_status":"Up-To-Date",
    "patch_status":"Compliant",
    'dlp_status':"Compliant",
    'proxy_status':"Installed"
}

ALLOWED={
    "bitlocker_status": [
        "Enabled", "Disabled", "Unknown"
    ],
    "antivirus_status": [
        "Up-to-Date", "Outdated", "Missing", "Unknown"
    ],
    "patch_status": [
        "Compliant", "Pending", "Outdated", "Unknown"
    ],
    "dlp_status": [
        "Compliant", "Non-Compliant", "Unknown"
    ],
    "proxy_status": [
        "Installed", "Missing", "Unknown"
    ],
    "device_health": [
        "Healthy", "Warning", "Critical", "Unknown"
    ],
    "overall_compliance": [
        "Compliant", "Non-Compliant", "Unknown"
    ],
    "action_required": ["Yes", "No", "Unknown"]
}

INACTIVE_STATUSES={
    'detagged','returned','decommissioned'
}

UNKNOWN_VALUES={"",'unknown','not availaible','na','n/a'}

# Extract bronze and ready for silver
# Get latest bronze key
def find_latest_bronze_file(table_folder,filename):
    prefix=f'bronze/{table_folder}/dt={DT}/'
    
    paginator=s3.get_paginator('list_object_v2')
    matches=[]
    
    for page in paginator.paginate(Bucket=BUCKET,Prefix=prefix):
        for obj in page.get("Contents",[]):
            if obj['Key'].endswith('/'+filename) or (obj['Key']==prefix+filename):
                matches.append(obj)
    
    if not matches:
        raise FileNotFoundError(
            f"No Bronze Parquet file found under s3://{BUCKET}/{prefix}\n"
            f"Expected filename: {filename}\n"
            "Check your Bronze output path and DT."
        )
    
    matches.sort(key=lambda obj:obj['Lastmodified'], reverse=True)
    
    return matches[0]['Key']

# Get latest data in df
def read_bronze(table_folder,filename):
    key=find_latest_bronze_file(table_folder,filename)
    
    response=s3.get_object(Bucket=BUCKET, Key=key)
    df=pd.read_parquet(io.BytesIO(response['Body'].read()))
    
    batch_id=None
    for part in key.split('/'):
        if part.startswith('batch_id='):
            batch_id=part.split('=',1)[1]
    
    df['bronze_key']=key
    df['batch_id']=batch_id or 'not_recorded'
    
    print(f"Read: s3://{BUCKET}/{key}")
    print(f"Rows: {len(df):,}")

    return df

# Write data for silver
def write_silver(df,key):
    buffer=io.BytesIO()
    df.to_parquet(buffer,index=False,engine='pyarrow')
    buffer.seek(0)
    
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    print(f"Written: s3://{BUCKET}/{key} | rows={len(df):,}")
    
# Common data cleaning
def normalise_text(series):
    result=series.astype('string').str.strip()
    result=result.mask(result.eq(""))
    return result

def normalise_ids(df,columns):
    for col in columns:
        df[col]=normalise_text(df[col]).str.upper()
        
def standarise_status(series,allowed):
    result=normalise_text(series)
    lower=result.str.lower()
    
    aliases = {
        "up to date": "Up-to-Date",
        "up-to-date": "Up-to-Date",
        "uptodate": "Up-to-Date",
        "non compliant": "Non-Compliant",
        "non-compliant": "Non-Compliant",
        "not compliant": "Non-Compliant",
        "not installed": "Missing",
        "not enabled": "Disabled",
    }

    result = lower.map(aliases).fillna(result)

    lookup = {value.lower(): value for value in allowed}
    result = result.astype("string")
    lower = result.str.lower()

    mapped = lower.map(lookup)
    return mapped.fillna(result)

def require_column(df,required,source_name):
    missing=[col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{source_name} is missing required columns: {missing}")

def add_reason(df,mask,reason):
    if mask.any():
        existing=df.loc[mask,'quarantine_reason'].fillna('')
        df.loc[mask,'quarantine_reason']=np.where(
            existing.eq(''),
            reason,
            existing+';'+reason
        )

# Asset details cleaning
