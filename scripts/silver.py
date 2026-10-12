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
def clean_assets(raw):
    df=raw.copy()
    
    required = [
        "asset_id", "employee_id", "employee_email",
        "employee_name", "department", "location",
        "device_type", "device_model", "os_version",
        "employee_status", "asset_status",
        "assignment_date", "return_date", "exit_date",
        "asset_notes",
    ]
    
    require_column(df,required,'asset_details')
    
    df['quarantine_reason']=pd.NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    
    normalise_ids(df,['asset_id','employee_id'])
    
    for col in ["assignment_date", "return_date", "exit_date"]:
        df[col]=pd.to_datetime(df[col],errors='coerce',format='%Y-%m-%d')
        
    missing_id=df['asset_id'].isna()
    add_reason(df,missing_id,'MISSING_ASSET_ID')
    
    quarantine=df[missing_id].copy()
    valid=df[~missing_id].copy()
    
    duplicate_mask=valid.duplicated('asset_id',keep=False)
    duplicate_rows=valid[duplicate_mask].copy()
    
    if not duplicate_rows.empty:
        duplicate_rows['quarantine_reason']=("DUPLICATE_ASSET_ID_REQUIRES_REVIEW")
        quarantine=pd.concat(quarantine,duplicate_rows,ignore_index=True)
        
    valid=valid.drop_duplicates('asset_id',keep='first')
    return valid,quarantine

# Compliance details cleaning
def clean_compliance(raw):
    df=raw.copy()
    
    required = [
        "asset_id", "employee_id", "source_system",
        "last_check_date", "bitlocker_status",
        "antivirus_status", "patch_status", "dlp_status",
        "proxy_status", "device_health", "overall_compliance",
        "primary_issue", "action_required", "compliance_notes",
    ]
    require_column(df,required,'compliance_status')
    
    df['quarantine_reason']=pd.NA
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    
    normalise_ids(df,['asset_id','employee_id'])
    
    df['last_check_date']=pd.to_datetime(df['last_check_date'],errors='coerce',format="%Y-%m-%d")
    
    for col,allow in ALLOWED.items():
        df[col]=standarise_status(df[col],allow)
    
    missing_id=df['asset_id'].isna()
    invalid_date=df['last_check_date'].isna()
    
    add_reason(df,missing_id,'MISSING_ASSET_ID')
    add_reason(df,invalid_date,'INVALID_OR_MISSING_CHECK_DATE')
    
    bad=missing_id | invalid_date
    quarantine=df[bad].copy()
    valid=df[~bad].copy()
    
    duplicate_mask=valid.duplicated('asset_id',keep=False)
    duplicate_rows=valid[duplicate_mask].copy()
    
    if not duplicate_rows.empty:
        duplicate_rows['quarantine_reason']=('"DUPLICATE_COMPLIANCE_ASSET_REQUIRES_REVIEW"')
        quarantine=pd.concat(quarantine,duplicate_rows,ignore_index=True)
        
    valid=valid.sort_values('last_check_date',keep='stable').drop_duplicates('asset_id',keep='last')
    
    failed=pd.DataFrame(index=valid.index)
    unknown=pd.DataFrame(index=valid.index)
    
    for col,expected in PASS.items():
        value=valid[col].astype('string').str.lower()
        
        unknown[col]=(value.isna() | value.isin(UNKNOWN_VALUES))
        
        failed[col]=(value.notna() & ~value.isin(UNKNOWN_VALUES) & value.ne(expected.lower()))
        
        valid['failed_checks']=failed.sum(axis=1).astype(int)
        valid['unknown_checks']=failed.sum(axis=1).astype(int)
        
        critical=(valid['device_health'].astype('string').str.lower().eq('critical').fillna(False))
        
        valid['recomputed_compliance']=np.select(
            [valid['failed_checks'].gt(0),valid['unknown_checks'].gt(0)],
            ['Non-Compliant','Needs Review'],
            default='Compliant'
        )
        
        valid['flag_mismatch']=(
            valid['recomputed_compliance'].isin(['Compliant','Non-Compliant'])
            & valid['overall_compliance'].isin(['Compliant','Non-Compliant'])
            & valid['recomputed_compliance'].ne(valid['overall_compliance'])
        )
        
        valid['priority']=np.select([
            critical | valid['failed_checks'].ge(3),
            valid['failed_checks'].eq(2),
            valid['failed_checks'].eq(1),
            valid['unknown_checks'].gt(0)
        ],['P1','P2','P3','P4'],default=None)
        
        return valid,quarantine
    
    
    
    
    