from pathlib import Path
import boto3
from botocore.exceptions import ClientError

REGION='us-east-1'

BUCKET='amandataeng-compliance-de-2026'

FILES={
    Path('data/raw/asset_details.csv'):"raw/asset_details/asset_details.csv",
    Path('data/raw/compliance_status.csv'):"raw/compliance_details/compliance_status.csv"
}

def upload_bucket():
    s3=boto3.client('s3',region_name=REGION)
    
    # Create Bucket
    try:
        s3.create_bucket(Bucket=BUCKET)
        print("Bucket created:", BUCKET)
        
    except ClientError as error:
        code=error.response['Error']['Code']
        
        if code=="BucketAlreadyOwnedByYou":
            print("Using your existing bucket:", BUCKET)
        
        elif code=="BucketAlreadyExists":
            print("Bucket name is taken globally.")
            print("Change BUCKET in this script and run again.")
            return
        
        else:
            raise
        
    # Block Public Access
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":True,
            "IgnorePublicAcls":True,
            "BlockPublicPolicy":True,
            "RestrictPublicBuckets":True
        }
    )
    
    # Enabling Versioning
    s3.put_bucket_versioning(
        Bucket=BUCKET,
        VersioningConfiguration={'Status':'Enabled'}
    )
    
    # Configure default encryption
    s3.put_bucket_encryption(
        Bucket=BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules":[
                {
                    "ApplyServerSideEncryptionByDefault":{
                        "SSEAlgorithm":"AES256"
                    }
                }
            ]
        }
    )
    
    # Upload both csv in s3
    for local_path,s3_key in FILES.items():
        if not local_path.is_file():
            raise FileNotFoundError(f"file not found: {local_path.resolve()}")
        
        s3.upload_file(
            str(local_path),
            Bucket=BUCKET,
            Key=s3_key,
            ExtraArgs={"ServerSideEncryption":"AES256"}
        )
        print(f"Uploaded: {local_path.name} -> s3://{BUCKET}/{s3_key}")
        
    # Verify objects in s3
    response=s3.list_objects_v2(Bucket=BUCKET, Prefix='raw/')
    
    print("\nObjects found in S3:")
    for item in response.get("Contents", []):
        print(item["Key"], "-", item["Size"], "bytes")


upload_bucket()