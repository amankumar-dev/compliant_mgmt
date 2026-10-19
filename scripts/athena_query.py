import argparse
import time
from pathlib import Path
import boto3

DATABASE='compliance_analytics'
REGION='us-east-1'
WORKGROUP='primary'
CATALOG='AwsDataCatalog'

athena=boto3.client('athena',region_name=REGION)

def run_query(sql_file):
    # Check path of sql file
    file_path=Path(sql_file)
    
    if not file_path.is_file():
        raise FileNotFoundError(f'SQL file is not found: {sql_file}')
    
    # Read query
    query=file_path.read_text(encoding='utf-8').strip()
    
    if not query:
        raise ValueError('SQL file is empty')
    
    # Submit Query in athena
    response=athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database":DATABASE,'Catalog':CATALOG},
        Workgroup=WORKGROUP
    )
    
    query_id=response['QueryExecutionId']
    print(f"Query submitted: {query_id}")
    
    # Query complete honey ka wait
    while True:
        execution=athena.get_query_execution(
            QueryExecutionId=query_id
        )['QueryExecution']
        
        status=execution['Status']['State']
        print("Query status:", status)
        
        if status in ['SUCCEEDED','FAILED','CANCELLED']:
            break
        
        time.sleep(2)
        
    if status!='SUCCEEDED':
        reason=execution['Status'].get('StateChangeReason','Unknown error')
        raise RuntimeError(f"Query {status}: {reason}")

    # Fetch Query result
    paginator=athena.get_paginator('get_query_result')
    
    print("\nQuery Results:")
    
    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page['ResultSet']['Rows']:
            values=[cell.get('VarCharValue','') for cell in row['Data']]
            print(" | ".join(values))
            
parser=argparse.ArgumentParser(description='Run any SQL file using Amazon Athena')
parser.add_argument('sql_file',help='Path of the sql file to execute')
args=parser.parse_args()
run_query(args.sql_file)