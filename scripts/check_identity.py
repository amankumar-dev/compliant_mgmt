import boto3
from botocore.exceptions import NoCredentialsError,ClientError

def main():
    try:
        sts=boto3.client('sts',region_name="us-east-1")
        identity=sts.get_caller_identity()
    
        print('AWS connection successfully !!')
        print('Account ID: ', identity['Account'])
        print('User/Role ARN: ', identity['Arn'])
        print('Identity Verified')
        
    except NoCredentialsError:
        print("AWS credentials not found.")
        print("Check your AWS CLI configuration.")
        
    except ClientError as error:
        print("AWS request failed:")
        print(error.response["Error"]["Code"])
        print(error.response["Error"]["Message"])
    
    
main()
    