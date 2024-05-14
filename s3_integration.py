# s3_integration.py
import boto3
from io import StringIO

AWS_KEY_ID = 'AKIAZQ3DQVW5HEBI465G'
AWS_SECRET_ACCESS_KEY = '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'


def upload_to_s3_and_grant_permissions(data, bucket_name, file_name):
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Body=data, Bucket=bucket_name,
                  Key=file_name, ACL='public-read')
    # Grant public access to the bucket
    s3.put_bucket_acl(ACL='public-read', Bucket=bucket_name)
    url = s3.generate_presigned_url(
        'get_object', Params={'Bucket': bucket_name, 'Key': file_name}, ExpiresIn=3600)
    return url
