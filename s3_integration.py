"""
Module for integrating with AWS S3.
"""

from io import StringIO
import boto3


AWS_KEY_ID = 'AKIAZQ3DQVW5HEBI465G'
AWS_SECRET_ACCESS_KEY = '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'


def upload_to_s3_and_grant_permissions(data, bucket_name, file_name):
    """
    Uploads data to S3 and generates a presigned URL with public-read access.

    Args:
        data: Data to be uploaded.
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Name of the file in the S3 bucket.

    Returns:
        str: Presigned URL with public-read access.
    """
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)

    # Generate a presigned URL with public-read access
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': file_name},
        ExpiresIn=3600,  # URL expiration time (1 hour)
        HttpMethod='GET',  # Allow GET requests
    )

    return url


# import boto3
# from io import StringIO

# AWS_KEY_ID = 'AKIAZQ3DQVW5HEBI465G'
# AWS_SECRET_ACCESS_KEY = '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'


# def upload_to_s3_and_grant_permissions(data, bucket_name, file_name):
#     s3 = boto3.client('s3', aws_access_key_id=AWS_KEY_ID,
#                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     s3.create_bucket(Bucket=bucket_name)
#     s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)

#     # Generate a presigned URL with public-read access
#     url = s3.generate_presigned_url(
#         'get_object',
#         Params={'Bucket': bucket_name, 'Key': file_name},
#         ExpiresIn=3600,  # URL expiration time (1 hour)
#         HttpMethod='GET',  # Allow GET requests
#     )

#     return url
