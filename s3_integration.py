import zipfile
import io
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
    # Create a BytesIO object to store compressed data
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr(file_name, data)
    zip_buffer.seek(0)

    # Upload the zip file to S3
    s3 = boto3.client('s3', aws_access_key_id=AWS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Body=zip_buffer.getvalue(),
                  Bucket=bucket_name, Key=file_name)

    # Generate a presigned URL with public-read access for the zip file
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': file_name},
        ExpiresIn=3600,  # URL expiration time (1 hour)
        HttpMethod='GET',  # Allow GET requests
    )

    return url
