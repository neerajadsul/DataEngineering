# coding: utf-8
import boto3
from botocore.exceptions import NoCredentialsError, ParamValidationError
from botocore.errorfactory import ClientError


def create_new_bucket():
    s3 = boto3.client('s3')
    s3.create_bucket(
        Bucket='mooc-courses-certificates',
        CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'}
    )


def upload_file_to_bucket():
    s3 = boto3.client('s3')
    file_to_upload = '/Users/neeraj/Desktop/Codeacademy_HTML_Certificate.pdf'
    bucket_name = 'mooc-courses-certificates'
    object_key = 'certificates/Codeacademy_HTML_Certificate1.pdf'
    try:
        s3.upload_file(
            file_to_upload,
            bucket_name,
            object_key
        )
    except NoCredentialsError as e:
        print('AWS Credentials not found, please configure S3 credentials.', e)
    except ParamValidationError as e:
        print('Input parameters invalid', e)
    except ClientError as e:
        if 'NoSuchBucket' in e.response:
            print('Bucket does not exists.')

def copy_object():
    s3 = boto3.client('s3')
    src_bucket ='mooc-courses-certificates'
    object_to_copy = f'{src_bucket}/certificates/Codeacademy_HTML_Certificate.pdf'
    object_key = 'courses/Codeacademy-HTML.pdf'
    dest_bucket = 'www.125systems.com'

    s3.copy_object(
        Bucket=dest_bucket,
        Key=object_key,
        CopySource=object_to_copy
    )

def move_object():
    """There is no move operation on boto3 client.
    Copy the object and then delete the source."""

def delete_object():
    bucket_name = 'mooc-courses-certificates'
    object_key = 'certificates/Codeacademy_HTML_Certificate1.pdf'

    s3 = boto3.client('s3')
    s3.delete_object(
        Bucket=bucket_name,
        Key=object_key
    )


if __name__ == "__main__":
    # copy_object()
    # upload_file_to_bucket()
    # move_object()
    delete_object()