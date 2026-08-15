import json

import boto3
from botocore.exceptions import ClientError


s3_client = boto3.client("s3", region_name="us-east-1")
s3_resource = boto3.resource("s3", region_name="us-east-1")

BUCKET = "my-demo-bucket"
REGION = "us-east-1"


# 1) Bucket operations

def create_bucket(bucket_name: str, region: str = "us-east-1") -> dict:
    """Create a bucket. us-east-1 does not need LocationConstraint."""
    if region == "us-east-1":
        return s3_client.create_bucket(Bucket=bucket_name)
    return s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": region},
    )


def list_buckets() -> list:
    """List all buckets in the account."""
    return s3_client.list_buckets().get("Buckets", [])


def get_bucket_location(bucket_name: str) -> str:
    """Get the region of a bucket."""
    return (s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint") or "us-east-1")


def delete_bucket(bucket_name: str) -> dict:
    """Delete an empty bucket."""
    return s3_client.delete_bucket(Bucket=bucket_name)


# 2) Object operations

def upload_file(local_path: str, bucket_name: str, key: str) -> None:
    """Upload a local file to an S3 key."""
    s3_client.upload_file(local_path, bucket_name, key)


def put_object(bucket_name: str, key: str, data: bytes | str) -> dict:
    """Write bytes or a string to S3."""
    body = data.encode("utf-8") if isinstance(data, str) else data
    return s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)


def download_file(bucket_name: str, key: str, local_path: str) -> None:
    """Download an S3 object to a local file."""
    s3_client.download_file(bucket_name, key, local_path)


def get_object(bucket_name: str, key: str) -> bytes:
    """Read object body into memory."""
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read()


def copy_object(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> dict:
    """Copy an object within or across buckets."""
    return s3_client.copy_object(
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Bucket=dst_bucket,
        Key=dst_key,
    )


def move_object(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
    """Copy then delete original object."""
    copy_object(src_bucket, src_key, dst_bucket, dst_key)
    delete_object(src_bucket, src_key)


def delete_object(bucket_name: str, key: str) -> dict:
    """Delete a single object."""
    return s3_client.delete_object(Bucket=bucket_name, Key=key)


def delete_objects_batch(bucket_name: str, keys: list[str]) -> dict:
    """Delete many objects in one request."""
    objects = [{"Key": key} for key in keys]
    return s3_client.delete_objects(
        Bucket=bucket_name,
        Delete={"Objects": objects, "Quiet": False},
    )


def list_objects(bucket_name: str, prefix: str = "", max_keys: int = 1000) -> list:
    """List objects under a prefix with pagination support."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, PaginationConfig={"MaxItems": max_keys}):
        objects.extend(page.get("Contents", []))
    return objects


def head_object(bucket_name: str, key: str) -> dict:
    """Get object metadata without downloading body."""
    return s3_client.head_object(Bucket=bucket_name, Key=key)


# 3) Metadata and tags

def put_object_tags(bucket_name: str, key: str, tags: dict) -> None:
    """Set object tags."""
    tag_set = [{"Key": key_name, "Value": value} for key_name, value in tags.items()]
    s3_client.put_object_tagging(
        Bucket=bucket_name,
        Key=key,
        Tagging={"TagSet": tag_set},
    )


def get_object_tags(bucket_name: str, key: str) -> list:
    """Get object tags."""
    response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
    return response.get("TagSet", [])


def put_bucket_tags(bucket_name: str, tags: dict) -> None:
    """Set bucket tags."""
    tag_set = [{"Key": key_name, "Value": value} for key_name, value in tags.items()]
    s3_client.put_bucket_tagging(
        Bucket=bucket_name,
        Tagging={"TagSet": tag_set},
    )


def get_bucket_tags(bucket_name: str) -> list:
    """Get bucket tags."""
    try:
        response = s3_client.get_bucket_tagging(Bucket=bucket_name)
        return response.get("TagSet", [])
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchTagSet":
            return []
        raise


# 4) Storage classes

STORAGE_CLASSES = [
    "STANDARD",
    "INTELLIGENT_TIERING",
    "STANDARD_IA",
    "ONEZONE_IA",
    "GLACIER",
    "DEEP_ARCHIVE",
]


def change_storage_class(bucket_name: str, key: str, new_class: str) -> None:
    """Move object to another storage class by copying the object over itself."""
    s3_client.copy_object(
        CopySource={"Bucket": bucket_name, "Key": key},
        Bucket=bucket_name,
        Key=key,
        StorageClass=new_class,
        MetadataDirective="COPY",
    )


# 5) Security basics

def enable_block_public_access(bucket_name: str) -> None:
    """Turn on S3 Block Public Access."""
    s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def put_bucket_policy(bucket_name: str, policy: dict) -> None:
    """Attach a bucket policy."""
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))


HTTPS_ONLY_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DenyHTTP",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::my-demo-bucket",
                "arn:aws:s3:::my-demo-bucket/*",
            ],
            "Condition": {"Bool": {"aws:SecureTransport": "false"}},
        }
    ],
}


# 6) Encryption

def set_default_encryption_sse_s3(bucket_name: str) -> None:
    """Turn on default S3-managed encryption for the bucket."""
    s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    "BucketKeyEnabled": False,
                }
            ]
        },
    )


def set_default_encryption_sse_kms(bucket_name: str, kms_key_id: str) -> None:
    """Turn on default KMS encryption for the bucket."""
    s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "aws:kms",
                        "KMSMasterKeyID": kms_key_id,
                    },
                    "BucketKeyEnabled": True,
                }
            ]
        },
    )


def upload_sse_kms(bucket_name: str, key: str, data: bytes, kms_key_id: str = None) -> dict:
    """Upload a file encrypted with AWS KMS."""
    kwargs = {
        "Bucket": bucket_name,
        "Key": key,
        "Body": data,
        "ServerSideEncryption": "aws:kms",
    }
    if kms_key_id:
        kwargs["SSEKMSKeyId"] = kms_key_id
    return s3_client.put_object(**kwargs)


# 7) Pre-signed URLs

def generate_presigned_get_url(bucket_name: str, key: str, expiry_seconds: int = 3600) -> str:
    """Create a temporary downloadable URL for a private object."""
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=expiry_seconds,
    )


def generate_presigned_put_url(bucket_name: str, key: str, expiry_seconds: int = 3600) -> str:
    """Create a temporary upload URL for a client to PUT data."""
    return s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket_name, "Key": key},
        ExpiresIn=expiry_seconds,
    )


# 8) Versioning

def enable_versioning(bucket_name: str) -> None:
    """Enable object versioning on a bucket."""
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )


def get_versioning_status(bucket_name: str) -> str:
    """Return bucket versioning state."""
    response = s3_client.get_bucket_versioning(Bucket=bucket_name)
    return response.get("Status", "Not enabled")


def list_object_versions(bucket_name: str, key: str) -> dict:
    """List all versions for a key."""
    return s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)


# 9) Lifecycle rules

def put_lifecycle_configuration(bucket_name: str, rules: list) -> None:
    """Apply lifecycle rules to a bucket."""
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={"Rules": rules},
    )


LOG_LIFECYCLE_RULE = {
    "ID": "archive-logs",
    "Status": "Enabled",
    "Filter": {"Prefix": "logs/"},
    "Transitions": [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 90, "StorageClass": "GLACIER"},
    ],
    "Expiration": {"Days": 365},
}

TEMP_FILE_RULE = {
    "ID": "delete-temp-files",
    "Status": "Enabled",
    "Filter": {"Prefix": "tmp/"},
    "Expiration": {"Days": 1},
}


# 10) Replication

def put_bucket_replication(
    source_bucket: str,
    destination_bucket_arn: str,
    replication_role_arn: str,
    prefix: str = "",
    dest_storage_class: str = "STANDARD",
) -> None:
    """Configure replication from source bucket to destination."""
    rule = {
        "ID": "replication-rule-1",
        "Status": "Enabled",
        "Filter": {"Prefix": prefix},
        "Destination": {
            "Bucket": destination_bucket_arn,
            "StorageClass": dest_storage_class,
        },
        "DeleteMarkerReplication": {"Status": "Enabled"},
    }
    s3_client.put_bucket_replication(
        Bucket=source_bucket,
        ReplicationConfiguration={
            "Role": replication_role_arn,
            "Rules": [rule],
        },
    )


# 11) Example usage

if __name__ == "__main__":
    # create_bucket("my-demo-bucket", "us-east-1")
    # upload_file("data.csv", BUCKET, "raw/data.csv")
    # print(get_object(BUCKET, "raw/data.csv")[:100])
    # enable_versioning(BUCKET)
    # set_default_encryption_sse_kms(BUCKET, "arn:aws:kms:us-east-1:123456789012:key/xxxx")
    # put_lifecycle_configuration(BUCKET, [LOG_LIFECYCLE_RULE, TEMP_FILE_RULE])
    # print(generate_presigned_get_url(BUCKET, "raw/data.csv", 3600))
    pass
