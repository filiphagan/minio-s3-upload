#!/usr/bin/env python

"""
Main.py: Uploads a file into S3-like object storage via API
"""

__author__ = "Filip Hagan"

import sys
import time
import logging
import hashlib

from typing import Union
from argparse import ArgumentParser, Namespace

from minio import Minio
from minio.helpers import ObjectWriteResult
from urllib3.exceptions import MaxRetryError, ResponseError

# Logging settings. Logs are being saved in ./s3.log
logging.basicConfig(filename="s3.log")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info(f"Run started at {time.asctime()}")


def parse_args() -> Namespace:
    """Returns argparse.Namespace with parsed parameters"""

    # Parsed parameters
    parser = ArgumentParser()
    parser.add_argument("--s3url", default="localhost:9000", type=str)
    parser.add_argument("--s3key", default="minioadmin", type=str)
    parser.add_argument("--s3secret", default="minioadmin", type=str)
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--s3path", type=str)
    parser.add_argument("-f", "--file", type=str)

    params = parser.parse_args()

    return params


def upload_to_s3(access_key: str, secret_key: str, s3_api_host: str,
                 bucket_name: str, s3_file_path: str, local_file_path: str) -> Union[ObjectWriteResult, None]:
    """Desc

    Parameters
    ----------
    access_key : str
        S3 server access key
    secret_key : str
        S3 server secret key
    s3_api_host : str
        Host:port where S3 is running
    bucket_name : str
        Unique S3 bucket name
    s3_file_path : str
        Output path to the file on S3
    local_file_path : str
        Local input file path (single file)

    Returns
    ----------
    result : ObjectWriteResult or None
        MinIO Response object after the upload (PUT event)
        Returns None if function was unable to produce the result
    """

    # Minio expects hostname:port string
    if s3_api_host[:4] == "http":
        s3_api_host = s3_api_host.split("//")[1]

    try:
        minio_client = Minio(s3_api_host, access_key=access_key, secret_key=secret_key, secure=False)
    except (ValueError, AttributeError) as e:
        logger.error(f"{time.asctime()}: Client instance error: {e}")
        return
    except (MaxRetryError, ResponseError) as e:
        logger.error(f"{time.asctime()}: Connection error: {e}")
        return

    if not minio_client.bucket_exists(bucket_name):
        logger.error(f"{time.asctime()}: Bucket {bucket_name} does not exist. Create bucket before running this script")
        return
    else:
        try:
            result = minio_client.fput_object(bucket_name, s3_file_path, local_file_path)
        except (MaxRetryError, ResponseError) as e:
            logger.error(f"{time.asctime()}: Connection error: {e}")
            return
        except FileNotFoundError as e:
            logger.error(f"{time.asctime()}: File {local_file_path} not found")
            return
        if result:
            logger.info(f"File {local_file_path} successfully uploaded to {bucket_name}{s3_file_path}")
            return result


def get_hash_md5(file_path):
    """Returns MD5 hash value of given file
    Credits: https://stackoverflow.com/a/21565932/15440362"""

    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

    return file_hash.hexdigest()


if __name__ == "__main__":

    # Parse parameters
    args = parse_args()

    # Upload file to S3
    response = upload_to_s3(access_key=args.s3key,
                            secret_key=args.s3secret,
                            s3_api_host=args.s3url,
                            bucket_name=args.bucket,
                            s3_file_path=args.s3path,
                            local_file_path=args.file)

    if not response:
        logger.info("Failed to upload the file")

    else:
        # Validate the file
        if get_hash_md5(args.file) == response.etag:
            logger.info("File MD5 hash is valid")
        else:
            logger.info("File MD5 hash is not valid")

    logger.info(f"Run ended at {time.asctime()}")
