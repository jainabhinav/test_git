from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test_pipeline.config.ConfigStore import *
from test_pipeline.functions import *

def s3_file_operations(spark: SparkSession):
    if not 'SColumnExpression' in locals():
        import re
        import boto3
        from urllib.parse import urlparse
        from botocore.exceptions import ClientError
        mode = "copy"
        fileRegex = None
        ignoreEmptyFiles = False
        src_url = urlparse("bjjl")
        dest_url = urlparse(" nkbkj")
        src_bucket = src_url.netloc
        src_prefix = src_url.path.lstrip('/')
        dest_bucket = dest_url.netloc
        dest_prefix = dest_url.path.lstrip('/')
        s3 = boto3.client("s3")

        for obj in boto3.client("s3").list_objects_v2(Bucket = src_url.netloc, Prefix = src_url.path.lstrip('/'))['Contents']:
            new_dest_prefix = re.sub(src_prefix, dest_prefix, obj['Key'], 1)
            src_details = s3.head_object(Bucket = src_bucket, Key = obj['Key'])

            if mode in ['copy', 'move', 'sync'] and not obj['Key'].endswith("/"):

                if mode == 'sync':
                    try:
                        dest_details = s3.head_object(Bucket = dest_bucket, Key = new_dest_prefix)
                    except ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            dest_details = None
                        else:
                            raise e

                if (
                    (
                      bool(ignoreEmptyFiles)
                      and src_details['ContentLength'] == 0
                    )
                    or (
                      bool(fileRegex)
                      and fileRegex != ""
                      and not bool(re.compile(fileRegex).match(obj['Key']))
                    )
                    or (
                      mode == "sync"
                      and bool(dest_details)
                      and src_details['LastModified'] <= dest_details['LastModified']
                    )
                ):
                    continue

                s3.copy({'Bucket' : src_bucket, 'Key' : obj['Key']}, dest_bucket, new_dest_prefix)

                if mode == "move":
                    s3.delete_object(Bucket = src_bucket, Key = obj['Key'])
