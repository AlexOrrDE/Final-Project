import boto3
from datetime import datetime
import re


def move_files_to_folder(bucket_name="marble-test-bucket"):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    folder_name = f"{timestamp}/"

    csv_pattern = re.compile(r"^[^/]*\.csv$")

    for obj in response.get("Contents", []):
        file_name = obj["Key"]

        if csv_pattern.match(file_name):
            destination_name = f"{folder_name}{file_name}"

            s3.copy_object(
                CopySource={"Bucket": bucket_name, "Key": file_name},
                Bucket=bucket_name,
                Key=destination_name,
            )

            s3.delete_object(Bucket=bucket_name, Key=file_name)
