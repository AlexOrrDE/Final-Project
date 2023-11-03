import boto3
import re
import logging


def move_files_to_folder(timestamp, bucket_name="ingestion-data-bucket-marble"):
    """Moves old .csv files in bucket into timestamped folder.

    - Connects to AWS s3 service,
    - Checks if files in root directory of bucket are .csv,
    - Copies these files to timestamped directory,
    - Deletes files in root bucket directory.
    """
    try:
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket_name)

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
    except KeyError as ke:
        logging.error("Error occured in move_files_to_folder")
        raise ke
