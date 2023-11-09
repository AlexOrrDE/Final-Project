import boto3
import io


def write_to_s3(key, dataframe, bucket_name="processing-data-bucket-marble"):
    """Uploads files to AWS s3 bucket.

    Puts objects in s3 bucket with a timestamp in the filename.

    Typical usage example:

      write_to_s3(file_name, file_data)
    """

    try:
        with io.BytesIO() as output:
            dataframe.to_parquet(output, engine="fastparquet")
            output.seek(0)

            s3_key = f"{key}.parquet"

            boto3.client("s3").put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=output.read(),
            )

    except AttributeError as e:
        print(
            f"Error converting to parquet: {e}."
            "Ensure convert_to_parquet() is being passed a pandas dataframe."
        )
