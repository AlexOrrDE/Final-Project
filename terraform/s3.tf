# Create a bucket to store function code
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "ingestion-code-bucket"
}

resource "aws_s3_bucket" "ingestion_data_bucket" {
  bucket = "ingestion-data-bucket-marble"
}

resource "aws_s3_bucket" "processed_data_bucket" {
  bucket = "processed-data-bucket-marble"
}

# If we just want to zip one python file (with not local dependencies)
# data "archive_file" "lambda_zip" {
#   type        = "zip"
#   source_file = "${path.module}/../src/ingestion/handler.py"
#   output_path = "${path.module}/../handler.zip"
# }

# Zip directory with each of our python files
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir = "../src/ingestion"
  output_path = "../handler.zip"
}

# Turn zipped function into s3 object
resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "ingestion/handler.zip"
  source = "${path.module}/../handler.zip"
}



#  Zip up modules
data "archive_file" "modules" {
  type        = "zip"
  source_dir = "../layer"
  output_path = "../layer.zip"
}

# Turn modules to s3 object
resource "aws_s3_object" "packages" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "layer"
  source = "${path.module}/../layer.zip"
}