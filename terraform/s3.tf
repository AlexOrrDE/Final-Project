# Create a bucket to store function code
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "ingestion-code-bucket"
}

resource "aws_s3_bucket" "ingestion_data_bucket" {
  bucket = "ingestion-data-bucket-marble"
  force_destroy = true
}

resource "aws_s3_bucket" "processed_data_bucket" {
  bucket = "processed-data-bucket-marble"
  force_destroy = true
}

# Zip directory with each of our ingestion python files
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir = "../src/ingestion"
  output_path = "../handler.zip"
}

# Turn zipped functions into s3 object
resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "ingestion/handler.zip"
  source = "${path.module}/../handler.zip"
  source_hash =  data.archive_file.lambda_zip.output_base64sha256
}

# Zip directory with each of our processing python files
data "archive_file" "processing_lambda_zip"{
  type = "zip"
  source_dir = "../src/processing"
  output_path = "../processing_handler.zip"
}

# Turn zipped functions into s3 object
resource "aws_s3_object" "processing_lambda_code"{
  bucket = aws_s3_bucket.code_bucket.id
  key = "processing/processing_handler.zip"
  source = "${path.module}/../processing_handler.zip"
  source_hash = data.archive_file.processing_lambda_zip.output_base64sha256
}

# Zip directory with each of our processing python files
data "archive_file" "loading_lambda_zip"{
  type = "zip"
  source_dir = "../src/loading"
  output_path = "../loading_handler.zip"
}

# Turn zipped functions into s3 object
resource "aws_s3_object" "loading_lambda_code"{
  bucket = aws_s3_bucket.code_bucket.id
  key = "loading/loading_handler.zip"
  source = "${path.module}/../loading_handler.zip"
  source_hash = data.archive_file.loading_lambda_zip.output_base64sha256
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