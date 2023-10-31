# Make two buckets
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "nc-jm-de-code-"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "nc-jm-de-data-"
}

# Zip up the function
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../file_reader/reader.py"
  output_path = "${path.module}/../function.zip"
}

# Turn zipped folder into s3 object
resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "s3_file_reader/function.zip"
  source = "${path.module}/../function.zip"
}

# Zip up modules
data "archive_file" "pg8000" {
  type        = "zip"
  source_dir = "../layer"
  output_path = "../layer.zip"
}

# Turn modules to s3 object
resource "aws_s3_object" "pg8000" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "layer"
  source = "${path.module}/../layer.zip"
}

# Make bucket notification object
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_file_reader.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# Add a file to bucket (for testing)
resource "aws_s3_object" "demo_object" {
    key = "requirements.txt"
    bucket = aws_s3_bucket.data_bucket.id
    source = "../requirements.txt"
}