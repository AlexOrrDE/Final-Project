# Create a bucket to store function code
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "shabbir-code-bucket"
}

# Zip up our function
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/injestion/alex_s3_injestion.py"
  output_path = "${path.module}/../alex_s3_injestion.zip"
}

# Turn zipped function into s3 object
resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "s3_file_reader/alex_s3_injestion.zip"
  source = "${path.module}/../alex_s3_injestion.zip"
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