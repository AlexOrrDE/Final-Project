# Create a bucket to store function code
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "shabbir-code-bucket"
}

# Zip up our function
# data "archive_file" "lambda_zip" {
#   type        = "zip"
#   source_file = "${path.module}/../src/ingestion/handler.py"
#   output_path = "${path.module}/../handler.zip"
# }

# Trying to zip directory with each of our python files
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