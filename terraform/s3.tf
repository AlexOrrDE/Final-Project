resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "shabbir-code-bucket"
}
#  Zip up modules
data "archive_file" "pg8000" {
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