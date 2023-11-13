# Make a layer from the s3 object that is the zipped packages
resource "aws_lambda_layer_version" "packages_layer" {
  s3_bucket =  aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.packages.key
  layer_name = "packages_layer"
  compatible_runtimes = ["python3.11"]
}

resource "aws_lambda_function" "handler" {
  function_name = "handler"
  role = aws_iam_role.lambda_role.arn
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.lambda_code.key
  layers = [aws_lambda_layer_version.packages_layer.arn]
  handler = "handler.handler"
  runtime = "python3.11"
  timeout = 300
  depends_on    = [aws_cloudwatch_log_group.lambda_log_group]
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
}

resource "aws_lambda_function" "processing_handler" {
  function_name = "processing_handler"
  role = aws_iam_role.lambda_role.arn
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.processing_lambda_code.key
  layers = [aws_lambda_layer_version.packages_layer.arn]
  handler = "handler.handler"
  runtime = "python3.11"
  timeout = 300
  depends_on = [aws_cloudwatch_log_group.processing_lambda_log_group]
  source_code_hash = data.archive_file.processing_lambda_zip.output_base64sha256
  reserved_concurrent_executions = 1
}

resource "aws_lambda_function" "loading_handler" {
  function_name = "loading_handler"
  role = aws_iam_role.lambda_role.arn
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.loading_lambda_code.key
  layers = [aws_lambda_layer_version.packages_layer.arn]
  handler = "handler.handler"
  runtime = "python3.11"
  timeout = 300
  depends_on = [aws_cloudwatch_log_group.loading_lambda_log_group]
  source_code_hash = data.archive_file.loading_lambda_zip.output_base64sha256
  reserved_concurrent_executions = 1
}