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
  depends_on    = [aws_cloudwatch_log_group.lambda_log_group]
}