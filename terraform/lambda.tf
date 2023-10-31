resource "aws_lambda_layer_version" "lambda_pg8000_layer" {
  s3_bucket =  aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.pg8000.key
  layer_name = "lambda_pg8000_layer"

  compatible_runtimes = ["python3.9"]
}

resource "aws_lambda_function" "s3_file_reader" {
  function_name = "s3-file-reader"
  role = aws_iam_role.lambda_role.arn
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.lambda_code.key
  layers = [aws_lambda_layer_version.lambda_pg8000_layer.arn]
  runtime = "python3.9"
  handler = "reader.lambda_handler"
}

resource "aws_lambda_permission" "allow_s3" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_file_reader.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.data_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}