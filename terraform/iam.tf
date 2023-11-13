# General lambda role, used by the three lambda functions
resource "aws_iam_role" "lambda_role" {
    name_prefix = "role-${var.lambda_name}"
    assume_role_policy = jsonencode(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    })
}

# Policy to allow a lambda access to S3
resource "aws_iam_policy" "lambda_access_s3_policy" {
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "s3-object-lambda:*"
            ],
            "Resource": "*"
        }
    ]
})
}

# Attach access s3 policy to lambda role
resource "aws_iam_role_policy_attachment" "lambda_access_s3_attach" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_access_s3_policy.arn
}

# Data for the policy for allowing the invokation of lambdas
data "aws_iam_policy_document" "example" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = ["${aws_lambda_function.handler.arn}", "${aws_lambda_function.loading_handler.arn}"]
    effect = "Allow"
  }
}

# Make a policy of the above data
resource "aws_iam_policy" "test_policy" {
    policy = data.aws_iam_policy_document.example.json
}

# Attach policy for executing lambdas to the eventbridge role
resource "aws_iam_role_policy_attachment" "test_attach" {
  role = aws_iam_role.eventbride_role.name
  policy_arn = aws_iam_policy.test_policy.arn
}

# Policy for invoking the second lambda
resource "aws_iam_policy" "invoke_second_lambda_policy" {
  # name   = "invoke_second_lambda_policy"
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "*"
        }
    ]
})
}

# Attach this policy to lambda role
resource "aws_iam_role_policy_attachment" "charles_attach" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.invoke_second_lambda_policy.arn
}


# https://us-east-1.console.aws.amazon.com/iamv2/home?region=eu-west-2#/policies/details/arn%3Aaws%3Aiam%3A%3Aaws%3Apolicy%2FAmazonEventBridgeSchedulerFullAccess?section=permissions&view=json

# https://docs.aws.amazon.com/scheduler/latest/UserGuide/setting-up.html#setting-up-execution-role

resource "aws_iam_role" "eventbride_role" {
    name_prefix = "role-${var.lambda_name}"
    assume_role_policy = jsonencode(
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "scheduler.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
})
}

# Cloudwatch policies

resource "aws_iam_policy" "cloudwatch_log_policy" {
  # name   = "function-logging-policy"
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        Action : [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Effect : "Allow",
        Resource : "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "test_joe_attach" {
  role = aws_iam_role.lambda_role.id
  policy_arn = aws_iam_policy.cloudwatch_log_policy.arn
}

# Define the log group
# https://stackoverflow.com/questions/59949808/write-aws-lambda-logs-to-cloudwatch-log-group-with-terraform#:~:text=If%20you%20want%20Terraform%20to,change%20the%20name%20at%20all.

# Log group for first lambda
resource "aws_cloudwatch_log_group" "lambda_log_group" {
    name              = "/aws/lambda/handler"
}

# Log group for second lambda
resource "aws_cloudwatch_log_group" "processing_lambda_log_group" {
    name              = "/aws/lambda/processing_handler"
}

# Log group for third lambda
resource "aws_cloudwatch_log_group" "loading_lambda_log_group" {
    name              = "/aws/lambda/loading_handler"
}

# Policies for accessing secrets
# https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access_examples.html

resource "aws_iam_policy" "secrets_policy" {
  # name   = "secrets-policy"
  policy = jsonencode({
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": ["arn:aws:secretsmanager:eu-west-2:377515970402:secret:Totesys-Credentials-WT7z06", 
      "arn:aws:secretsmanager:eu-west-2:377515970402:secret:Warehouse-Credentials-gtPzJF"]
    }
  ]
})
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "secrets_policy_attach" {
  role = aws_iam_role.lambda_role.id
  policy_arn = aws_iam_policy.secrets_policy.arn
}