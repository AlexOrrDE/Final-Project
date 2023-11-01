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

# https://docs.aws.amazon.com/lambda/latest/dg/access-control-resource-based.html
# https://developer.hashicorp.com/terraform/tutorials/aws/aws-iam-policy

# Make a policy of the json file below
resource "aws_iam_policy" "test_policy" {
    name = "testnametestesttest"
    policy = data.aws_iam_policy_document.example.json
}

# Policy for invoking a lambda, from
# https://docs.aws.amazon.com/scheduler/latest/UserGuide/setting-up.html#setting-up-execution-role
# this is just, the data, it is not attached here
data "aws_iam_policy_document" "example" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = ["${aws_lambda_function.alex_s3_injestion.arn}"]
    effect = "Allow"
  }
}

# Attach policy to the eventbridge role
resource "aws_iam_role_policy_attachment" "test_attach" {
  role = aws_iam_role.eventbride_role.name
  policy_arn = aws_iam_policy.test_policy.arn
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
