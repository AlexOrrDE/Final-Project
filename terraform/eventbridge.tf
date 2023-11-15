# Evnetbridge schedule for calling the first (ingestion) lambda function
resource "aws_scheduler_schedule" "lambdascheduler" {
  # name       = "my-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(5 minutes)"

  target {
    arn      = aws_lambda_function.handler.arn
    role_arn = aws_iam_role.eventbride_role.arn
  }
}

# Eventbridge schedule for calling the third (loading) lambda function
resource "aws_scheduler_schedule" "thirdlambdascheduler" {
  # name       = "my-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(5 minutes)"

  target {
    arn      = aws_lambda_function.loading_handler.arn
    role_arn = aws_iam_role.eventbride_role.arn
  }
}