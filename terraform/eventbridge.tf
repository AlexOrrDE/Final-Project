resource "aws_scheduler_schedule" "lambdascheduler" {
  # name       = "my-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(2 minutes)"

  target {
    arn      = aws_lambda_function.handler.arn
    role_arn = aws_iam_role.eventbride_role.arn
  }
}

resource "aws_scheduler_schedule" "thirdlambdascheduler" {
  # name       = "my-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(2 minutes)"

  target {
    arn      = aws_lambda_function.loading_handler.arn
    role_arn = aws_iam_role.eventbride_role.arn
  }
}