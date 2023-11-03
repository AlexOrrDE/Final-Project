resource "aws_cloudwatch_log_metric_filter" "error" {
  name           = "any_alarm"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.lambda_log_group.name

  metric_transformation {
    name      = "EventCount"
    namespace = "any_error"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_errors" {
  alarm_name          = "any_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "EventCount"
  namespace           = "any_error"
  period              = 60
  statistic           = "SampleCount"
  threshold           = 1
  alarm_description   = "This metric monitors any error occusrence"
  alarm_actions       = ["arn:aws:sns:eu-west-2:377515970402:ingestion_errors"]
}