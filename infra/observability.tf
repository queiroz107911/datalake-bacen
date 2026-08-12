resource "aws_sns_topic" "alertas_pipeline" {
  name = "${local.prefix}-alertas"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alertas_pipeline.arn
  protocol  = "email"
  endpoint  = var.alert_email
}