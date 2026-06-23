# eventbridge.tf — agendamento da ingestão.
#
# Uma regra do EventBridge (Scheduler clássico, baseado em regra) dispara a
# Lambda de ingestão no horário definido em var.schedule_expression.
#
# São necessárias 3 peças:
#   1. A regra (quando disparar).
#   2. O alvo (o que disparar — a Lambda).
#   3. A permissão para o EventBridge invocar a Lambda.

resource "aws_cloudwatch_event_rule" "ingestao_agendada" {
  name                = "${local.prefix}-ingestao-schedule"
  description         = "Dispara a ingestão de séries do BACEN periodicamente."
  schedule_expression = var.schedule_expression
}

# Liga a regra à Lambda (define a Lambda como alvo do evento).
resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.ingestao_agendada.name
  target_id = "lambda-ingestao"
  arn       = aws_lambda_function.ingestao.arn
}

# Sem esta permissão, o EventBridge NÃO consegue invocar a Lambda (erro de
# AccessDenied silencioso). `source_arn` restringe a invocação a esta regra.
resource "aws_lambda_permission" "permite_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestao.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestao_agendada.arn
}
