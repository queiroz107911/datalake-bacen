# lambda.tf — empacotamento e definição da Lambda de ingestão.
#
# Como a Lambda usa só a biblioteca padrão + boto3 (já presente no runtime), o
# pacote de deploy é simplesmente o handler.py zipado. Sem build de dependências.

# Gera o .zip do código da Lambda a partir do diretório lambda/.
# O `archive_file` roda no `terraform plan/apply` e produz o artefato local.
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/handler.py"
  output_path = "${path.module}/build/lambda_ingestao.zip"
}

resource "aws_lambda_function" "ingestao" {
  function_name = "${local.prefix}-ingestao"
  role          = aws_iam_role.lambda.arn

  # handler = "<arquivo>.<função>". Nosso arquivo é handler.py e a função é
  # lambda_handler.
  handler = "handler.lambda_handler"
  runtime = var.lambda_runtime

  filename = data.archive_file.lambda_zip.output_path
  # source_code_hash detecta mudança no código e força novo deploy quando o zip muda.
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  timeout     = var.lambda_timeout_seconds
  memory_size = var.lambda_memory_mb

  # Variáveis de ambiente: a Lambda lê tudo daqui (nada hardcoded no código).
  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.bronze.id
      SGS_SERIES    = join(",", var.sgs_series)
      GLUE_JOB_NAME = aws_glue_job.transform.name
    }
  }
}

# Log group explícito com retenção definida. Se não criarmos, a Lambda cria um
# automaticamente com retenção infinita (acumula custo). 14 dias é razoável p/ dev.
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.ingestao.function_name}"
  retention_in_days = 14
}
