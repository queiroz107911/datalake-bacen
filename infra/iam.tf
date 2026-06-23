# iam.tf — papéis (roles) e políticas (policies) de permissão.
#
# Princípio do MENOR PRIVILÉGIO: cada serviço recebe só as permissões que
# precisa, nos recursos específicos que usa. Nada de "Resource: *" sem motivo.
#
# São dois atores principais:
#   - A Lambda de ingestão: escreve na Bronze, escreve logs, dispara o Glue Job.
#   - O Glue (job + crawler): lê Bronze/script, escreve Silver, escreve no
#     Data Catalog e logs.

# ===========================================================================
# Role da Lambda de ingestão
# ===========================================================================

# "Trust policy": diz QUEM pode assumir esta role. No caso, o serviço Lambda.
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.prefix}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Permissões EFETIVAS da Lambda.
data "aws_iam_policy_document" "lambda_permissions" {
  # Escrever os JSONs brutos na Bronze.
  statement {
    sid       = "EscreverBronze"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.bronze.arn}/*"]
  }

  # Disparar o Glue Job após a ingestão (fluxo end-to-end).
  statement {
    sid       = "DispararGlueJob"
    actions   = ["glue:StartJobRun"]
    resources = ["arn:aws:glue:${var.aws_region}:${local.account_id}:job/${local.prefix}-transform"]
  }

  # Escrever logs no CloudWatch (observabilidade básica de toda Lambda).
  statement {
    sid = "LogsCloudWatch"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${var.aws_region}:${local.account_id}:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${local.prefix}-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_permissions.json
}

# ===========================================================================
# Role do Glue (compartilhada pelo Job e pelo Crawler)
# ===========================================================================

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

# Anexa a política gerenciada da AWS para o Glue. Ela cobre as permissões
# básicas que o serviço Glue precisa para operar (catálogo, logs, etc.).
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Permissões específicas de S3 para o Glue: ler Bronze + script, escrever Silver.
data "aws_iam_policy_document" "glue_s3" {
  # Ler os JSONs da Bronze e listar o bucket.
  statement {
    sid       = "LerBronze"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*"]
  }

  # Ler o script do Glue Job no bucket de scripts.
  statement {
    sid       = "LerScript"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.scripts.arn, "${aws_s3_bucket.scripts.arn}/*"]
  }

  # Ler e escrever na Silver (escrita do Parquet; leitura para o crawler).
  statement {
    sid = "EscreverSilver"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "${local.prefix}-glue-s3-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}
