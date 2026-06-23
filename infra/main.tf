# main.tf — configuração base do Terraform e do provider AWS.
#
# Este arquivo define:
#   - As versões mínimas do Terraform e dos providers (reprodutibilidade).
#   - O provider AWS (região e tags padrão aplicadas a TODOS os recursos).
#   - Dados de contexto da conta (account_id), usados para nomear buckets de
#     forma globalmente única.
#   - Um workgroup do Athena com local de resultados configurado.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # default_tags aplica estas tags automaticamente a todo recurso que suporta
  # tags. Boa prática de governança: dá pra rastrear custo e dono de cada recurso.
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# Identidade da conta AWS atual. Usamos o account_id como sufixo nos nomes de
# bucket, porque nomes de bucket S3 são GLOBALMENTE únicos (em toda a AWS).
data "aws_caller_identity" "current" {}

# Locals: valores computados reutilizados em vários arquivos. Centralizar a
# convenção de nomes aqui evita inconsistência entre s3.tf, iam.tf, etc.
locals {
  account_id = data.aws_caller_identity.current.account_id
  prefix     = "${var.project_name}-${var.environment}"

  bronze_bucket_name  = "${local.prefix}-bronze-${local.account_id}"
  silver_bucket_name  = "${local.prefix}-silver-${local.account_id}"
  scripts_bucket_name = "${local.prefix}-scripts-${local.account_id}"
  athena_bucket_name  = "${local.prefix}-athena-results-${local.account_id}"
}

# Workgroup do Athena: agrupa consultas e define ONDE os resultados são salvos.
# Sem um local de resultados, o Athena recusa a executar queries.
resource "aws_athena_workgroup" "this" {
  name = "${local.prefix}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/results/"
    }
  }

  force_destroy = true
}
