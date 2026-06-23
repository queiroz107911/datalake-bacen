# s3.tf — buckets S3 das camadas do data lake e de apoio.
#
# Criamos 4 buckets, cada um com responsabilidade única:
#   - bronze:         JSON bruto vindo da API (camada de aterrissagem).
#   - silver:         Parquet limpo e particionado (camada confiável).
#   - scripts:        guarda o script .py do Glue Job.
#   - athena_results: guarda os resultados das consultas do Athena.
#
# Em todos aplicamos boas práticas: bloqueio de acesso público, versionamento e
# criptografia em repouso. `force_destroy = true` permite destruir o ambiente de
# dev mesmo com objetos dentro (NÃO use em produção sem pensar duas vezes).

# ----------------------------- Bucket Bronze -------------------------------
resource "aws_s3_bucket" "bronze" {
  bucket        = local.bronze_bucket_name
  force_destroy = true
}

# ----------------------------- Bucket Silver -------------------------------
resource "aws_s3_bucket" "silver" {
  bucket        = local.silver_bucket_name
  force_destroy = true
}

# ----------------------------- Bucket Scripts ------------------------------
resource "aws_s3_bucket" "scripts" {
  bucket        = local.scripts_bucket_name
  force_destroy = true
}

# ------------------------- Bucket Athena Results ---------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_bucket_name
  force_destroy = true
}

# Lista local com todos os buckets, para aplicar as mesmas configurações de
# segurança a cada um sem repetir blocos (DRY via for_each).
locals {
  todos_buckets = {
    bronze         = aws_s3_bucket.bronze.id
    silver         = aws_s3_bucket.silver.id
    scripts        = aws_s3_bucket.scripts.id
    athena_results = aws_s3_bucket.athena_results.id
  }
}

# Bloqueia QUALQUER acesso público. Data lake nunca deve ser público por padrão.
resource "aws_s3_bucket_public_access_block" "todos" {
  for_each = local.todos_buckets
  bucket   = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versionamento: protege contra sobrescrita/exclusão acidental, mantendo
# histórico de versões dos objetos.
resource "aws_s3_bucket_versioning" "todos" {
  for_each = local.todos_buckets
  bucket   = each.value

  versioning_configuration {
    status = "Enabled"
  }
}

# Criptografia em repouso (SSE-S3 / AES256). Padrão mínimo de segurança.
resource "aws_s3_bucket_server_side_encryption_configuration" "todos" {
  for_each = local.todos_buckets
  bucket   = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Faz upload do script do Glue Job para o bucket de scripts. O Glue lê o .py
# diretamente do S3 na hora de executar o job.
#
# `etag` força o re-upload sempre que o conteúdo do arquivo muda (detecta
# alteração via hash do conteúdo).
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue/transform_job.py"
  source = "${path.module}/../glue/transform_job.py"
  etag   = filemd5("${path.module}/../glue/transform_job.py")
}
