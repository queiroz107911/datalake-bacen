# glue.tf — Glue Job (transformação), Database e Crawler (catalogação).
#
# Fluxo: o Glue Job lê a Bronze e escreve Parquet na Silver. O Glue Crawler
# varre a Silver e cria/atualiza tabelas no Glue Data Catalog, tornando os dados
# consultáveis pelo Athena.

# ===========================================================================
# Glue Job (Python Shell) — transformação Bronze -> Silver
# ===========================================================================
resource "aws_glue_job" "transform" {
  name     = "${local.prefix}-transform"
  role_arn = aws_iam_role.glue.arn

  # Python Shell: ideal para volumes pequenos/médios (não precisa de Spark).
  command {
    name            = "pythonshell"
    python_version  = var.glue_python_version
    script_location = "s3://${aws_s3_bucket.scripts.id}/${aws_s3_object.glue_script.key}"
  }

  # 1 DPU (= 0.0625 ou 1 conforme tipo) é o suficiente para Python Shell pequeno.
  max_capacity = 1

  default_arguments = {
    # Caminhos de entrada/saída lidos pelo script via getResolvedOptions.
    "--BRONZE_PATH" = "s3://${aws_s3_bucket.bronze.id}/sgs/"
    "--SILVER_PATH" = "s3://${aws_s3_bucket.silver.id}/sgs/"

    # Conjunto "analytics" do Python Shell 3.9 JÁ traz awswrangler (2.15.1),
    # pandas, boto3 e s3fs pré-instalados. Por isso NÃO usamos
    # --additional-python-modules: forçar awswrangler==3.9.1 obrigaria o pip a
    # compilar o pyarrow do zero, e o ambiente do Python Shell não tem cmake
    # (a build falha). Usar o que já vem pronto evita o problema por completo.
    "--library-set" = "analytics"

    # NÃO usamos --enable-metrics nem --enable-continuous-cloudwatch-log: esses
    # argumentos são exclusivos de jobs Glue *Spark* e são ignorados num job
    # Python Shell. O Python Shell já grava stdout/stderr no CloudWatch por
    # padrão (grupos /aws-glue/python-jobs/output e .../error).
  }

  max_retries = 0

  # Timeout em MINUTOS. Job pequeno termina rápido; 20 min é folga de sobra.
  timeout = 20
}

# ===========================================================================
# Glue Data Catalog — banco de dados lógico
# ===========================================================================
resource "aws_glue_catalog_database" "this" {
  name = replace("${local.prefix}_catalog", "-", "_")
  # Nomes de database/tabela no Glue não aceitam hífen, por isso o replace.
}

# ===========================================================================
# Glue Crawler — cataloga a camada Silver
# ===========================================================================
resource "aws_glue_crawler" "silver" {
  name          = "${local.prefix}-silver-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.this.name

  # Aponta para a raiz da Silver. O crawler infere o schema e as partições
  # (serie_codigo, ano, mes) a partir da estrutura de pastas do Parquet.
  s3_target {
    path = "s3://${aws_s3_bucket.silver.id}/sgs/"
  }

  # Prefixo nas tabelas criadas, para identificá-las facilmente no catálogo.
  table_prefix = "silver_"

  # Mantém o schema estável e só adiciona partições novas em execuções futuras.
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}
