# outputs.tf — valores expostos após o `terraform apply`.
#
# Outputs servem para: (1) conferir rapidamente o que foi criado, e (2) alimentar
# comandos manuais (ex.: rodar o Glue Job, abrir o bucket certo no console).

output "bronze_bucket" {
  description = "Nome do bucket S3 da camada Bronze (JSON bruto)."
  value       = aws_s3_bucket.bronze.id
}

output "silver_bucket" {
  description = "Nome do bucket S3 da camada Silver (Parquet particionado)."
  value       = aws_s3_bucket.silver.id
}

output "scripts_bucket" {
  description = "Nome do bucket S3 que guarda o script do Glue Job."
  value       = aws_s3_bucket.scripts.id
}

output "athena_results_bucket" {
  description = "Nome do bucket S3 que guarda os resultados das queries do Athena."
  value       = aws_s3_bucket.athena_results.id
}

output "lambda_function_name" {
  description = "Nome da função Lambda de ingestão."
  value       = aws_lambda_function.ingestao.function_name
}

output "glue_job_name" {
  description = "Nome do Glue Job de transformação Bronze -> Silver."
  value       = aws_glue_job.transform.name
}

output "glue_database_name" {
  description = "Nome do banco de dados no Glue Data Catalog (consultável no Athena)."
  value       = aws_glue_catalog_database.this.name
}

output "glue_crawler_name" {
  description = "Nome do Glue Crawler que cataloga a camada Silver."
  value       = aws_glue_crawler.silver.name
}

output "athena_workgroup" {
  description = "Workgroup do Athena para executar as consultas."
  value       = aws_athena_workgroup.this.name
}
