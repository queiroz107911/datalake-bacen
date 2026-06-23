# variables.tf — entradas configuráveis da infraestrutura.
#
# Centralizar variáveis aqui permite mudar região, agendamento ou séries sem
# tocar no código dos recursos. Cada variável tem tipo, descrição e default.

variable "aws_region" {
  description = "Região AWS onde os recursos serão criados."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Nome do projeto. Usado como prefixo nos nomes dos recursos."
  type        = string
  default     = "datalake-bacen"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod). Faz parte do prefixo dos recursos."
  type        = string
  default     = "dev"
}

variable "sgs_series" {
  description = "Códigos das séries do SGS/BACEN a ingerir (ex.: 12=SELIC, 433=IPCA)."
  type        = list(string)
  default     = ["12", "433"]
}

variable "lambda_runtime" {
  description = "Runtime do Python para a Lambda de ingestão."
  type        = string
  default     = "python3.11"
}

variable "lambda_timeout_seconds" {
  description = "Timeout da Lambda em segundos."
  type        = number
  default     = 60
}

variable "lambda_memory_mb" {
  description = "Memória alocada para a Lambda (MB)."
  type        = number
  default     = 128
}

variable "schedule_expression" {
  description = <<-EOT
    Expressão de agendamento do EventBridge para disparar a ingestão.
    Padrão: todo dia às 09:00 UTC. Sintaxe cron do EventBridge:
    cron(Minutos Horas Dia-do-mês Mês Dia-da-semana Ano)
  EOT
  type        = string
  default     = "cron(0 9 * * ? *)"
}

variable "glue_python_version" {
  description = "Versão do Python para o Glue Job (Python Shell)."
  type        = string
  default     = "3.9"
}
