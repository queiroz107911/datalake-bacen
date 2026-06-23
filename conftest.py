"""Configuração de path para o pytest.

Os módulos `handler` (lambda/) e `transform_job` (glue/) ficam em pastas que não
são pacotes Python instaláveis. Para os testes conseguirem importá-los, adicionamos
esses diretórios ao sys.path aqui — o pytest carrega este conftest.py automaticamente
antes de coletar os testes.
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Adiciona lambda/ e glue/ ao caminho de importação do Python.
for subdir in ("lambda", "glue"):
    caminho = str(BASE_DIR / subdir)
    if caminho not in sys.path:
        sys.path.insert(0, caminho)
