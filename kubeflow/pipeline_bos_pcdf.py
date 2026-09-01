#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline Kubeflow equivalente ao DAG do Airflow:

    extract -> clean -> load

Entrada:
    data/bos_sinteticos.csv

Saída:
    data/bos_tratados.csv

Execução local:
    python pipeline_bos_pcdf.py

Também gera:
    pipeline_bos_pcdf.yaml
"""

import io
import os

# Necessário antes de importar o KFP.
# Permite que o kfp.local instale os packages necessários
# mesmo em ambientes Python gerenciados pelo sistema (PEP 668).
os.environ.setdefault("PIP_BREAK_SYSTEM_PACKAGES", "1")

from kfp import dsl
from kfp import compiler
from kfp import local


# ============================================================
# COMPONENTE 1 - EXTRACT
# ============================================================

@dsl.component(
    base_image="python:3.11",
    packages_to_install=["pandas"]
)
def extrair_relatos(
    caminho_entrada: str
) -> str:
    """
    Lê o CSV de entrada e retorna os dados em formato JSON.
    """

    import pandas as pd

    df = pd.read_csv(
        caminho_entrada,
        sep=";"
    )

    print(f"Registros extraídos: {len(df)}")

    return df.to_json()


# ============================================================
# COMPONENTE 2 - CLEAN
# ============================================================

@dsl.component(
    base_image="python:3.11",
    packages_to_install=["pandas"]
)
def limpar_relatos(
    relatos_json: str
) -> str:
    """
    Limpa e padroniza os dados.

    Equivalente à task 'clean' do Airflow.
    """

    import io
    import pandas as pd

    df = pd.read_json(
        io.StringIO(relatos_json)
    )

    print(f"Registros antes da limpeza: {len(df)}")

    # --------------------------------------------------------
    # 1. Remover registros com dados obrigatórios vazios
    # --------------------------------------------------------

    for coluna in [
        "texto_denuncia",
        "tipo_denuncia",
        "urgencia"
    ]:
        df[coluna] = (
            df[coluna]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    df = df[
        (df["texto_denuncia"] != "") &
        (df["tipo_denuncia"] != "") &
        (df["urgencia"] != "")
    ]

    # --------------------------------------------------------
    # 2. Padronizar o texto da denúncia
    # --------------------------------------------------------

    df["texto_denuncia"] = (
        df["texto_denuncia"]
        .str.lower()
    )

    # --------------------------------------------------------
    # 3. Padronizar os valores de urgência
    # --------------------------------------------------------

    df["urgencia"] = (
        df["urgencia"]
        .str.lower()
    )

    df["urgencia"] = df["urgencia"].replace({
        "alta": "Alta",
        "média": "Média",
        "media": "Média",
        "baixa": "Baixa",
        "urgente": "Alta"
    })

    # --------------------------------------------------------
    # 4. Remover registros duplicados
    # --------------------------------------------------------

    df = df.drop_duplicates()

    print(f"Registros após a limpeza: {len(df)}")

    print("\nDados tratados:")
    print(df.to_string(index=False))

    return df.to_json()


# ============================================================
# COMPONENTE 3 - LOAD
# ============================================================

@dsl.component(
    base_image="python:3.11",
    packages_to_install=["pandas"]
)
def carregar_relatos(
    relatos_json: str,
    caminho_saida: str
) -> int:
    """
    Grava os dados tratados no CSV de saída.

    Equivalente à task 'load' do Airflow.
    """

    import io
    import pandas as pd

    df = pd.read_json(
        io.StringIO(relatos_json)
    )

    df.to_csv(
        caminho_saida,
        sep=";",
        index=False
    )

    print(f"Registros carregados: {len(df)}")
    print(f"Arquivo gerado: {caminho_saida}")

    return len(df)


# ============================================================
# PIPELINE
# ============================================================

@dsl.pipeline(
    name="pipeline-bos-pcdf",
    description=(
        "Pipeline ETL de boletins de ocorrência: "
        "extract -> clean -> load."
    ),
)
def pipeline_bos_pcdf(
    caminho_entrada: str,
    caminho_saida: str,
):
    tarefa_extrair = extrair_relatos(
        caminho_entrada=caminho_entrada
    )

    tarefa_limpar = limpar_relatos(
        relatos_json=tarefa_extrair.output
    )

    carregar_relatos(
        relatos_json=tarefa_limpar.output,
        caminho_saida=caminho_saida
    )


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":

    # Diretório onde este script está localizado.
    diretorio_projeto = os.path.dirname(
        os.path.abspath(__file__)
    )

    # Arquivos de entrada e saída.
    caminho_entrada = os.path.join(
        diretorio_projeto,
        "data",
        "bos_sinteticos.csv"
    )

    caminho_saida = os.path.join(
        diretorio_projeto,
        "data",
        "bos_tratados.csv"
    )

    print("=== 1) Rodando o pipeline localmente ===")
    print()
    print(f"Entrada: {caminho_entrada}")
    print(f"Saída:   {caminho_saida}")
    print()

    # Inicializa o executor local do KFP.
    local.init(
        runner=local.SubprocessRunner(
            use_venv=False
        )
    )

    # Executa o pipeline de verdade.
    pipeline_task = pipeline_bos_pcdf(
        caminho_entrada=caminho_entrada,
        caminho_saida=caminho_saida
    )

    print()
    print("=== Pipeline executado com sucesso ===")
    print()

    # Compila o pipeline para YAML.
    print("=== 2) Compilando o pipeline ===")
    print()

    caminho_yaml = os.path.join(
        diretorio_projeto,
        "pipeline_bos_pcdf.yaml"
    )

    compiler.Compiler().compile(
        pipeline_func=pipeline_bos_pcdf,
        package_path=caminho_yaml,
    )

    print(f"OK: {caminho_yaml} gerado.")
