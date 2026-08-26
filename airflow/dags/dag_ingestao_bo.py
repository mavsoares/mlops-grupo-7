from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import io
import pandas as pd


def extract(**kw):
    df = pd.read_csv(
        "/opt/airflow/data/bos_sinteticos.csv",
        sep=";"
    )

    print(f"Registros extraídos: {len(df)}")

    kw["ti"].xcom_push(
        key="raw",
        value=df.to_json()
    )


def clean(**kw):
    raw = kw["ti"].xcom_pull(
        task_ids="extract",
        key="raw"
    )

    df = pd.read_json(io.StringIO(raw))

    print(f"Registros antes da limpeza: {len(df)}")

    # 1. Remover registros com dados obrigatórios vazios
    for coluna in ["texto_denuncia", "tipo_denuncia", "urgencia"]:
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

    # 2. Padronizar o texto da denúncia
    df["texto_denuncia"] = (
        df["texto_denuncia"]
        .str.lower()
    )

    # 3. Padronizar os valores de urgência
    df["urgencia"] = df["urgencia"].str.lower()

    df["urgencia"] = df["urgencia"].replace({
        "alta": "Alta",
        "média": "Média",
        "media": "Média",
        "baixa": "Baixa",
        "urgente": "Alta"
    })

    # 4. Remover registros duplicados
    df = df.drop_duplicates()

    print(f"Registros após a limpeza: {len(df)}")

    kw["ti"].xcom_push(
        key="clean",
        value=df.to_json()
    )


def load(**kw):
    clean_data = kw["ti"].xcom_pull(
        task_ids="clean",
        key="clean"
    )

    df = pd.read_json(
        io.StringIO(clean_data)
    )

    df.to_csv(
        "/opt/airflow/data/bos_tratados.csv",
        sep=";",
        index=False
    )

    print(f"Registros carregados: {len(df)}")


with DAG(
    "ingestao_bo_pcdf",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="clean",
        python_callable=clean
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load
    )

    t1 >> t2 >> t3