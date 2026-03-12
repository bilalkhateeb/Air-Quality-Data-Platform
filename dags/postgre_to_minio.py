from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


POSTGRES_CONN_ID = "postgres_source"
MINIO_CONN_ID = "minio_raw"
MINIO_BUCKET = "raw"
SOURCE_SCHEMA = "Aria"

TABLES = [
    "comuni", "comuni_usl", "comuni_zone", "province", "rqa_configurazioni", "rqa_dati_aria_cor", "rqa_elementi",
    "rqa_enti", "rqa_metodi", "rqa_parametri", "rqa_t_frequenze", "rqa_t_reti", "rqa_t_stclass_eu", "rqa_t_zona", 
    "rqa_ubicazioni", "t_unita_misura", "v_rqa_rete_regionale_ipr",
]


def build_task_configs() -> list[dict]:
    configs = []

    for table_name in TABLES:
        configs.append(
            {
                "task_id": f"load_{table_name}",
                "query": f'SELECT * FROM "{SOURCE_SCHEMA}"."{table_name}"',
                "s3_key": f"air_quality/{table_name}/{table_name}.parquet",
            }
        )

    return configs


@dag(
    dag_id="postgre_to_minio",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["air_quality", "postgres", "minio", "raw"],
    description="Dynamically load PostgreSQL tables into MinIO raw as Parquet files",
)
def postgre_to_minio():
    SqlToS3Operator.partial(
        task_id="load_table",
        sql_conn_id=POSTGRES_CONN_ID,
        aws_conn_id=MINIO_CONN_ID,
        s3_bucket=MINIO_BUCKET,
        replace=False,
        file_format="parquet",
    ).expand_kwargs(build_task_configs())


postgre_to_minio()