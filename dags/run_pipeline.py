import pendulum 
import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.extract import extract, load_bronze_datalake
from src.transform import transform, load_silver_datalake
from src.build_metrics import build_metrics, load_gold_datalake
from src.load import load 

BRONZE = "/opt/airflow/data_lake/bronze"
SILVER = "/opt/airflow/data_lake/silver/analise_geral_2025.parquet"
GOLD = "/opt/airflow/data_lake/gold"

def task_extract_load_bronze():
    df_bronze = extract()
    load_bronze_datalake(df_bronze)

def task_transform_load_silver():
    df_semestre_1 = pd.read_parquet(f"{BRONZE}/analise_semestre_1.parquet")
    df_semestre_2 = pd.read_parquet(f"{BRONZE}/analise_semestre_2.parquet")
    df_silver = transform(df_semestre_1, df_semestre_2)
    load_silver_datalake(df_silver)

def task_build_metrics_load_gold():
    df = pd.read_parquet(SILVER)
    df_gold = build_metrics(df)
    load_gold_datalake(df_gold)

def task_load():
    df_tempo = pd.read_parquet(f"{GOLD}/dim_tempo.parquet")
    df_revenda = pd.read_parquet(f"{GOLD}/dim_revenda.parquet")
    df_produto = pd.read_parquet(f"{GOLD}/dim_produto.parquet")
    df_localizacao = pd.read_parquet(f"{GOLD}/dim_localizacao.parquet")
    fato_preco = pd.read_parquet(f"{GOLD}/fato_preco.parquet")
    
    load(
        df_tempo,
        df_revenda,
        df_produto,
        df_localizacao,
        fato_preco
    )
    
with DAG(
    dag_id = "Monitoramento_preco_combustivel",
    start_date = pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup = False,
    schedule = None 
) as dag:
    
    extract_task = PythonOperator(
        task_id = "extract_load_bronze",
        python_callable = task_extract_load_bronze
    )
    
    transform_task = PythonOperator(
        task_id = "transform_load_silver",
        python_callable = task_transform_load_silver
    )
    
    build_metrics_task = PythonOperator(
        task_id = "build_metrics_load_gold",
        python_callable = task_build_metrics_load_gold
    )
    
    load_task = PythonOperator(
        task_id = "load_dw",
        python_callable = task_load
    )
    
    extract_task >> transform_task >> build_metrics_task >> load_task
        