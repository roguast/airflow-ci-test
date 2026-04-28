from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

# --- Simulação Lambda ---
def lambda_ingestion():
    print("🚀 Lambda: ingestão iniciada")
    time.sleep(2)
    print("✅ Lambda: ingestão concluída")

# --- Simulação S3 check ---
def s3_check():
    print("🪣 S3: verificando arquivo...")
    time.sleep(1)
    print("✅ S3: arquivo encontrado")

# --- Simulação ETL ---
def etl_process():
    print("⚙️ ETL: processamento iniciado")
    time.sleep(3)
    print("✅ ETL: processamento concluído")

# --- Validação final ---
def validation():
    print("🔍 Validação: conferindo dados finais...")
    time.sleep(1)
    print("✅ Validação OK")

with DAG(
    dag_id="aws_pipeline_observability",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "observability"]
) as dag:

    t1 = PythonOperator(task_id="lambda_ingestion", python_callable=lambda_ingestion)
    t2 = PythonOperator(task_id="s3_check", python_callable=s3_check)
    t3 = PythonOperator(task_id="etl_process", python_callable=etl_process)
    t4 = PythonOperator(task_id="validation", python_callable=validation)

    t1 >> t2 >> t3 >> t4
