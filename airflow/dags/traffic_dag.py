from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def generate_report():
    df = pd.read_parquet("data/traffic_parquet")
    df["hour"] = pd.to_datetime(df["window.start"]).dt.hour
    result = df.groupby(["sensor_id","hour"])["total_vehicles"].sum().reset_index()
    peak = result.loc[result.groupby("sensor_id")["total_vehicles"].idxmax()]
    peak.to_csv("report/daily_traffic_report.csv", index=False)

with DAG(
    dag_id="traffic_batch_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_report
    )

