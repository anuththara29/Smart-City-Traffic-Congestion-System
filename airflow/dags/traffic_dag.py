from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


def generate_report():
    path = "/opt/airflow/data/traffic_parquet"
    out = "/opt/airflow/report"

    os.makedirs(out, exist_ok=True)

    df = pd.read_parquet(path)

    if df.empty:
        print("No data found.")
        return

    # Extract window start
    df["window_start"] = df["window"].apply(
        lambda x: x["start"] if isinstance(x, dict) else None
    )

    df["window_start"] = pd.to_datetime(df["window_start"], unit="ns")
    df["hour"] = df["window_start"].dt.hour

    # Traffic Volume vs Time 
    traffic_volume = (
        df.groupby(["sensor_id", "hour"])["total_vehicles"]
        .sum()
        .reset_index()
    )

    traffic_volume.to_csv(
        f"{out}/traffic_volume_vs_time.csv",
        index=False
    )

    # Peak Hour per Sensor
    peak = traffic_volume.loc[
        traffic_volume.groupby("sensor_id")["total_vehicles"].idxmax()
    ]

    # Decision Logic 
    threshold = 1800
    peak["needs_intervention"] = peak["total_vehicles"] > threshold

    peak.to_csv(
        f"{out}/peak_traffic_report.csv",
        index=False
    )

    print("Reports generated successfully.")


with DAG(
    dag_id="traffic_batch_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["traffic", "batch"],
) as dag:

    generate_daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_report,
    )