from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


def generate_report():
    path = "/opt/airflow/data/traffic_parquet"
    out = "/opt/airflow/report"

    os.makedirs(out, exist_ok=True)

    # Load parquet produced by Spark
    df = pd.read_parquet(path)

    if df.empty:
        print("No data found.")
        return

    print("Loaded columns:", df.columns)

    # Extract window start
    df["window_start"] = df["window"].apply(
        lambda x: x["start"] if isinstance(x, dict) else None
    )

    df["window_start"] = pd.to_datetime(df["window_start"], unit="ns")
    df["hour"] = df["window_start"].dt.hour

    # Traffic per sensor per hour
    sensor_hourly = (
        df.groupby(["sensor_id", "hour"])["total_vehicles"]
        .sum()
        .reset_index()
    )

    sensor_hourly.to_csv(
        f"{out}/traffic_per_sensor_per_hour.csv",
        index=False
    )

    # Overall traffic per hour 
    hourly_total = (
        df.groupby("hour")["total_vehicles"]
        .sum()
        .reset_index()
    )

    hourly_total.to_csv(
        f"{out}/traffic_volume_vs_time.csv",
        index=False
    )

    # Peak hour per sensor
    peak = sensor_hourly.loc[
        sensor_hourly.groupby("sensor_id")["total_vehicles"].idxmax()
    ]

    peak.to_csv(
        f"{out}/peak_hour_per_sensor.csv",
        index=False
    )

    print("Reports generated successfully.")


with DAG(
    dag_id="traffic_batch_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["traffic", "batch", "spark"],
) as dag:

    generate_daily_report = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_report,
    )
