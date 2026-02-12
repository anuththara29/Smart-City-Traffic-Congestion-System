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

    print("Loaded columns:", df.columns)

    # Extract start time from window struct
    df["window_start"] = df["window"].apply(
        lambda x: x["start"] if isinstance(x, dict) else None
    )

    # Convert nanoseconds to datetime
    df["window_start"] = pd.to_datetime(df["window_start"], unit="ns")

    df["hour"] = df["window_start"].dt.hour

    # Aggregate
    summary = (
        df.groupby(["sensor_id", "hour"])["total_vehicles"]
        .sum()
        .reset_index()
    )

    # Find peak hour per sensor
    peak = summary.loc[
        summary.groupby("sensor_id")["total_vehicles"].idxmax()
    ]

    # Save report
    output_file = f"{out}/daily_traffic_report.csv"
    peak.to_csv(output_file, index=False)

    print("Report saved:", output_file)


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
