
# Smart City Traffic Congestion System

This project implements an end-to-end real-time traffic monitoring system for a Smart City scenario. The system simulates IoT traffic sensors at four junctions in Colombo and processes streaming traffic data to detect congestion and generate daily analytical reports.

The pipeline supports:

- Real-time congestion alerts
- Window-based traffic aggregation
- Scheduled batch analytics
- Automated reporting


## System Architecture
![Screenshot](https://github.com/anuththara29/Smart-City-Traffic-Congestion-System/blob/main/assets/Architecture%20Diagram.png?raw=true)


## Data Format

Each sensor generates JSON events:
```bash
{
  "sensor_id": "J2",
  "timestamp": "2026-02-12 09:35:44",
  "vehicle_count": 43,
  "avg_speed": 34
}
```
## Data Format

Each sensor generates JSON events:
```bash
{
  "sensor_id": "J2",
  "timestamp": "2026-02-12 09:35:44",
  "vehicle_count": 43,
  "avg_speed": 34
}
```
## Real-Time Processing (Spark)
### Window-Based Aggregation
- 5-minute tumbling windows
- Event-time processing
- Aggregations:
   - Average speed
   - Total vehicle count

```bash
window(col("event_time"), "5 minutes")
```
### Congestion Detection
Congestion rule:
```bash
avg_speed < 10 km/h
```
If triggered:
- Alert printed to console
- Can publish to Critical-Traffic topic

This ensures immediate response to severe traffic conditions.
## Event Time Handling
This project uses Event-Time Processing, not Processing-Time.
```bash
.withColumn("event_time", to_timestamp("timestamp"))
.withWatermark("event_time", "1 minute")
```
Why this matters:
- Handles late-arriving data
- Ensures correct window closing
- Prevents unbounded state growth
- Produces accurate congestion metrics
## Storage Layer
Aggregated window data is stored in:
```bash
data/traffic_parquet/
```
Parquet is chosen because:
- Columnar format
- Efficient compression
- Optimized for analytical workloads
## Batch Processing (Airflow)
Airflow schedules a nightly DAG that:
- Reads Parquet data
- Extracts hourly traffic volume
- Identifies Peak Traffic Hour
- Generates daily_traffic_report.csv
## How to Run
1️⃣ Start Kafka
```bash
zookeeper-server-start.sh
kafka-server-start.sh
```
2️⃣ Run Spark Streaming
```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
spark/traffic_stream.py
```
3️⃣ Run Producer
```bash
python producer/traffic_producer.py
```
4️⃣ Start Airflow
```bash
airflow webserver
airflow scheduler
```
Trigger DAG from UI.
## Tool Justification
### Apache Kafka
- High-throughput distributed ingestion
- Fault tolerance
- Real-time streaming support

### Apache Spark Structured Streaming
- Event-time windowing
- Watermark support
- Exactly-once semantics
- Seamless Kafka integration

### Apache Airflow
- Workflow scheduling
- DAG dependency management
- Monitoring & logging

### Parquet Storage
- Columnar efficiency
- Reduced storage footprint
- Optimized for batch analytics