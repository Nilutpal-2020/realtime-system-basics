# Fault-Tolerant Realtime Data Ingestion with QuestDB Write-Ahead Logs

### How to Run
1. **Save the files**: Create the directories (producer, consumer) and save the Dockerfiles, `requirements.txt` files, and your Python scripts in the correct locations.

2. Open your terminal in the root directory where `docker-compose.yml` is located.

3. Build and run the services:

```bash
docker-compose up --build
```

This command will:

- Build the producer and consumer images from their Dockerfiles.
- Download the confluentinc/cp-zookeeper, confluentinc/cp-kafka, and questdb/questdb images.
- Start all services in the correct order, waiting for dependencies (e.g., Kafka waits for Zookeeper).

### Testing the Setup
1. **QuestDB Console**: Open your browser to `http://localhost:9000/index.html`. You should see the QuestDB web console.
2. **Create Table**: In the QuestDB console, run your CREATE TABLE statement:
```sql
CREATE TABLE locations (
    id SYMBOL,
    lat DOUBLE,
    lon DOUBLE,
    timestamp TIMESTAMP
) timestamp(timestamp) PARTITION BY DAY WITH WAL;
```
3. **Send Data (to FastAPI)**: In a separate terminal, send data to your FastAPI producer:
```bash
curl -X POST http://localhost:8000/track -H "Content-Type: application/json" -d '{"id": "v1", "lat": 34.0522, "lon": -118.2437}'
```
You should get a `{"status": "success", ...}` response. Send a few more with different `ids` or `lat/lon` values.
5. **Verify Data in QuestDB**: Go back to the QuestDB console and query your table:
```sql
SELECT * FROM locations;
```
