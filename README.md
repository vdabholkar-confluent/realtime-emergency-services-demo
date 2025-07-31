# Real-time Emergency Services Demo

A full-stack demo for monitoring major incidents, personnel, and vehicle responses on a real-time dashboard using Kafka, Flink, Python (Flask), and Leaflet.js.

---

## Quickstart Setup

### 1. Clone the Repository

```bash
git clone https://github.com/vdabholkar-confluent/realtime-emergency-services-demo
cd realtime-emergency-services-demo
```


### 2. Provision Confluent Cloud Resources

- Create a **Kafka cluster** and **Confluent Flink compute** in the same cloud provider and region.
- Note down the **Bootstrap Server**, **API Key/Secret**, and **Schema Registry URL/credentials**.

### 3. Configure Connection Details

Edit the following files to add your Kafka and Schema Registry connection details:

- `base_topic_streamer.py`
- `app.py`

Update the following in both files:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_API_KEY`
- `KAFKA_API_SECRET`
- `SCHEMA REGISTRY URL`
- `SCHEMA REGISTRY basic.auth.user.info`

These can be set as environment variables or directly in the code for demo purposes.

### 4. Install Python Dependencies & Stream Demo Data

```
pip install -r requirements.txt
python base_topic_streamer.py
```
This creates the required Kafka topics and streams sample incidents, personnel, and vehicle data.

### 5. Deploy Flink SQL Streaming Pipeline

- Open the **Confluent Flink SQL UI** in your cluster.
- Copy all statements from `flink_queries.sql` and run them in order.
- This sets up the streaming joins and "gold" output topics for the dashboard.

### 6. Start the Real-time Dashboard

`python app.py`


### 7. Open the Dashboard

- Go to [http://localhost:5000](http://localhost:5000) in your browser.
- You should see incidents, vehicles, personnel, routes, and live alerts updating in real time.

---

**Note:**  
Remember to clean up cloud resources after your demo to avoid unwanted cloud costs.



