import json
import psycopg2
from confluent_kafka import Consumer

print("Connecting to PostgreSQL...")
# 1. Connect to our new Docker database
conn = psycopg2.connect(
    dbname="fraud_alerts", 
    user="aml_admin", 
    password="adminpassword", 
    host="localhost", 
    port="5432"
)
cursor = conn.cursor()

# 2. Automatically create the table if it doesn't exist yet
cursor.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id SERIAL PRIMARY KEY,
        account_id VARCHAR(50),
        amount DECIMAL,
        risk_score DECIMAL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
conn.commit()

# 3. Connect to the Kafka Alerts pipe
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'postgres-writer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['aml-alerts'])

print("🛡️ Alert Router Online. Pushing Kafka Alerts to Postgres...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue

        # Decode the alert from Kafka
        alert = json.loads(msg.value().decode('utf-8'))
        
        account = alert.get('Account', 'Unknown')
        amount = alert.get('Amount Paid', 0)
        risk = alert.get('fraud_probability', 0) * 100

        # Insert it into the database for Grafana to read
        cursor.execute(
            "INSERT INTO alerts (account_id, amount, risk_score) VALUES (%s, %s, %s)",
            (account, amount, risk)
        )
        conn.commit()
        print(f"Saved to DB: Account {account} | Risk: {risk:.1f}%")

except KeyboardInterrupt:
    print("\nShutting down Router...")
finally:
    consumer.close()
    conn.close()
