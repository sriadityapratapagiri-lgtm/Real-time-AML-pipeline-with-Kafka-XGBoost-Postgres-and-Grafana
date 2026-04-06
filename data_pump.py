import pandas as pd
import json
import time
from confluent_kafka import Producer

print("Loading streaming dataset...")
# Load the 30% holdout data we saved in Jupyter
df_stream = pd.read_csv('dev_stream.csv')

# 1. Connect to Kafka
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

print(f"Loaded {len(df_stream)} transactions. Starting the data pump...")
print("Pumping 10 transactions per second. Press Ctrl+C to stop.")

try:
    for index, row in df_stream.iterrows():
        # Convert the Pandas row into a standard JSON dictionary
        tx_dict = row.to_dict()
        
        # Send it to the exact topic your Stream Processor is listening to
        producer.produce('raw-transactions', value=json.dumps(tx_dict).encode('utf-8'))
        
        # Send the data immediately
        producer.flush()
        
        # Pause for 0.1 seconds to simulate real-time global traffic
        time.sleep(0.1)

except KeyboardInterrupt:
    print("\nStopping the Data Pump...")
