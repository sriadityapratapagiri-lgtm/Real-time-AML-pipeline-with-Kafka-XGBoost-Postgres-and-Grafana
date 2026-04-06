import json
import joblib
import pandas as pd
import numpy as np
from confluent_kafka import Consumer, Producer

print("Loading Brain (XGBoost) and Memory (Feature Names)...")
model = joblib.load('aml_xgboost.pkl')
expected_features = joblib.load('model_features.pkl')

# 1. Setup Kafka Consumer (Listens to raw data)
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'aml-fraud-hunters',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['raw-transactions'])

# 2. Setup Kafka Producer (Sends fraud alerts)
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

def process_single_transaction(raw_tx_dict):
    """Transforms a raw JSON transaction into the exact format XGBoost requires."""
    df = pd.DataFrame([raw_tx_dict])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Recreate the exact features from Jupyter
    df['Cross_Bank_Transfer'] = (df['From Bank'] != df['To Bank']).astype(int)
    df['Hour_of_Day'] = df['Timestamp'].dt.hour
    df['Currency_Mismatch'] = (df['Receiving Currency'] != df['Payment Currency']).astype(int)
    df['Log_Amount_Paid'] = np.log1p(df['Amount Paid'].astype(float))
    
    # Handle the One-Hot Encoding on the fly
    for col in expected_features:
        if col not in df.columns:
            if col.startswith('Format_'):
                expected_format = col.replace('Format_', '')
                df[col] = (df['Payment Format'] == expected_format).astype(int)
            else:
                df[col] = 0 # Safety fallback
                
    return df[expected_features]

print("🧠 Stream Processor Online. Listening to 'raw-transactions'...")

try:
    while True:
        # Poll Kafka every 1.0 seconds for new transactions
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Decode the raw JSON message from Kafka
        raw_tx = json.loads(msg.value().decode('utf-8'))
        
        # 1. Transform the data
        features = process_single_transaction(raw_tx)
        
        # 2. Ask XGBoost for a probability (predict_proba returns [[prob_normal, prob_fraud]])
        fraud_probability = model.predict_proba(features)[0][1]
        
        # 3. Apply our custom Risk Appetite Threshold (20%)
        if fraud_probability > 0.20:
            print(f"🚨 ALERT! 🚨 Transaction {raw_tx.get('Account', 'Unknown')} is {fraud_probability*100:.1f}% suspicious!")
            
            # Send the exact transaction + the score to the alerts pipe
            alert_message = raw_tx.copy()
            alert_message['fraud_probability'] = float(fraud_probability)
            
            producer.produce('aml-alerts', value=json.dumps(alert_message).encode('utf-8'))
            producer.flush()
        else:
            print(f"✅ Normal (Risk: {fraud_probability*100:.1f}%) | Amount: ${raw_tx.get('Amount Paid', 0)}")

except KeyboardInterrupt:
    print("\nShutting down Stream Processor...")
finally:
    consumer.close()
