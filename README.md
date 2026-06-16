#  Real-Time Anti-Money Laundering (AML) Detection Engine

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-black)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-Machine%20Learning-orange)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestration-blue)

##  Overview
A high-throughput, real-time anomaly detection system designed to identify suspicious financial transactions and potential money laundering activities. Engineered with a focus on production-grade MLOps, this system ingests live transaction streams, computes behavioral and velocity-based features on the fly, and runs them through a machine learning inference pipeline to flag fraudulent behavior with sub-second latency.

##  System Architecture
* **Data Ingestion:** Live financial transaction streams are ingested and buffered using **Apache Kafka** to handle high-velocity data continuously.
* **Feature Engineering:** Dynamic computation of velocity metrics (e.g., transaction frequency, sudden volume spikes) and behavioral profiling is applied directly to incoming data streams.
* **Machine Learning Inference:** A **Scikit-learn** anomaly detection model evaluates the engineered features in real-time to generate fraud probability scores.
* **Infrastructure & Deployment:** The complete microservice architecture is containerized using **Docker** and orchestrated via **Kubernetes**, ensuring high availability, fault tolerance, and seamless scalability.

##  Key Features
* **Real-Time Streaming Inference:** Shifts from traditional batch processing to continuous, live data evaluation for immediate threat detection.
* **Velocity-Based Feature Engineering:** Captures complex, time-sensitive fraud patterns that easily evade static, rules-based systems.
* **Production-Ready Pipeline:** Fully containerized inference services deployed on scalable clusters, bridging the gap between data science and robust software engineering.

##  Tech Stack
* **Languages:** Python
* **Data Streaming:** Apache Kafka
* **Machine Learning:** Scikit-learn, Pandas, NumPy
* **DevOps & Infrastructure:** Docker, Kubernetes

##  Local Setup & Installation

### Prerequisites
* Docker & Docker Compose
* Python 3.9+
* Git

### Quick Start
1. **Clone the repository:**
   ```bash
   git clone [https://github.com/sriadityapratapagiri-lgtm/real-time-aml-detection.git](https://github.com/sriadityapratapagiri-lgtm/real-time-aml-detection.git)
   cd real-time-aml-detection
