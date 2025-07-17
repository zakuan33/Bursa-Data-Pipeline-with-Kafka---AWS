# Bursa Market Share Analysis (Kafka + AWS Glue ETL)

A real-time-capable batch data pipeline for analyzing Bursa stock market share, built using **Kafka** hosted on an **AWS EC2**  instance, processes it using **AWS Glue**,stores results in **S3** , and **Athena** for analytics. The pipeline collects market share data via a custom scraper, ingests it into Kafka, stores batches in S3, and processes it using a **Medallion Architecture** (Bronze → Silver → Gold).

This project demonstrates cloud-native data engineering skills using streaming infrastructure, scalable ETL, and lakehouse querying.

---

## 🚀 Technologies & Purpose

| Tool / Tech        | Purpose                                                                 |
|--------------------|-------------------------------------------------------------------------|
| **Kafka (KRaft)**  | Handles ingestion of scraped stock data; batch-oriented in this project but can be switched to real-time with minimal changes. |
| **KRaft Mode**     | Kafka without ZooKeeper – simpler ops with controller + broker in one.  |
| **Kafdrop**        | Open-source web UI to view Kafka topics and messages (running via Docker). |
| **AWS EC2**        |  Ubuntu server used to deploy and manage Kafka. |
| **AWS S3**         | Acts as the data lake, storing raw, silver, and gold layer data.        |
| **AWS Glue**       | Serverless Spark-based ETL to clean, enrich, and transform the stock data. |
| **AWS Athena**     | SQL interface to query cleaned data directly from S3 (gold layer).       |
| **Python (Kafka, Pandas)** | Used for data producer, consumer, and pre-cleaning logic.      |
| **.env**           | Secure storage of sensitive configs like broker IPs and bucket names.   |

---

## 🧱 Medallion Architecture Flow

[Bursa Screener Website]
|
▼
[Scraper (Python + Selenium)]
|
▼
[Kafka Producer → Kafka Topic]
| (Kafka on AWS EC2)
▼
[Kafka Consumer (Python)]
|
▼
[Raw S3 Bucket: bursa-raw (NDJSON)]
|
▼
[AWS Glue ETL Job]
|
├──> Curated S3 (bursa-curated)(cleaned fields)
└──> Transformed S3 (bursa-transformed)(derived features like spread, pressure)
|
▼
[AWS Athena SQL Queries(Insights)]
|
▼
[CSV Insights]
---

## 📁 Project Structure

```plaintext
📁 bursa-market-share-analysis/
│
├── .env                          # Sensitive configs (Kafka IP, S3 bucket)
├── README.md                     # Full project overview
├── Issues_and_Fixes.txt          # Known issues + resolutions (Kafka, Glue, AWS)
├── kafka-stock-trading-project.pem  # EC2 access key
│
├── 📁 data-ingestion/
│   ├── kafka-producer/           # Sends scraped data to Kafka
│   │   └── kafka_producer.ipynb
│   ├── kafka-consumer/           # Reads from Kafka and writes to S3
│   │   └── kafka_consumer.ipynb
│   └── bursa-scraper/            # Custom scraper 
│       └── bursaMarketShare.py
│
├── 📁 etl/
│   ├── glue-scripts/             # PySpark Glue transformation scripts
│   ├── curated/                  # Silver layer output (cleaned)
│   └── transformed/              # Gold layer output (aggregated)
│
├── 📁 analytics/
│   ├── insights/                 # SQL output
│   └── sql/                      # Athena queries
│
├── 📁 infra-setup/
│   ├── aws-cli/                  # AWS CLI setup commands/scripts
│   ├── kafka-setup/              # Kafka 4.0.0 + KRaft mode instructions
│   └── docker-kafdrop/           # Kafdrop docker setup
│
└── 📁 docs/
    └── architecture_diagram.png  


---

## 📈 Athena-Powered Insights

Using **Athena**, we run SQL queries directly over **S3 gold-layer data** to extract:

- **Buy/Sell Pressure Score**
- **Top Gainers & Losers**
- **Average Spread Per Sector**
- **High-Volume Outliers**

These queries prove the transformation pipeline delivers analytics-ready datasets.

---

## 🛠️ Setup Instructions

### 1. Clone the repo

```bash
git clone https://github.com/your-username/bursa-market-share-analysis.git
cd bursa-market-share-analysis
```

### 2. Add `.env` file

```env
KAFKA_BROKER_IP=your-public-ip:9092
S3_BUCKET_NAME=bursa-raw
```

> ✅ Recommended: Store this `.env` file in the root directory and any working notebook folder.

---

## 🔄 Steps to Reproduce the Pipeline

### Step 1: Scrape & Produce to Kafka
- Run: `kafka-producer/Kafka Producer.ipynb`
- Reads CSV → pushes to `bursaMarketShare` topic

### Step 2: Consume from Kafka to S3 (Bronze)
- Run: `kafka-consumer/Kafka Consumer.ipynb`
- Collects Kafka records → saves to `s3://bursa-raw/`

### Step 3: Run Glue Job for Silver Layer
- Cleans raw fields like `%chg`, `cashtag`, removes unwanted columns
- Output to: `s3://bursa-curated/`

### Step 4: Run Glue Job for Gold Layer
- Adds derived fields like spread, buy/sell pressure, delta
- Output to: `s3://bursa-transformed/`

### Step 5: Query via Athena
- Point Athena to `bursa-transformed` bucket
- Use SQL from `insights/` folder

---

## ⚙️ Optional Tools

### Kafdrop UI

```bash
docker run -d -p 9000:9000 \
  -e KAFKA_BROKERCONNECT=<YOUR_PUBLIC_IP>:9092 \
  --name kafdrop \
  obsidiandynamics/kafdrop
```

View messages: [http://localhost:9000](http://localhost:9000)

---

## 🔥 Real-Time Ready

This project currently uses **Kafka as a batch buffer**, but with minimal changes (e.g., stream writes to S3 instead of batch), it can be fully real-time. All architecture choices support real-time scalability.

---

---

## 🙌 Honourable Mention

This project drew inspiration and guidance from the excellent work by [Darshil Parmar](https://github.com/darshilparmar):

- GitHub Repository: [Stock Market Kafka Data Engineering Project](https://github.com/darshilparmar/stock-market-kafka-data-engineering-project)


