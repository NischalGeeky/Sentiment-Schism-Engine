# ⚡ Reddit Sentiment Schism Engine

A real-time big-data pipeline that streams **Reddit posts**, classifies them into tech **ecosystems**, scores sentiment with **VADER** on **Apache Spark**, and hunts for **schisms** — divergences in emotional sentiment between enthusiast and critic communities.

> Data sourced from **Arctic Shift** (JSONL dumps).

---

## 🏗️ Architecture

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Arctic Shift │   │              │   │  Streamlit   │
│  JSONL dumps  │   │   Kafka      │   │  Dashboard   │
└──────┬───────┘   │  (broker)    │   └──────▲───────┘
       │            └──────┬───────┘          │
       ▼                   ▼                  │
┌──────────────────┐   ┌──────────────────────┘
│    ingestion/     │   │
│  artic_producer   │   │
│  → classify eco   │   │
└──────────┬───────┘   │
           │ publish   │
           ▼           │
               │ publish to topics │
               ▼                   │
       ┌──────────────┐            │
       │   Kafka       │           │
       │ reddit.*      │           │
       └──────┬───────┘            │
              │ subscribe          │
              ▼                    │
       ┌──────────────────┐        │
       │  Spark (Pyspark) │        │
       │  batch sentiment │        │
       │  + schism detect │        │
       └──────┬───────────┘        │
              │ write parquet      │
              ▼                    │
       ┌──────────────┐            │
       │  output/*.parquet  ───────┘
       └──────────────┘
```

### Ecosystem classification
Posts are tagged into one of three ecosystems based on community culture:

| Ecosystem | Subreddits |
|-----------|-----------|
| 🟠 **enthusiasts** | `apple`, `hardware`, `homelab`, `buildapc` |
| 🔵 **critics** | `technology`, `privacy`, `anticonsumption`, `android` |
| 🟢 **public** | everything else (`gadgets`, `Futurology`, …) |

A **schism** is flagged when the mean sentiment between two ecosystems diverges by `≥ 0.1`.

---

## 📁 Project Layout

```
├── ingestion/
│   ├── kafka_producer_base.py   # producer + ecosystem classification + message builder
│   └── artic_producer.py        # replay Arctic Shift JSONL dumps into Kafka
├── processing/
│   ├── batch_sentiment.py       # Spark job: batch sentiment scoring + schism detection
│   └── spark_stream.py          # (streaming variant placeholder)
├── dashboard/
│   └── app.py                   # Streamlit + Plotly visualization
├── dataset/                     # 🌐 Arctic Shift JSONL dumps  (gitignored)
├── output/                      # 📊 spark parquet results       (gitignored)
├── docker-compose.yml           # Kafka + Zookeeper
└── requirements.txt
```

---

## 🚀 Getting Started

### 1. Start Kafka + Zookeeper

```bash
docker compose up -d
```

### 2. Create a Python environment & install deps

```bash
python3 -m venv projenv
source projenv/bin/activate
pip install -r requirements.txt
```

### 3. Ingest data

Replay an Arctic Shift dump:

```bash
python ingestion/artic_producer.py dataset/r_technology_posts.jsonl
```

### 4. Run the Spark sentiment job

```bash
python processing/batch_sentiment.py
```

This reads everything back from Kafka (`startingOffsets=earliest`), scores each post's text with VADER, computes per-ecosystem averages, detects schisms, and writes parquet results to `output/`.

### 4. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

---

## 🧰 Tech Stack

- **Apache Kafka** (Confluent, Docker) — message bus
- **Apache Spark / PySpark** — distributed batch processing
- **VADER** — lexicon-based sentiment analysis
- **Arctic Shift** — Reddit data dumps
- **Streamlit + Plotly** — visualization

---

## 📝 Notes

- Dataset and generated parquet outputs are **gitignored** — only source code is versioned.
- The Spark job runs in batch mode (reads from `earliest` offset). For a true streaming run, use `spark_stream.py`.
- VADER is lexicon-based and fast; swap `sentiment_udf` for a transformer model if you need deeper nuance.
