# Dish Second Course – Data Pipeline Challenge

This project demonstrates an end-to-end **data engineering workflow** — from **API data extraction** to **ETL processing** and **data loading into Google BigQuery (BQ)**.  
The pipeline is orchestrated using **Apache Airflow** and containerized with **Docker** for reproducibility and portability.

---

## 🧭 Project Structure

```
.
├── src/
│   ├── api-int-data-explore/   # API exploration and data inspection
│   ├── etl/                    # ETL modules, including Airflow DAGs
│   │   ├── data_pipeline.py    # Final ETL module for data movement
│   │   ├── etl_google_analytics_dag.py  # Airflow DAG definition
│   └── ...
├── Dockerfile                  # Docker setup for the data pipeline
└── README.md                   # Project documentation
```

---

## 1️⃣ API Exploration

The **API exploration** phase involves understanding and testing the **Google Analytics API** endpoints and parameters before designing the ETL process.  
Refer to the detailed documentation in:  
📄 `src/api-int-data-explore/README.md`

---

## 2️⃣ ETL Pipeline

The **ETL process** handles:
- **Extract**: Data pulled from the Google Analytics API.  
- **Transform**: Cleansing, structuring, and transforming the raw API data.  
- **Load**: Pushing the processed data into **BigQuery (BQ)** for analytics and visualization.

For detailed instructions, refer to:  
📄 `src/etl/README.md`

The main executable module is:  
`src/etl/data_pipeline.py`

---

## 3️⃣ Airflow Pipeline

The ETL workflow is orchestrated using **Apache Airflow**.  
The DAG file is located at:  
📄 `src/etl/etl_google_analytics_dag.py`

### 🚀 Steps to Run Airflow DAG

```bash
# Set environment variables
export AIRFLOW_HOME=$(pwd)
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gcp/service-account.json"

# Initialize Airflow database
airflow db init

# Create Airflow admin user
airflow users create     --username admin     --firstname Abhijit     --lastname Yadav     --role Admin     --email abhijitjan22@gmail.com

# Disable Airflow example DAGs
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Point Airflow to the DAGs folder
export AIRFLOW__CORE__DAGS_FOLDER=/home/adminabhi/gitrepo/dish-second-course/src/etl

# Start Airflow standalone mode
airflow standalone
```

After setup, access the **Airflow UI** (by default at http://localhost:8080) and trigger the DAG:  
`etl_google_analytics_dag`

---

## 4️⃣ Docker Setup

To run the entire pipeline inside a Docker container:

### 🐳 Build the Docker image

```bash
docker build -t data-pipeline:latest .
```

### ▶️ Run the Docker container

```bash
docker run --rm   -e GCP_PROJECT="dish-second-course"   -e GOOGLE_APPLICATION_CREDENTIALS="/app/infra/keys/service-account.json"   -e API_KEY="AIzaSyDMMWBOHgMG1u7P9jX9neaUQHY2vwlBTbM"   -v $(pwd)/infra/keys:/app/infra/keys   data-pipeline:latest   --start-date 2016-08-01   --end-date 2016-08-01   --limit 500
```

This will execute the ETL pipeline end-to-end within Docker, from data extraction to BigQuery load.

