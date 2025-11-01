# dish-second-course
This is a comprehensive 5-step technical assessment designed to evaluate your skills in data engineering, API integration, workflow orchestration, and containerization. The task follows a progressive difficulty approach, starting with simple data extraction and building up to advanced pipeline automation.


mkdir -p ~/.gcp
mv service-account.json ~/.gcp/
chmod 600 ~/.gcp/service-account.json


export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gcp/service-account.json"

source ~/.bashrc

poetry run env | grep GOOGLE_APPLICATION_CREDENTIALS


python3 src/etl/load_json_to_bigquery.py   --path "/data/daily_visits/"   --project "dish-second-course"   --table "analytics.daily_visits"

python3 src/etl/bq_data_load.py   --path "data/daily_visits/"   --project "dish-second-course"   --table "dish-second-course.analytics.daily_visits"

python3 src/etl/bq_data_load.py   --path "data/ga_sessions/"   --project "dish-second-course"   --table "dish-second-course.analytics.ga_sessions"


--------------------------------------------------------------------------------
### Airflow

poetry add "apache-airflow==2.10.2" "apache-airflow-providers-google==10.17.0"

export AIRFLOW_HOME=$(pwd)

airflow db init

airflow users create \
    --username admin \
    --firstname Abhijit \
    --lastname Yadav \
    --role Admin \
    --email abhijitjan22@gmail.com

export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db reset

airflow standalone

------------------
Docker

docker build -t data-pipeline:latest .

docker run --rm \
  -e GCP_PROJECT="dish-second-course" \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/infra/keys/service-account.json" \
  -e API_KEY="AIzaSyDMMWBOHgMG1u7P9jX9neaUQHY2vwlBTbM" \
  -v $(pwd)/infra/keys:/app/infra/keys \
  data-pipeline:latest --start-date 2016-08-01     --end-date 2016-08-01






