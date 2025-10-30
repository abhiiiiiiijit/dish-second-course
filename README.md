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