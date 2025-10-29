# dish-second-course
This is a comprehensive 5-step technical assessment designed to evaluate your skills in data engineering, API integration, workflow orchestration, and containerization. The task follows a progressive difficulty approach, starting with simple data extraction and building up to advanced pipeline automation.


mkdir -p ~/.gcp
mv service-account.json ~/.gcp/
chmod 600 ~/.gcp/service-account.json


export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gcp/service-account.json"

source ~/.bashrc

poetry run env | grep GOOGLE_APPLICATION_CREDENTIALS
