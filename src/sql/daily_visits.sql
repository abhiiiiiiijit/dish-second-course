CREATE TABLE IF NOT EXISTS analytics.daily_visits (
  total_visits INT64,
  visit_date DATE,
  load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  loaded_by STRING DEFAULT SESSION_USER(),
  source_file STRING
)
PARTITION BY
  visit_date;