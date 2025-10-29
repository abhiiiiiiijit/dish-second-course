CREATE TABLE IF NOT EXISTS dev.daily_visits (
  total_visits INT64,
  visit_date DATE
)
PARTITION BY
  visit_date;