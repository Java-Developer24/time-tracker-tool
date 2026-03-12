

CREATE SCHEMA IF NOT EXISTS `<PROJECT_ID>.<DATASET>`;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET>.activity_logs` (
  id STRING NOT NULL,
  agent_id STRING NOT NULL,
  activity_type STRING NOT NULL,
  sub_category STRING,
  url STRING,
  page_title STRING,
  additional_info STRING,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  duration_seconds INT64,
  manually_categorized BOOL,
  logged_at TIMESTAMP
)
PARTITION BY DATE(start_time)
CLUSTER BY agent_id, activity_type;

CREATE TABLE IF NOT EXISTS `<PROJECT_ID>.<DATASET>.agent_status` (
  agent_id STRING NOT NULL,
  current_status STRING,
  current_activity STRING,
  current_url STRING,
  status_updated_at TIMESTAMP,
  last_seen TIMESTAMP
);

CREATE OR REPLACE VIEW `<PROJECT_ID>.<DATASET>.daily_summary` AS
SELECT
  agent_id,
  DATE(start_time) AS shift_date,
  SUM(CASE WHEN activity_type = 'PRODUCTIVE' THEN duration_seconds ELSE 0 END) AS productive_seconds,
  SUM(
    CASE
      WHEN activity_type IN ('BREAK', 'ONE_ON_ONE', 'HUDDLE', 'MEETING', 'NON_PRODUCTIVE')
      THEN duration_seconds
      ELSE 0
    END
  ) AS non_productive_seconds,
  SUM(CASE WHEN activity_type = 'IDLE' THEN duration_seconds ELSE 0 END) AS idle_seconds,
  COUNT(1) AS record_count
FROM `<PROJECT_ID>.<DATASET>.activity_logs`
GROUP BY agent_id, DATE(start_time);
