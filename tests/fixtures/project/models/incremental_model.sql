MODEL (
  name sqlmesh_example.incremental_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds
  ),
  start '2020-01-01',
  cron '@daily',
  grain [id, ds]
);

SELECT
  id AS id,
  item_id AS item_id,
  ds AS ds
FROM sqlmesh_example.seed_model
WHERE
  ds BETWEEN @start_ds AND @end_ds