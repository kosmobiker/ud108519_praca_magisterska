# sgh-diploma
this is a my diploma repo

1. Fetching historical data pipeline

> read_config >> get_hist_data >> save_to_S3 >> validate_data >>
send_metrics >> transformation >> save_to_parquet

2. Fetching current data pipeline

> read_config >> get_daily_data >> save_to_S3 >> validate_data >>
send_metrics >> transformation >> save_to_parquet

3. Near Real-time data fetching (one per minute)???

> read_config >> transform_csv >> visualize






