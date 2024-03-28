all_columns_df = df.select(col("metadata.operation").alias("operation"), "data.*")
column_dict = {col: dtype for col, dtype in df.dtypes}
df1 = df1.withColumn(column_name, col(column_name).cast(target_data_type))

columns = ', '.join(data.keys())

values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])

sql_statement = f"INSERT INTO your_table_name ({columns}) VALUES ({values});"

print(sql_statement)


CREATE TABLE iceberg_table (
  APPLICATION_ID INT,
  SET_OF_BOOKS_ID INT,
  PERIOD_NAME STRING,
  LAST_UPDATE_DATE STRING,
  LAST_UPDATED_BY INT,
  CLOSING_STATUS STRING,
  START_DATE STRING,
  END_DATE STRING,
  YEAR_START_DATE STRING,
  QUARTER_NUM INT,
  QUARTER_START_DATE STRING,
  PERIOD_TYPE STRING,
  PERIOD_YEAR INT,
  EFFECTIVE_PERIOD_NUM INT,
  PERIOD_NUM INT,
  ADJUSTMENT_PERIOD_FLAG STRING,
  CREATION_DATE STRING,
  CREATED_BY INT,
  LAST_UPDATE_LOGIN INT,
  ELIMINATION_CONFIRMED_FLAG STRING,
  ATTRIBUTE1 STRING,
  ATTRIBUTEC STRING,
  ATTRIBUTE3 STRING,
  ATTRIBUTE4 STRING,
  ATTRIBUTE5 STRING,
  CONTEXT STRING,
  CHRONOLOGICAL_SEQ_STATUS_CODE STRING,
  TRACK_BC_YTD_FLAG STRING,
  MIGRATION_STATUS_CODE STRING,
  LEDGER_ID INT
)
PARTITIONED BY (category STRING, bucket INT)
STORED AS ICEBERG
LOCATION 's3://DOC-EXAMPLE-BUCKET/iceberg-folder'
TBLPROPERTIES (
  'write_compression'='snappy',
  'optimize_rewrite_delete_file_threshold'='10'
);
