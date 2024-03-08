schema = StructType([
    StructField("APPLICATION_ID", IntegerType(), True),
    StructField("SET_OF_BOOKS_ID", IntegerType(), True),
    StructField("PERIOD_NAME", StringType(), True),
    StructField("LAST_UPDATE_DATE", StringType(), True),
    StructField("LAST_UPDATED_BY", IntegerType(), True),
    StructField("CLOSING_STATUS", StringType(), True),
    StructField("START_DATE", StringType(), True),
    StructField("END_DATE", StringType(), True),
    StructField("YEAR_START_DATE", StringType(), True),
    StructField("QUARTER_NUM", IntegerType(), True),
    StructField("QUARTER_START_DATE", StringType(), True),
    StructField("PERIOD_TYPE", StringType(), True),
    StructField("PERIOD_YEAR", IntegerType(), True),
    StructField("EFFECTIVE_PERIOD_NUM", IntegerType(), True),
    StructField("PERIOD_NUM", IntegerType(), True),
    StructField("ADJUSTMENT_PERIOD_FLAG", StringType(), True),
    StructField("CREATION_DATE", StringType(), True),
    StructField("CREATED_BY", IntegerType(), True),
    StructField("LAST_UPDATE_LOGIN", IntegerType(), True),
    StructField("LEDGER_ID", IntegerType(), True),
    StructField("EFFECTIVE_PERIOD_NUM", IntegerType(), True),
    StructField("APPLICATION_ID", IntegerType(), True)
])

data = [
    (6, 2, 'March', '2024-03-31', 1006, 'Closed', '2024-03-01', '2024-03-31', '2024-03-01', 1, '2024-03-01', 'Standard', 2024, 3, 3, 'N', '2024-03-01', 1006, 1006, 2),
    (7, 3, 'January', '2024-01-31', 1007, 'Closed', '2024-01-01', '2024-01-31', '2024-01-01', 1, '2024-01-01', 'Standard', 2024, 1, 1, 'N', '2024-01-01', 1007, 1007, 3),
    (8, 3, 'February', '2024-02-29', 1008, 'Open', '2024-02-01', '2024-02-29', '2024-02-01', 1, '2024-02-01', 'Standard', 2024, 2, 2, 'N', '2024-02-01', 1008, 1008, 3),
    (9, 3, 'March', '2024-03-31', 1009, 'Closed', '2024-03-01', '2024-03-31', '2024-03-01', 1, '2024-03-01', 'Standard', 2024, 3, 3, 'N', '2024-03-01', 1009, 1009, 3),
    (10, 4, 'January', '2024-01-31', 1010, 'Closed', '2024-01-01', '2024-01-31', '2024-01-01', 1, '2024-01-01', 'Standard', 2024, 1, 1, 'N', '2024-01-01', 1010, 1010, 4)
]

df = spark.createDataFrame(data, schema)


####################################################


INSERT INTO your_database.your_table
VALUES
  (1, 1, 'January', '2024-01-31', 1001, 'Closed', '2024-01-01', '2024-01-31', '2024-01-01', 1, '2024-01-01', 'Standard', 2024, 1, 1, 'N', '2024-01-01', 1001, 1001, 1),
  (2, 1, 'February', '2024-02-29', 1002, 'Open', '2024-02-01', '2024-02-29', '2024-02-01', 1, '2024-02-01', 'Standard', 2024, 2, 2, 'N', '2024-02-01', 1002, 1002, 1),
  (3, 1, 'March', '2024-03-31', 1003, 'Closed', '2024-03-01', '2024-03-31', '2024-03-01', 1, '2024-03-01', 'Standard', 2024, 3, 3, 'N', '2024-03-01', 1003, 1003, 1),
  (4, 2, 'January', '2024-01-31', 1004, 'Closed', '2024-01-01', '2024-01-31', '2024-01-01', 1, '2024-01-01', 'Standard', 2024, 1, 1, 'N', '2024-01-01', 1004, 1004, 2),
  (5, 2, 'February', '2024-02-29', 1005, 'Open', '2024-02-01', '2024-02-29', '2024-02-01', 1, '2024-02-01', 'Standard', 2024, 2, 2, 'N', '2024-02-01', 1005, 1005, 2);



CREATE DATABASE datacoding;

CREATE TABLE datacoding.iceberg_table (
  id int,
  data string,
  category string)
LOCATION 's3://datacoding-iceberg-table/iceberg-folder' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet'
);

INSERT INTO datacoding.iceberg_table VALUES (1,'a','c1');


spark.conf.set("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.default.warehouse", "s3://datacoding-iceberg-table/iceberg-folder/")
spark.conf.set("spark.sql.catalog.default.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.default.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.default.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
spark.conf.set("spark.sql.catalog.default.lock.table", "datacoding_iceberg_lock_table")

df = spark.sql("select id, data, category from default.datacoding.iceberg_table")
logger.info(f"Number of rows in data frame {df.count()}")

job.commit()






############################### statement create #######################################
"""

CREATE EXTERNAL TABLE your_database.your_table (
  `APPLICATION_ID` int,
  `SET_OF_BOOKS_ID` int,
  `PERIOD_NAME` string,
  `LAST_UPDATE_DATE` string,
  `LAST_UPDATED_BY` int,
  `CLOSING_STATUS` string,
  `START_DATE` string,
  `END_DATE` string,
  `YEAR_START_DATE` string,
  `QUARTER_NUM` int,
  `QUARTER_START_DATE` string,
  `PERIOD_TYPE` string,
  `PERIOD_YEAR` int,
  `EFFECTIVE_PERIOD_NUM` int,
  `PERIOD_NUM` int,
  `ADJUSTMENT_PERIOD_FLAG` string,
  `CREATION_DATE` string,
  `CREATED_BY` int,
  `LAST_UPDATE_LOGIN` int,
  `LEDGER_ID` int)
PARTITIONED BY (`EFFECTIVE_PERIOD_NUM`, `APPLICATION_ID`)
LOCATION 's3://your_bucket/your_path/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'snappy',
  'optimize_rewrite_delete_file_threshold' = '10'
)

"""



############################### code #######################################



from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Iceberg Operations") \
    .getOrCreate()

# {
#     "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#     "--datalake-formats": "iceberg"
# }

catalog_name = "glue_catalog"
bucket_name = ""
bucket_prefix = ""
database_name = "iceberg_sql"
table_name = "product"
warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"

spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.catalog.{catalog_name}.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
    .config(f"spark.sql.catalog.{catalog_name}.lock.table", dynamodb_table) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()


data = [
    (1, 101, 'January 2024', '2024-01-31', 100, 'Closed', '2024-01-01', '2024-01-31', '2024-01-01', 1),
    (2, 101, 'February 2024', '2024-02-29', 101, 'Open', '2024-02-01', '2024-02-29', '2024-01-01', 1)
]

columns = [
    "application_id", "set_of_books_id", "period_name", "last_update_date",
    "last_updated_by", "closing_status", "start_date", "end_date", "year_start_date", "quarter_num"
]

df_insert = spark.createDataFrame(data, columns)


df_insert.createOrReplaceTempView("tmp_insert")

query_insert = f"""
INSERT INTO {catalog_name}.{database_name}.{table_name}
SELECT * FROM tmp_insert
"""
spark.sql(query_insert)

# Delete records
query_delete = f"""
DELETE FROM {catalog_name}.{database_name}.{table_name} WHERE application_id = '2'
"""
spark.sql(query_delete)

data_upsert = [
    (3, 101, 'March 2024', '2024-03-31', 102, 'Open', '2024-03-01', '2024-03-31', '2024-01-01', 1)
]

df_upsert = spark.createDataFrame(data_upsert, columns)
df_upsert.createOrReplaceTempView("tmp_upsert")

query_upsert = f"""
MERGE INTO {catalog_name}.{database_name}.{table_name} AS t
USING (SELECT * FROM tmp_upsert) AS u
ON t.application_id = u.application_id
WHEN MATCHED THEN UPDATE SET t.last_update_date = u.last_update_date
WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(query_upsert)

spark.sql(f"SELECT * FROM {catalog_name}.{database_name}.{table_name}").show()



########################################another code  ###################################


data = {
    "APPLICATION_ID": 101,
    "SET_OF_BOOKS_ID": 1,
    "PERIOD_NAME": "FEB-02",
    "LAST_UPDATE_DATE": "2002-04-08T14:08:292",
    "LAST_UPDATED_BY": 1050,
    "CLOSING_STATUS": "N",
    "START_DATE": "2002-02-01T00:00:00Z",
    "END_DATE": "2002-02-28T00:00:00Z",
    "YEAR_START_DATE": "2002-01-01T00:00:00Z",
    "QUARTER_NUM": 1,
    "QUARTER_START_DATE": "2002-01-01T00:00:00Z",
    "PERIOD_TYPE": "1",
    "PERIOD_YEAR": 2002,
    "EFFECTIVE_PERIOD_NUM": 20020002,
    "PERIOD_NUM": 2,
    "ADJUSTMENT_PERIOD_FLAG": "N",
    "CREATION_DATE": "2002-04-08T14:08:29Z",
    "CREATED_BY": 1050,
    "LAST_UPDATE_LOGIN": 458,
    "LEDGER_ID": 1
}

metadata = {
    "timestamp": "2024-02-03T08:18:39.5652922",
    "record-type": "data",
    "operation": "update",
    "partition-key-type": "schema-table",
    "schema-name": "GL",
    "table-name": "KINESIS_TEST"
}

operation = metadata.get("operation")
schema_name = metadata.get("schema-name")
table_name = metadata.get("table-name")
application_id = data.get("APPLICATION_ID")



if operation == "insert":
    row = Row(**data)
    df = spark.createDataFrame([row])
    # Write to Iceberg table
    df.writeTo(f"{catalog_name}.{schema_name}.{table_name}").append()

elif operation == "delete":
    df = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")
    new_df = df.filter(col("APPLICATION_ID") != application_id)
    new_df.write.format('iceberg').mode('overwrite').save(f"{catalog_name}.{schema_name}.{table_name}")

elif operation == "update":
    df = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")
    df_without_target = df.filter(col("APPLICATION_ID") != application_id)
    updated_data = Row(**data)
    df_updated = spark.createDataFrame([updated_data])
    new_df = df_without_target.union(df_updated)
    new_df.write.format('iceberg').mode('overwrite').save(f"{catalog_name}.{schema_name}.{table_name}")

elif operation == "upsert":
    df = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}")
    df_without_target = df.filter(col("APPLICATION_ID") != application_id)
    new_data = Row(**data)
    df_new = spark.createDataFrame([new_data])
    df_upsert = df_without_target.union(df_new)
    df_upsert.write.format('iceberg').mode('overwrite').save(f"{catalog_name}.{schema_name}.{table_name}")
