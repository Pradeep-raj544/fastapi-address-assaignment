from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergTableUpdate") \
    .config("spark.sql.catalog.ic", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ic.type", "hadoop") \
    .config("spark.sql.catalog.ic.warehouse", "/path/to/warehouse") \
    .getOrCreate()

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


if operation == "delete":
    iceberg_table = spark.read.format("iceberg").load(f"{schema_name}.{table_name}")
    iceberg_table = iceberg_table.filter(iceberg_table.APPLICATION_ID != application_id)
    iceberg_table.write.format("iceberg").mode("overwrite").save(f"{schema_name}.{table_name}")
    print(f"Record with APPLICATION_ID {application_id} deleted from {schema_name}.{table_name}")

if operation == "insert":
    iceberg_table = spark.read.format("iceberg").load(f"{schema_name}.{table_name}")

    row = Row(**data)
    df = spark.createDataFrame([row])
    df.write.format("iceberg").mode("append").save(f"{schema_name}.{table_name}")
    print("Record inserted into", f"{schema_name}.{table_name}")

if operation == "update":
    iceberg_table = spark.read.format("iceberg").load(f"{schema_name}.{table_name}")

    filtered_table = iceberg_table.filter(iceberg_table.APPLICATION_ID == application_id)

    filtered_table.write.format("iceberg").mode("delete").save(f"{schema_name}.{table_name}")

    df = spark.createDataFrame([data])

    df.write.format("iceberg").mode("append").save(f"{schema_name}.{table_name}")

    print(f"Record with APPLICATION_ID {application_id} updated in {schema_name}.{table_name}")
spark.stop()
