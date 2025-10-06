import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from helper_functions import *

# =============================================================================
# SPARK SESSION & CONFIG
# =============================================================================

def create_spark_session():
    """Initializes and returns a Spark session connected to MinIO."""
    return SparkSession.builder \
        .appName("Raw to Processed Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://coms-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_user") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

raw_base_path = "s3a://raw"
processed_base_path = "s3a://processed"

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("phone", StringType(), True),
    StructField("region", StringType(), True)
])

employees_schema = StructType([
    StructField("employee_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("role", StringType(), True),
    StructField("team", StringType(), True),
    StructField("contact_number", StringType(), True),
    StructField("start_date", DateType(), True)
])

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("currency", StringType(), True)
])

order_items_schema = StructType([
    StructField("order_item_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price_per_unit", DoubleType(), True),
    StructField("discount", DoubleType(), True)
])

payments_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("payment_date", DateType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True)
])

# =============================================================================
# MAIN PROCESSING LOGIC
# =============================================================================

def read_table(spark_session, table_name, schema, primary_key, date_columns=[]):
    """
    - Read and normalize CSVs into structured DataFrames.
    - Convert all dates into consistent timestamp format.
    - Deduplicate based on primary keys (e.g., order_id, order_item_id).
    """
    try:
        print(f"Processing table: {table_name}...")
        
        # Read from raw zone
        input_path = f"{raw_base_path}/{table_name}.csv"
        df = spark_session.read.csv(input_path, header=True, schema=schema)
        
        # Convert date columns to timestamp format
        for date_col in date_columns:
            df = df.withColumn(date_col, to_timestamp(col(date_col)))
        
        # Deduplicate based on primary key
        df = df.dropDuplicates([primary_key])

        return df
    
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        send_etl_error_alert(tech_email=recipient_list, pipeline_stage="Read Raw to Processed", table_name=table_name, error_message=e)
        return None


def write_table(table_name, df):
    try:
        print(f"Writing table: {table_name}...")
        
        # Write to processed zone in CSV format with headers
        output_path = f"{processed_base_path}/{table_name}"
        df.write.mode("overwrite").parquet(output_path)
        # df.write.mode("overwrite").option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(output_path)
        
        print(f"Successfully processed and saved '{table_name}' as CSV to '{output_path}'.")
        
        # For verification, show a few rows
        df.printSchema()
        df.show(5, truncate=False)
        
    except Exception as e:
        print(f"Error writing table {table_name}: {e}")
        send_etl_error_alert(tech_email=recipient_list, pipeline_stage="Write Raw to Processed", table_name=table_name, error_message=e)


def process_table(spark, table_name):
    """Reads, transforms, and writes a single table from raw to processed."""
    print(f"--- Starting processing for table: {table_name} ---")
    output_path = f"{processed_base_path}/{table_name}"
    
    # Read, transform, and write based on table name
    if table_name == "customers_csv":
        # df = spark.read.csv(input_path, header=True, schema=customers_schema)
        # processed_df = df.withColumn("signup_date", to_timestamp(col("signup_date"))).dropDuplicates(["customer_id"])
        processed_customers_df = read_table(spark, table_name="customers_csv", 
                                    schema=customers_schema, 
                                    primary_key="customer_id", 
                                    date_columns=["signup_date"])
        if processed_customers_df:
            write_table("customers_csv", processed_customers_df)

    elif table_name == "employees_csv":
        processed_employees_df = read_table(table_name="employees_csv", 
                                    schema=employees_schema, 
                                    primary_key="employee_id", 
                                    date_columns=["start_date"])
        if processed_employees_df:
            write_table("employees_csv", processed_employees_df)
    
    elif table_name == "orders_csv":
        processed_orders_df = read_table(table_name="orders_csv",
                                 schema=orders_schema, 
                                 primary_key="order_id", 
                                 date_columns=["order_date"])
        if processed_orders_df:
            write_df = processed_orders_df.filter("total_amount > 0")
            write_table("orders_csv", write_df)

    elif table_name == "order_items_csv":
        processed_order_items_df = read_table(table_name="order_items_csv",
                                      schema=order_items_schema,
                                      primary_key="order_item_id")
        if processed_order_items_df:
            write_table("order_items_csv", processed_order_items_df)
    
    elif table_name == "payments_csv":
        # df = spark.read.csv(input_path, header=True, schema=payments_schema)
        # processed_df = df.withColumn("payment_date", to_timestamp(col("payment_date"))) \
        #                  .dropDuplicates(["payment_id"]) \
        #                  .filter(~col("payment_status").isin(["failed", "cancelled"]))
        processed_payments_df = read_table(table_name="payments_csv", 
                                   schema=payments_schema, 
                                   primary_key="payment_id", 
                                   date_columns=["payment_date"])
        if processed_payments_df:
            write_df = processed_payments_df.filter(~processed_payments_df.payment_status.isin(["failed", "cancelled"]))
            write_table("payments_csv", write_df)
    
    else:
        raise ValueError(f"Unknown table name: {table_name}")

    print(f"--- Successfully wrote processed data for {table_name} to {output_path} ---")
    send_etl_success_notification(tech_email=recipient_list, pipeline_stage=f"Raw to Processed: {table_name}", output_path=processed_base_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Raw to Processed ETL Job")
    parser.add_argument("--table", required=True, help="The name of the table to process (e.g., customers, orders).")
    args = parser.parse_args()
    
    spark_session = create_spark_session()
    
    raw_employees_df = spark_session.read.csv(f"{raw_base_path}/employees_csv.csv", header=True)
    data_engineers_df = raw_employees_df.filter(
        (col("team") == "Tech Team") & (col("role") == "Data Engineer")
    )
    TECH_TEAM_EMAILs = [row['email'] for row in data_engineers_df.select("email").collect()]
    recipient_list = ", ".join(TECH_TEAM_EMAILs)
    
    process_table(spark_session, args.table)
    spark_session.stop()