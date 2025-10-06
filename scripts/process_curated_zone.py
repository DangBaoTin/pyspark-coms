import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, max, date_sub, current_date, when, lit, rank, countDistinct
from pyspark.sql.window import Window
from helper_functions import *
# (Import any other functions you need, like for daily_sales_aggregates)

def create_spark_session():
    """Initializes and returns a Spark session connected to MinIO."""
    return SparkSession.builder \
        .appName("Processed to Curated Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://coms-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_user") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_curated_table(spark, table_name):
    """
    Loads processed data and generates a single curated table.
    """
    print(f"--- Starting processing for curated table: {table_name} ---")
    
    processed_base_path = "s3a://processed"
    curated_base_path = "s3a://curated"
    
    # Load all necessary processed tables
    customers_df = spark.read.parquet(f"{processed_base_path}/customers")
    orders_df = spark.read.parquet(f"{processed_base_path}/orders")
    order_items_df = spark.read.parquet(f"{processed_base_path}/order_items")
    payments_df = spark.read.parquet(f"{processed_base_path}/payments")

    if table_name == "customer_orders_summary":
        try:
            customer_orders_summary = orders_df.groupBy("customer_id").agg(
                count("order_id").alias("total_orders"),
                sum("total_amount").alias("total_amount_spent"),
                avg("total_amount").alias("average_order_value"),
                min("order_date").alias("first_order_date"),
                max("order_date").alias("last_order_date")
            ).withColumn(
                "active_status",
                when(col("last_order_date") >= date_sub(current_date(), 90), lit("active"))
                .otherwise(lit("inactive"))
            )
            
            # Write to the curated zone
            customer_orders_summary.write.mode("overwrite").parquet(f"{curated_base_path}/customer_orders_summary")
            
            print("Created and saved customer_orders_summary.")
            customer_orders_summary.show(5, truncate=False)

        except Exception as e:
            print(f"Error processing table customer_orders_summary: {e}")
            send_etl_error_alert(tech_email=recipient_list, pipeline_stage="Processed to Curated", table_name="customer_orders_summary", error_message=e)
    
    elif table_name == "order_facts":
        try:
            # Join orders, items, and customer info
            order_facts = order_items_df.join(
                orders_df,
                order_items_df.order_id == orders_df.order_id,
                "inner"
            ).join(
                customers_df,
                orders_df.customer_id == customers_df.customer_id,
                "inner"
            ).withColumn(
                "net_revenue",
                (col("quantity") * col("price_per_unit")) - col("discount")
            ).select(
                orders_df["order_date"],
                orders_df["order_id"],
                order_items_df["order_item_id"],
                customers_df["customer_id"],
                customers_df["full_name"].alias("customer_name"),
                customers_df["region"],
                order_items_df["product_id"],
                order_items_df["product_name"],
                order_items_df["category"],
                order_items_df["quantity"],
                order_items_df["price_per_unit"],
                order_items_df["discount"],
                "net_revenue",
                orders_df["channel"]
            )
            
            # Write to the curated zone, partitioned by order_date
            order_facts.write.mode("overwrite").partitionBy("order_date").parquet(f"{curated_base_path}/order_facts")
            
            print("Created and saved order_facts.")
            order_facts.show(5, truncate=False)

        except Exception as e:
            print(f"Error processing table order_facts: {e}")
            send_etl_error_alert(tech_email=recipient_list, pipeline_stage="Processed to Curated", table_name="order_facts", error_message=e)

    elif table_name == "daily_sales_aggregates":
        try:
            # Join the tables
            orders_payments_customers_df = orders_df.join(customers_df, "customer_id", "inner") \
                                                    .join(payments_df, "order_id", "inner")
            
            # rank payment methods within each group
            window_spec = Window.partitionBy("order_date", "region", "channel").orderBy(col("payment_method_count").desc())
            
            # count each payment method per group and rank them
            most_used_payment_method_df = orders_payments_customers_df.groupBy("order_date", "region", "channel", "payment_method") \
                .count().withColumnRenamed("count", "payment_method_count") \
                .withColumn("rank", rank().over(window_spec)) \
                .filter(col("rank") == 1) \
                .select(
                    col("order_date").alias("mu_order_date"),
                    col("region").alias("mu_region"),
                    col("channel").alias("mu_channel"),
                    col("payment_method").alias("most_used_payment_method")
                )
            
            daily_aggregates_df = orders_payments_customers_df.groupBy("order_date", "region", "channel").agg(
                sum("total_amount").alias("total_sales"),
                countDistinct("order_id").alias("order_count"),
                countDistinct("customer_id").alias("unique_customers")
            )
            
            # join the aggregates with the most used payment method
            daily_sales_aggregates = daily_aggregates_df.join(
                most_used_payment_method_df,
                (daily_aggregates_df.order_date == most_used_payment_method_df.mu_order_date) &
                (daily_aggregates_df.region == most_used_payment_method_df.mu_region) &
                (daily_aggregates_df.channel == most_used_payment_method_df.mu_channel),
                "inner"
            ).select(
                "order_date",
                "region",
                "channel",
                "total_sales",
                "order_count",
                "unique_customers",
                "most_used_payment_method"
            )
            
            # write to the curated zone
            daily_sales_aggregates.write.mode("overwrite").partitionBy("order_date").parquet(f"{curated_base_path}/daily_sales_aggregates")
            
            print("Created and saved daily_sales_aggregates.")
            daily_sales_aggregates.show(10, truncate=False)

        except Exception as e:
            print(f"Error processing table daily_sales_aggregates: {e}")
            send_etl_error_alert(tech_email=recipient_list, pipeline_stage="Processed to Curated", table_name="daily_sales_aggregates", error_message=e)
        
    else:
        raise ValueError(f"Unknown curated table name: {table_name}")
        
    print(f"--- Successfully wrote curated table {table_name} ---")
    send_etl_success_notification(tech_email=recipient_list, pipeline_stage=f"Processed to Curated: {table_name}", output_path=curated_base_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Processed to Curated ETL Job")
    parser.add_argument("--table", required=True, help="The name of the curated table to generate.")
    args = parser.parse_args()
    
    spark_session = create_spark_session()

    raw_employees_df = spark_session.read.csv("s3a://raw/employees_csv.csv", header=True)
    data_engineers_df = raw_employees_df.filter(
        (col("team") == "Tech Team") & (col("role") == "Data Engineer")
    )
    TECH_TEAM_EMAILs = [row['email'] for row in data_engineers_df.select("email").collect()]
    recipient_list = ", ".join(TECH_TEAM_EMAILs)
    
    process_curated_table(spark_session, args.table)
    spark_session.stop()