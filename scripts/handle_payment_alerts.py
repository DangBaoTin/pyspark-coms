from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
from helper_functions import *

def create_spark_session():
    """Initializes and returns a Spark session connected to MinIO."""
    return SparkSession.builder \
        .appName("Payment Alert Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://coms-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_user") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_alerts(spark):
    print("--- Alert for Payments Delayed > 2 Days ---")
    
    # Load necessary tables
    orders_df = spark.read.parquet("s3a://processed/orders")
    payments_df = spark.read.parquet("s3a://processed/payments")
    customers_df = spark.read.parquet("s3a://processed/customers")
    
    # Identify payments made more than 2 days after the order
    delayed_payments = orders_df.join(payments_df, "order_id", "inner") \
        .withColumn("days_to_pay", datediff(col("payment_date"), col("order_date"))) \
        .filter(col("days_to_pay") > 2)
        
    # Join with customer data to get email addresses
    alerts_to_send_df = delayed_payments.select("order_id", "customer_id", "order_date", "payment_date", "days_to_pay")
    
    alerts_with_customer_info = alerts_to_send_df.join(customers_df, "customer_id", "inner").join(orders_df, "order_id", "inner")
        
    # .collect() is safe here because the number of alerts should be small
    alerts = alerts_with_customer_info.collect()
    
    if not alerts:
        print("No delayed payments found.")
        return
        
    print(f"Found {len(alerts)} delayed payments. Sending alerts...")
    
    for alert in alerts:
        print(f"Sending email to {alert['email']} for order {alert['order_id']}")
        send_delayed_payment_alert(
            customer_name=alert["full_name"],
            customer_email=alert["email"],
            order_id=alert["order_id"],
            order_date=str(alert["order_date"].date()),
            order_amount=alert["total_amount"]
        )
    print("Finished sending alerts.")

if __name__ == "__main__":
    spark = create_spark_session()
    process_alerts(spark)
    spark.stop()