from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

spark = SparkSession.builder \
    .appName("PostgresToMinIO") \
    .getOrCreate()

# MinIO configs
hadoop_conf = spark._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.access.key", "admin")
hadoop_conf.set("fs.s3a.secret.key", "password123")
hadoop_conf.set("fs.s3a.endpoint", "http://host.docker.internal:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

jdbc_url = "jdbc:postgresql://host.docker.internal:5433/sales_db"

properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Read from Postgres
df = spark.read.jdbc(
    url=jdbc_url,
    table="orders",
    properties=properties
)

print("Original Data")
df.show()

# Transformation
transformed_df = df.withColumn(
    "customer_name",
    upper(df.customer_name)
)

print("Transformed Data")
transformed_df.show()

# Write to MinIO
transformed_df.write.mode("overwrite").parquet(
    "s3a://sales-data/orders_parquet"
)

spark.stop()