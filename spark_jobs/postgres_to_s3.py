from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresRead") \
    .config(
        "spark.jars",
        "../jars/postgresql-42.7.11.jar"
    ) \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5433/sales_db"

properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="orders",
    properties=properties
)

df.show()

spark.stop()