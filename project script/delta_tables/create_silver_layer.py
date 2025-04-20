from pyspark.sql import SparkSession

def create_silver_tables(
        spark,
        path="s3a://stocklake/delta",
        database: str = 'stocklake'
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {database}.daily_trades")
    spark.sql(
        f"""
            CREATE TABLE {database}.daily_trades (  
            trade_date DATE,
            symbol STRING,
            daily_volume LONG,
            daily_high DOUBLE,
            daily_low DOUBLE,
            daily_open DOUBLE,
            daily_close DOUBLE
            ) USING DELTA
            PARTITIONED BY (trade_date)
            LOCATION '{path}/daily_trades'
        """
    )

def drop_silver_tables(
    spark,
    database: str = "stocklake",
):
    spark.sql(f"DROP TABLE IF EXISTS {database}.daily_trades")


if __name__ == '__main__':
    spark = SparkSession.builder \
    .appName("silver_ddl") \
    .master('local[*]') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "stocklake") \
    .config("spark.hadoop.fs.s3a.secret.key", "stocklake") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .enableHiveSupport() \
    .getOrCreate()
    
    drop_silver_tables(spark)
    create_silver_tables(spark)
