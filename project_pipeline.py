from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("TaxiLakehouseProject")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate())

base_path = "/path/to/project"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"

# These files have .xls names but contain CSV text, so read them as CSV in Spark.
drivers_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/drivers.xls")
trips_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/trips.xls")
trip_logs_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/trip_logs.xls")

drivers_df.write.mode("overwrite").parquet(f"{bronze_path}/drivers")
trips_df.write.mode("overwrite").parquet(f"{bronze_path}/trips")
trip_logs_df.write.mode("overwrite").parquet(f"{bronze_path}/trip_logs")

def normalize(df):
    for c in df.columns:
        new_c = ''.join(ch.lower() if ch.isalnum() else '_' for ch in c).strip('_')
        df = df.withColumnRenamed(c, new_c)
    return df

drivers = normalize(drivers_df)
trips = normalize(trips_df)
trip_logs = normalize(trip_logs_df)

trips = (trips
    .withColumn("distance", F.col("distance").cast("double"))
    .withColumn("fare", F.col("fare").cast("double"))
    .withColumn("start_time", F.to_timestamp("start_time"))
    .withColumn("end_time", F.to_timestamp("end_time")))
trip_logs = trip_logs.withColumn("event_time", F.to_timestamp("event_time"))

latest_trip_log = (trip_logs
    .withColumn("rn", F.row_number().over(Window.partitionBy("trip_id").orderBy(F.col("event_time").desc())))
    .filter("rn = 1")
    .drop("rn"))

silver = (trips.alias("t")
    .join(drivers.alias("d"), F.col("t.driver_id") == F.col("d.driver_id"), "left")
    .join(latest_trip_log.alias("l"), F.col("t.trip_id") == F.col("l.trip_id"), "left")
    .withColumn("trip_duration_minutes", (F.unix_timestamp("t.end_time") - F.unix_timestamp("t.start_time")) / 60.0)
    .withColumn("completion_flag", F.lower(F.col("t.status")).isin("completed", "complete", "done", "finished"))
    .filter(F.col("distance").isNotNull() & (F.col("distance") >= 0))
    .filter(F.col("t.start_time").isNotNull() & F.col("t.end_time").isNotNull())
    .filter(F.col("trip_duration_minutes") >= 0)
    .dropDuplicates(["trip_id"]))

silver.write.mode("overwrite").parquet(f"{silver_path}/cleaned_trips")

driver_perf = (silver.groupBy("driver_id").agg(
    F.count("trip_id").alias("total_trips"),
    F.sum(F.when(F.col("completion_flag"), 1).otherwise(0)).alias("completed_trips"),
    F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
    F.sum("fare").alias("total_revenue"),
    F.sum("distance").alias("total_distance"),
    F.avg(F.when(F.lower(F.col("status")).contains("cancel"), 1).otherwise(0)).alias("cancellation_rate")
))

rank_window = Window.orderBy(F.col("completed_trips").desc(), F.col("total_revenue").desc())
driver_ranked = driver_perf.withColumn("driver_rank", F.dense_rank().over(rank_window))

pickup_demand = (silver.groupBy("pickup_location").agg(F.count("trip_id").alias("trip_count"), F.sum("fare").alias("revenue")).orderBy(F.col("trip_count").desc()))
revenue_insights = silver.groupBy("driver_id").agg(F.sum("fare").alias("driver_revenue")).orderBy(F.col("driver_revenue").desc())

driver_ranked.write.mode("overwrite").parquet(f"{gold_path}/driver_performance")
pickup_demand.write.mode("overwrite").parquet(f"{gold_path}/pickup_demand")
revenue_insights.write.mode("overwrite").parquet(f"{gold_path}/revenue_insights")

bronze_trips = spark.read.parquet(f"{bronze_path}/trips")
silver_trips = spark.read.parquet(f"{silver_path}/cleaned_trips")
assert bronze_trips.count() >= silver_trips.count()
assert driver_ranked.filter(F.col("driver_id").isNull()).count() == 0

silver = silver.repartition("driver_id")
silver.cache()
spark.catalog.clearCache()