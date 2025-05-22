from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Konfigurasi package Kafka untuk Spark. Pastikan versi ini sesuai dengan versi Spark Anda.
kafka_jar_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" 

spark = SparkSession.builder \
    .appName("KafkaLogistikConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

suhu_schema = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

kelembaban_schema = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

df_suhu_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "LAPTOP-ACIN:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

df_suhu = df_suhu_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), suhu_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time_suhu", col("timestamp").cast(TimestampType()))

df_kelembaban_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "LAPTOP-ACIN:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

df_kelembaban = df_kelembaban_raw.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), kelembaban_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time_kelembaban", col("timestamp").cast(TimestampType()))

# --- TUGAS 3: FILTERING DAN PERINGATAN INDIVIDUAL ---
# (Checkpointing bisa ditambahkan di sini jika diperlukan untuk ketahanan)
# Misalnya: .option("checkpointLocation", "D:/spark_checkpoints/peringatan_suhu")

peringatan_suhu_df = df_suhu.filter(col("suhu") > 80) \
    .selectExpr(
        "'[Peringatan Suhu Tinggi]' as tipe_peringatan", 
        "concat('Gudang ', gudang_id, ': Suhu ', cast(suhu as string), '°C') as pesan"
    )

query_peringatan_suhu = peringatan_suhu_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "D:/spark_checkpoints/peringatan_suhu_checkpoint") \
    .start()

peringatan_kelembaban_df = df_kelembaban.filter(col("kelembaban") > 70) \
    .selectExpr(
        "'[Peringatan Kelembaban Tinggi]' as tipe_peringatan",
        "concat('Gudang ', gudang_id, ': Kelembaban ', cast(kelembaban as string), '%') as pesan"
    )

query_peringatan_kelembaban = peringatan_kelembaban_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "D:/spark_checkpoints/peringatan_kelembaban_checkpoint") \
    .start()

# --- TUGAS 4: GABUNGKAN STREAM DAN PERINGATAN GABUNGAN ---

# Perlebar watermark dan window join untuk observasi
# Anda bisa menyesuaikan nilai "15 seconds" ini.
WATERMARK_DURATION = "15 seconds"
JOIN_WINDOW_DURATION_SECONDS = 15 # dalam detik untuk ekspresi interval

df_suhu_watermarked = df_suhu.withWatermark("event_time_suhu", WATERMARK_DURATION)
df_kelembaban_watermarked = df_kelembaban.withWatermark("event_time_kelembaban", WATERMARK_DURATION)

joined_df = df_suhu_watermarked.alias("s") \
    .join(
        df_kelembaban_watermarked.alias("k"),
        expr(f"""
            s.gudang_id = k.gudang_id AND 
            s.event_time_suhu >= k.event_time_kelembaban - interval {JOIN_WINDOW_DURATION_SECONDS} seconds AND
            s.event_time_suhu <= k.event_time_kelembaban + interval {JOIN_WINDOW_DURATION_SECONDS} seconds
        """),
        "inner"
    ) \
    .select(
        col("s.gudang_id").alias("gudang_id_joined"),
        col("s.suhu").alias("suhu"),
        col("k.kelembaban").alias("kelembaban"),
        col("s.event_time_suhu").alias("ts_suhu"),
        col("k.event_time_kelembaban").alias("ts_kelembaban")
    )

status_df = joined_df.withColumnRenamed("gudang_id_joined", "gudang_id") \
    .withColumn(
        "status",
        when((col("suhu") > 80) & (col("kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
        .when(col("suhu") > 80, "Suhu tinggi, kelembaban normal")
        .when(col("kelembaban") > 70, "Kelembaban tinggi, suhu aman")
        .otherwise("Aman")
    )

output_gabungan_df = status_df.select(
    col("gudang_id"),
    col("suhu"),
    col("kelembaban"),
    col("status"),
    col("ts_suhu"),
    col("ts_kelembaban")
)

query_gabungan = output_gabungan_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 30) \
    .option("checkpointLocation", "D:/spark_checkpoints/gabungan_checkpoint") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Menghentikan stream karena interupsi keyboard...")
except Exception as e:
    print(f"Terjadi error pada stream: {e}")
finally:
    print("Menghentikan semua query stream...")
    if query_peringatan_suhu.isActive:
        query_peringatan_suhu.stop()
    if query_peringatan_kelembaban.isActive:
        query_peringatan_kelembaban.stop()
    if query_gabungan.isActive:
        query_gabungan.stop()
    
    print("Menghentikan Spark session...")
    spark.stop()
    print("Aplikasi Spark selesai.")