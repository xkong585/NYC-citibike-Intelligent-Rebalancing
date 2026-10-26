# Historical Databricks export; see scripts/reproduce.py for the supported local workflow.



# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Citibike数据处理Pipeline
# MAGIC 
# MAGIC 这个笔记本包含了完整的数据处理流程，适用于Databricks社区版
# MAGIC - 数据持久化到DBFS
# MAGIC - 使用Delta Lake格式
# MAGIC - 包含初始化脚本，可在cluster重启后快速恢复环境

# MAGIC ## 2. 工具函数定义

# COMMAND ----------

# 检查DBFS路径是否存在
def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False

# 创建必要的目录
def create_directories():
    paths = [
        "/FileStore/citibike/raw",
        "/FileStore/citibike/processed",
        "/FileStore/citibike/features",
        "/FileStore/citibike/checkpoints"
    ]
    
    for path in paths:
        if not path_exists(path):
            dbutils.fs.mkdirs(path)
            print(f"Created directory: {path}")
        else:
            print(f"Directory already exists: {path}")

create_directories()

# MAGIC %md
# MAGIC ## 1. 环境初始化

# COMMAND ----------
# 导入必要的库
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functools import reduce
import pandas as pd

# 检查并创建数据库
spark.sql("CREATE DATABASE IF NOT EXISTS citibike_ny")
spark.sql("USE citibike_ny")

# MAGIC %md
# MAGIC ## 3. 数据持久化函数

# COMMAND ----------

# 将原始数据保存到DBFS（仅在首次运行时需要）
def save_raw_data_to_dbfs():
    for month in range(1, 13):
        table_name = f"citibike_{month}"
        
        # 检查表是否存在
        if spark.catalog.tableExists(f"citibike_ny.{table_name}"):
            df = spark.table(f"citibike_ny.{table_name}")
            
            # 保存到DBFS
            path = f"/FileStore/citibike/raw/{table_name}"
            df.write.format("delta").mode("overwrite").save(path)
            print(f"Saved {table_name} to {path}")
        else:
            print(f"Table {table_name} not found")

# 仅在需要时运行
save_raw_data_to_dbfs()

# MAGIC %md
# MAGIC ## 4. 环境重建函数（cluster重启后使用）

# COMMAND ----------

def rebuild_environment():
    """重建所有表和环境"""
    
    # 创建数据库
    spark.sql("CREATE DATABASE IF NOT EXISTS citibike_ny")
    spark.sql("USE citibike_ny")
    
    # 重建原始数据表
    for month in range(1, 13):
        table_name = f"citibike_{month}"
        path = f"/FileStore/citibike/raw/{table_name}"
        
        if path_exists(path):
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS citibike_ny.{table_name}
                USING DELTA
                LOCATION '{path}'
            """)
            print(f"Rebuilt table: {table_name}")
    
    # 重建处理后的特征表
    processed_path = "/FileStore/citibike/processed/features"
    if path_exists(processed_path):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS citibike_ny.processed_features
            USING DELTA
            LOCATION '{processed_path}'
        """)
        print("Rebuilt processed features table")

# 如果是重启后的第一次运行，执行重建
# rebuild_environment()

# MAGIC %md
# MAGIC ## 5. 数据处理函数

# COMMAND ----------

def load_all_months():
    """加载所有月份的数据"""
    dfs = []
    
    for month in range(1, 13):
        table_name = f"citibike_{month}"
        if spark.catalog.tableExists(f"citibike_ny.{table_name}"):
            df = spark.table(f"citibike_ny.{table_name}")
            dfs.append(df)
            print(f"Loaded {table_name}: {df.count()} records")
        else:
            print(f"Table {table_name} not found")
    
    if dfs:
        all_data = reduce(DataFrame.unionAll, dfs)
        print(f"Total records: {all_data.count()}")
        return all_data
    else:
        raise Exception("No data loaded")

def clean_citibike_data(df):
    """数据清理函数"""
    
    # 时间转换
    cleaned_df = df \
        .withColumn("started_at", to_timestamp("started_at")) \
        .withColumn("ended_at", to_timestamp("ended_at"))
    
    # 计算行程持续时间
    cleaned_df = cleaned_df \
        .withColumn("trip_duration_minutes", 
                   (unix_timestamp("ended_at") - unix_timestamp("started_at")) / 60)
    
    # 过滤异常值
    cleaned_df = cleaned_df \
        .filter(col("trip_duration_minutes").between(1, 1440)) \
        .filter(col("start_station_id").isNotNull()) \
        .filter(col("end_station_id").isNotNull()) \
        .filter(col("start_lat").isNotNull()) \
        .filter(col("start_lng").isNotNull()) \
        .filter(col("start_lat").between(40.4, 41.0)) \
        .filter(col("start_lng").between(-74.3, -73.7))
    
    return cleaned_df

def extract_time_features(df):
    """提取时间特征"""
    
    return df \
        .withColumn("start_hour", hour("started_at")) \
        .withColumn("start_day", dayofweek("started_at")) \
        .withColumn("start_month", month("started_at")) \
        .withColumn("start_date", to_date("started_at")) \
        .withColumn("is_weekend", 
                   when(col("start_day").isin([1, 7]), 1).otherwise(0)) \
        .withColumn("time_of_day", 
                   when(col("start_hour").between(6, 9), "morning_rush")
                   .when(col("start_hour").between(10, 16), "midday")
                   .when(col("start_hour").between(17, 19), "evening_rush")
                   .when(col("start_hour").between(20, 23), "evening")
                   .otherwise("night"))

def create_hourly_station_stats(df):
    """创建小时级别的站点统计"""
    
    # 出发统计
    departures = df.groupBy(
        "start_station_id",
        "start_station_name",
        date_trunc("hour", "started_at").alias("hour_timestamp")
    ).agg(
        count("*").alias("departures"),
        countDistinct("rideable_type").alias("bike_types_used"),
        sum(when(col("member_casual") == "member", 1).otherwise(0)).alias("member_departures"),
        sum(when(col("member_casual") == "casual", 1).otherwise(0)).alias("casual_departures"),
        avg("start_lat").alias("station_lat"),
        avg("start_lng").alias("station_lng")
    )
    
    # 到达统计
    arrivals = df.groupBy(
    col("end_station_id").alias("station_id"),
    date_trunc("hour", "ended_at").alias("arrival_hour_timestamp")   
    ).agg(
    count("*").alias("arrivals")
    )
    
    station_hourly = departures.join(
    arrivals,
    (departures.start_station_id == arrivals.station_id) &
    (departures.hour_timestamp == arrivals.arrival_hour_timestamp),  
    "left"
    ).fillna(0, subset=["arrivals"])

    
    # 计算净流量
    station_hourly = station_hourly \
        .withColumn("net_flow", col("arrivals") - col("departures")) \
        .withColumn("hour", hour("hour_timestamp")) \
        .withColumn("date", to_date("hour_timestamp"))
    
    return station_hourly

# COMMAND ----------

# 导入必要的库
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functools import reduce
import pandas as pd

# COMMAND ----------

def create_advanced_features(df):
    """创建高级时间序列特征"""
    
    # 定义窗口
    window_by_station = Window.partitionBy("start_station_id").orderBy("hour_timestamp")
    
    # 滞后特征
    for lag_value in [1, 2, 3, 24, 168]:  # 1h, 2h, 3h, 1d, 1w
        df = df.withColumn(f"departures_lag_{lag_value}", 
                          F.lag("departures", lag_value).over(window_by_station))
        df = df.withColumn(f"arrivals_lag_{lag_value}", 
                          F.lag("arrivals", lag_value).over(window_by_station))
        df = df.withColumn(f"net_flow_lag_{lag_value}", 
                          F.lag("net_flow", lag_value).over(window_by_station))
    
    # 滚动窗口特征
    for window_size in [3, 6, 12, 24]:
        rolling_window = Window.partitionBy("start_station_id") \
            .orderBy("hour_timestamp") \
            .rowsBetween(-window_size, -1)
        
        df = df.withColumn(f"departures_rolling_mean_{window_size}", 
                          F.avg("departures").over(rolling_window))
        df = df.withColumn(f"departures_rolling_std_{window_size}", 
                          F.stddev("departures").over(rolling_window))
        df = df.withColumn(f"arrivals_rolling_mean_{window_size}", 
                          F.avg("arrivals").over(rolling_window))
    
    return df

def process_all_data():
    """完整的数据处理流程"""
    
    print("Step 1: Loading data...")
    raw_df = load_all_months()
    
    print("Step 2: Cleaning data...")
    cleaned_df = clean_citibike_data(raw_df)
    
    print("Step 3: Extracting time features...")
    time_featured_df = extract_time_features(cleaned_df)
    
    print("Step 4: Creating hourly station stats...")
    hourly_stats = create_hourly_station_stats(time_featured_df)
    
    print("Step 5: Creating advanced features...")
    final_features = create_advanced_features(hourly_stats)
    
    # 添加月份列用于分区
    final_features = final_features.withColumn("month", F.month("hour_timestamp"))
    
    # 保存处理后的数据
    print("Step 6: Saving processed features...")
    final_features.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("month") \
        .save("/FileStore/citibike/processed/features")
    
    # 创建表
    spark.sql("""
        CREATE TABLE IF NOT EXISTS citibike_ny.processed_features
        USING DELTA
        LOCATION '/FileStore/citibike/processed/features'
    """)
    
    print("Processing complete!")
    return final_features


# MAGIC %md
# MAGIC ## 7. 执行处理（仅在需要时运行）

# 执行处理流程
processed_df = process_all_data()

# MAGIC %md
# MAGIC ## 8. 数据验证和检查

# COMMAND ----------

def validate_processed_data():
    """验证处理后的数据"""
    
    # 加载处理后的数据
    features_df = spark.table("citibike_ny.processed_features")
    
    # 基本统计
    print(f"Total records: {features_df.count()}")
    print(f"Total stations: {features_df.select('start_station_id').distinct().count()}")
    
    # 检查空值
    null_counts = features_df.select([count(when(col(c).isNull(), c)).alias(c) 
                                     for c in features_df.columns])
    display(null_counts)
    
    # 显示样本数据
    display(features_df.limit(10))
    
    # 时间范围
    date_range = features_df.selectExpr("min(date)", "max(date)")
    display(date_range)

# 运行验证
validate_processed_data()

# MAGIC %md
# MAGIC ## 9. 创建优化视图

# COMMAND ----------

# 创建常用视图以提高查询效率
spark.sql("""
    CREATE OR REPLACE VIEW citibike_ny.station_hourly_view AS
    SELECT 
        start_station_id,
        start_station_name,
        hour_timestamp,
        hour,
        date,
        departures,
        arrivals,
        net_flow,
        member_departures,
        casual_departures,
        departures_lag_1,
        departures_lag_24,
        departures_rolling_mean_24
    FROM citibike_ny.processed_features
""")

# 创建站点元数据视图
spark.sql("""
    CREATE OR REPLACE VIEW citibike_ny.station_metadata AS
    SELECT 
        start_station_id as station_id,
        FIRST(start_station_name) as station_name,
        AVG(station_lat) as station_lat,
        AVG(station_lng) as station_lng,
        COUNT(*) as total_records,
        AVG(departures) as avg_hourly_departures,
        AVG(arrivals) as avg_hourly_arrivals
    FROM citibike_ny.processed_features
    GROUP BY start_station_id
""")


# MAGIC %md
# MAGIC ## 10. 使用指南
# MAGIC 
# MAGIC ### 首次使用：
# MAGIC 1. 运行第1-5节的所有代码定义函数
# MAGIC 2. 运行 `save_raw_data_to_dbfs()` 将原始数据保存到DBFS
# MAGIC 3. 运行 `process_all_data()` 处理数据
# MAGIC 
# MAGIC ### Cluster重启后：
# MAGIC 1. 运行第1-5节的所有代码定义函数
# MAGIC 2. 运行 `rebuild_environment()` 重建表
# MAGIC 3. 运行 `validate_processed_data()` 验证数据
# MAGIC 
# MAGIC ### 查询数据：
# MAGIC ```sql
# MAGIC SELECT * FROM citibike_ny.station_hourly_view 
# MAGIC WHERE start_station_id = '123' 
# MAGIC AND date >= '2024-01-01'
# MAGIC ```

import os

# Define project root name
project_root = "citibike-intelligent-rebalancing"

# Define folders to create
folders = [
    project_root,
    f"{project_root}/notebooks",
    f"{project_root}/scripts",
    f"{project_root}/data",
    f"{project_root}/figures",
    f"{project_root}/docs"
]

# Create folders
for folder in folders:
    os.makedirs(folder, exist_ok=True)
    print(f"✅ Created: {folder}")

# Create basic placeholder files
with open(f"{project_root}/README.md", "w") as f:
    f.write("# CitiBike Intelligent Rebalancing System\n")

with open(f"{project_root}/requirements.txt", "w") as f:
    f.write("# Add your Python package requirements here\n")

with open(f"{project_root}/.gitignore", "w") as f:
    f.write("""
# Byte-compiled files
__pycache__/
*.py[cod]

# Databricks metadata
.dbfs/
.checkpoints/
.delta_log/
spark-warehouse/

# Jupyter / Notebook artifacts
.ipynb_checkpoints/

# System files
.DS_Store
Thumbs.db

# Logs
*.log

# Temporary files
tmp/
""")

print("\n✨ Project folders and basic files created successfully!")