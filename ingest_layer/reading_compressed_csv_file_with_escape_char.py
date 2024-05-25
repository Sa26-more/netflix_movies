# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType(
    [
        StructField("show_id", StringType(), True),
    ]
)

df = spark.read.option("delimiter", "\n").csv(
    "/mnt/zip-files/movies.csv.gz", header=False, schema=schema
)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

df = df.withColumn("show_id", regexp_replace(col("show_id"), "\,", "|"))
df.display()

# COMMAND ----------

df = df.withColumn("show_id", regexp_replace(col("show_id"), r"\\\|", ","))
df.display()

# COMMAND ----------

from pyspark.sql.functions import split

# Split the `show_id` column into multiple columns based on the pipe separator
df_split = df.withColumn("split_value", split(df["show_id"], "\|"))

# Create new columns from the split_value array
for i in range(12):
    df_split = df_split.withColumn(f"col{i+1}", df_split["split_value"].getItem(i))

# Drop the original show_id column and the intermediate split_value column
df_final = df_split.drop("show_id", "split_value")

df_final.display()

# COMMAND ----------

# Supported in spark 3.5 and above
df_final.withColumnsRenamed(
    {
        "col1": "show_id",
        "col2": "type",
        "col3": "title",
        "col4": "director",
        "col5": "cast",
        "col6": "country",
        "col7": "date_added",
        "col8": "release_year",
        "col9": "rating",
        "col10": "duration",
        "col11": "listed_in",
        "col12": "description",
    }
).display()

# COMMAND ----------

# Supported in all spark version
df_final = (
    df_final.withColumnRenamed("col1", "show_id")
    .withColumnRenamed("col2", "type")
    .withColumnRenamed("col3", "title")
    .withColumnRenamed("col4", "director")
    .withColumnRenamed("col5", "cast")
    .withColumnRenamed("col6", "country")
    .withColumnRenamed("col7", "date_added")
    .withColumnRenamed("col8", "release_year")
    .withColumnRenamed("col9", "rating")
    .withColumnRenamed("col10", "duration")
    .withColumnRenamed("col11", "listed_in")
    .withColumnRenamed("col12", "description")
)
df_final.display()

# COMMAND ----------

from datetime import datetime

# Get the current date
current_date = datetime.now()

# Format the date as 'yyyyMMdd'
formatted_date = current_date.strftime('%Y%m%d')

print(formatted_date)

# COMMAND ----------

df_final.write.format("delta").save(f"/mnt/zip-files/bronze_netflix/{formatted_date}")
