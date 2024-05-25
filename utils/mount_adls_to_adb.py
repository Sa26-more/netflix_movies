# Databricks notebook source
# MAGIC %md
# MAGIC dbutils.fs.mounts()
# MAGIC mount_point = "/mnt/myadlsgen2"
# MAGIC storage_account_name = "<storage-account-name>"
# MAGIC container_name = "zip-files"
# MAGIC client_id = "<client-id>"
# MAGIC tenant_id = "<tenant-id>"
# MAGIC client_secret = "<client-secret>"
# MAGIC
# MAGIC configs = {
# MAGIC   "fs.azure.account.auth.type": "OAuth",
# MAGIC   "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC   "fs.azure.account.oauth2.client.id": client_id,
# MAGIC   "fs.azure.account.oauth2.client.secret": client_secret,
# MAGIC   "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
# MAGIC }
# MAGIC
# MAGIC if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC         source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
# MAGIC         mount_point = mount_point,
# MAGIC         extra_configs = configs
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC client_id == application_id, client_secret == service_credential, tenant_id == directory_id

# COMMAND ----------

# Get the secrete scope name.
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get('adb-scope','kv-application-id')

# COMMAND ----------

application_id = dbutils.secrets.get('adb-scope','kv-application-id')
directory_id = dbutils.secrets.get('adb-scope','kv-directory-id')
service_credential = dbutils.secrets.get('adb-scope','kv-service-credential')

# COMMAND ----------

application_id = dbutils.secrets.get('adb-scope','kv-application-id')
directory_id = dbutils.secrets.get('adb-scope','kv-directory-id')
service_credential = dbutils.secrets.get('adb-scope','kv-service-credential')

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

if not any(mount.mountPoint == "/mnt/zip-files" for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "abfss://zip-files@practiceadb.dfs.core.windows.net/",
    mount_point = "/mnt/zip-files",
    extra_configs = configs)
else:
    print(f"Mount point '/mnt/zip-files' already exists")

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------

dbutils.fs.ls('/mnt/zip-files/')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("show_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("director", StringType(), True),
    StructField("cast", StringType(), True),
    StructField("country", StringType(), True),
    StructField("date_added", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("rating", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("listed_in", StringType(), True),
    StructField("description", StringType(), True)
])

df = spark.read.option("compression", "gzip").option("escapechar", "\\\\").csv('/mnt/zip-files/data.csv.gz', header=False, schema=schema)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("show_id", StringType(), True),
])

df = spark.read.option("compression", "gzip").option("escapechar", "\\\\").option("quote", "").option("delimiter", "\n").csv('/mnt/zip-files/movies.csv.gz', header=False, schema=schema)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

df = spark.read.option("compression", "gzip").option("encoding","UTF8").option("escapechar","\\\\").option("quote","").csv('/mnt/zip-files/movies.csv.gz', header=False)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import split

df = df.withColumn("split_col", split(df["show_id"], "(?<!\\\\),"))

for i in range(12):
    df = df.withColumn(f"c{i+1}", df.split_col.getItem(i))

df = df.drop("split_col")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the correct schema that includes all the fields you want to work with
schema = StructType([
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("director", StringType(), True),
    StructField("cast", StringType(), True),
    StructField("country", StringType(), True),
    StructField("date_added", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("rating", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("listed_in", StringType(), True),
    StructField("description", StringType(), True)
])

# Read the CSV file with the correct schema
df = spark.read.option("compression", "gzip") \
    .option("escapechar", "\\") \
    .option("quote", "\"") \
    .option("delimiter", ",") \
    .csv('/mnt/zip-files/movies.csv.gz', header=False, schema=schema)

# Apply the regexp_replace function to clean up the data
for i, field in enumerate(schema.fields, start=1):
    df = df.withColumn(field.name, regexp_replace(col(field.name), "\\\\", ""))
    df = df.withColumn(field.name, regexp_replace(col(field.name), "\\\\N", ""))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("show_id", StringType(), True),
])

df = spark.read.option("compression", "gzip").option("escapechar", "\\\\").option("quote", "").option("delimiter", "\n").csv('/mnt/zip-files/movies.csv.gz', header=False, schema=schema)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = df.withColumn("show_id",regexp_replace(col("show_id"), "\\\\", ""))
df.display()

# COMMAND ----------



# COMMAND ----------

# Loop through the schema fields and apply the regexp_replace transformations
for field in schema.fields:
    df = df.withColumn(field.name, regexp_replace(col(field.name), "\\\\", ""))
    df = df.withColumn(field.name, regexp_replace(col(field.name), "\\\\N", ""))

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# Assuming df is your initial DataFrame and it has been loaded correctly

# Drop the "show_id" column if it exists
df = df.drop("show_id") if "show_id" in df.columns else df

# Loop through the schema fields and apply the regexp_replace transformations
for field in schema.fields:
    df = df.withColumn(field.name, regexp_replace(col(field.name), "\\\\", ""))
    df = df.withColumn(field.name, regexp_replace(col(field.name), "\\\\N", ""))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import expr

df = df.withColumn("new_column", expr("""regexp_replace(regexp_replace(concat('"', regexp_replace(regexp_replace(show_id, '\\\\,', 'COMMA_PLACEHOLDER'), ',', '","'), '"'), 'COMMA_PLACEHOLDER', ','))"""))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col, window

spark = SparkSession.builder.appName("Unzip").getOrCreate()
df = spark.read.option("compression", "gzip").csv(r"C:\Users\91900\Downloads\data.csv.gz", header=True)
df.display()
