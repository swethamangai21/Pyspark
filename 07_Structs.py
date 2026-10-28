# Databricks notebook source
# struct()- a nested object inside one column

# COMMAND ----------

data=[
    (1, "Arun", "Bangalore", "Karnatake", "560001"),
    (2, "Sneha", "Mumbai", "Maharashtra", "400001"),
    (3, "Ravi", "Hyderabad", "Telangana", "500001")
]

df=spark.createDataFrame(
    data,
    ["employee_id", "name", "city", "state", "pincode"]
)

df.show()

# COMMAND ----------

from pyspark.sql.functions import *

df2=df.withColumn(
    "address",
    struct(
        col("city"), col("state"), col("pincode")
    )
)

df2.show(truncate=False)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# accessing nested fields

df2.select(
    "name",
    col("address.city").alias("city")
).show()

# COMMAND ----------

# another way to access nested fields

df2.select(
    "name",
    col("address")["city"].alias("city")
).show()

# COMMAND ----------

# selecting all fields inside struct

df2.select(
    "employee_id",
    "name",
    "address.*"
).show()

# COMMAND ----------

# updating a nested field

df2.withColumn(
    "address",
    col("address").withField(
        "city",
        upper(col("address.city"))
    )
).show(truncate=False)

# COMMAND ----------

# dropping a field from struct

df2.withColumn(
    "address",
    col("address").dropFields("pincode")
).show(truncate=False)