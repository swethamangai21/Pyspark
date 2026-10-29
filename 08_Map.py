# Databricks notebook source
# map- key-value structure inside one column

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data=[
    (1, "arun", {"Python": 5, "SQL": 4, "Spark": 4}),
    (2, "Sneha", {"Excel": 5, "SQL": 3}),
    (3, "Ravi", {"Python": 4, "Databricks": 5})
]

df= spark.createDataFrame(
    data,
    ["employee_id", "name", "skill_rating"]
)

df.show(truncate=False)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# access a value using a key

df.select(
    "name",
    col("skill_rating")["Python"].alias("python_rating")
).show()

# COMMAND ----------

# another way to access the map

df.select(
    "name",
    element_at(
        col("skill_rating"),
        lit("SQL")
    ).alias("sql_rating")
).show()

# COMMAND ----------

# map_key()- get all keys

df.select(
    "name",
    map_keys(col("skill_rating")).alias("skills")
).show()

# COMMAND ----------

# map_values()- return map values

df.select(
    "name",
    map_values(
        col("skill_rating")
    ).alias("ratings")
).show()

# COMMAND ----------

# map_contains_key()- check whether a map contains a particular key

df.select(
    "name",
    map_contains_key(
        col("skill_rating"),
        "Python"
    ).alias("knows_python")
).show()

# COMMAND ----------

# explode()

df.select(
    "name",
    explode(
        col("skill_rating")
    ).alias("skills", "rating")
).show()

# COMMAND ----------

# posexplode()

df.select(
    "name",
    posexplode(
        col("skill_rating")
    ).alias("position", "skill", "rating")
).show()

# COMMAND ----------

# creating a map using create_map()

# spark.range(1) creates a DataFrame with one row

spark.range(1).select(
    create_map(
    lit("Pyrhon"),
    lit(5),
    lit("SQL"),
    lit(4)
).alias("skills")
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,map_from_arrays()
# map_from_arrays()

from pyspark.sql.functions import col, map_from_arrays

data = [
    (1, ["Python", "SQL"], [5, 4])
]

map_df = spark.createDataFrame(
    data,
    ["id", "skills", "ratings"]
)

map_df.select(
    "id",
    map_from_arrays(
        col("skills"),
        col("ratings")
    ).alias("skill_map")
).show(truncate=False)

# COMMAND ----------

# map_concat()

data=[
    (
        1,
        {"Python": 5, "SQL": 4},
        {"Spark": 4, "Databricks": 5}
    )
]

map_df=spark.createDataFrame(
    data,
    ["id", "technical_skills", "platform_skills"]
)

map_df.show()

map_df.select(
    "id",
    map_concat(
        col("technical_skills"),
        col("platform_skills")
    ).alias("all_skills")
).show(truncate=False)

# COMMAND ----------

# transform_values()

df.select(
    "name",
    transform_values(
        col("skill_rating"),
        lambda k, v: v+1
    ).alias("updated_rating")
).show(truncate=False)

# COMMAND ----------

# transform_keys()

df.select(
    "name",
    transform_keys(
        col("skill_rating"),
        lambda k, v: upper(k)
    ).alias("uppercase_skils")
).show(truncate=False)

# COMMAND ----------

# map_filter()- filter key value pairs

df.select(
    "name",
    map_filter(
        col("skill_rating"),
        lambda k, v: v>=5
    ).alias("strong_skills")
).show(truncate=False)