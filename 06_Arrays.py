# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data=[
    (1, "Arun", ["Python", "SQL", "Spark"]),
    (2, "Sneha", ["Excel", "SQL"]),
    (3, "Ravi", ["Python", "Databricks"])
]

df=spark.createDataFrame(
    data,
    ["employee_id", "name", "skills"]
)

df.show()

# COMMAND ----------

# accessing array elements

df.select(
    "name",
    col('skills')[0].alias("first_skill")
).show()

# COMMAND ----------

# element_at()- Another way to access array elements

df.select(
    "name",
    element_at(col("skills"), 1).alias("first_skill")
).show()

# COMMAND ----------

# size()- returns number of elements in the array

df.select(
    "name",
    size("skills").alias("number_of_skills")
).show()

# COMMAND ----------

# array_contains()

# find employees who know python

df.filter(
    array_contains(col("skills"), "Python")
).show()

# COMMAND ----------

# explode()

df.select(
    "name",
    explode(col("skills")).alias("skill")
).show()

# COMMAND ----------

# posexplode()- gives postition too

df.select(
    "employee_id",
    "name",
    posexplode("skills").alias("position", "skill")
).show()

# COMMAND ----------

# array_position()

df.select(
    "name",
    array_position(col("skills"), "SQL").alias("sql_position")
).show()

# COMMAND ----------

# array_sort()

df.select(
    "name",
    array_sort(col("skills")).alias("sorted_skills")
).show()

# COMMAND ----------

# array_distinct()

df1 = spark.createDataFrame(
    [
        (1, "Arun", ["Python", "SQL", "Python", "Spark"])
    ],
    ["employee_id", "name", "skills"]
)

df1.select(
    "name",
    array_distinct(col("skills")).alias("unique_skills")
).show()

# COMMAND ----------

# array_union

skills_df = spark.createDataFrame(
    [
        (1, ["Python", "SQL"], ["Spark", "SQL"]),
        (2, ["Excel"], ["SQL", "PowerBI"])
    ],
    ["id", "skills_1", "skills_2"]
)

skills_df.show()

skills_df.select(
    "id",
    array_union(
        col("skills_1"), col("skills_2")
    ).alias("all_skills")
).show()

# COMMAND ----------

# array_intersect()

skills_df.select(
    "id",
    array_intersect(
        col("skills_1"), col("skills_2")
    ).alias("common_skills")
).show()

# COMMAND ----------

# array_except()- Find elements present in first array but not second

skills_df.select(
    "id",
    array_except(
        col("skills_1"), col("skills_2")
    ).alias("only_in_first")
).show()

# COMMAND ----------

# slice()- extract part of an array
# slice(array, start, length)

df.select(
    "name",
    slice(col("skills"), 1, 2).alias("first_two_skills")
).show()

# COMMAND ----------

# array_min(), array_max()

score_df = spark.createDataFrame(
    [
        (1, [10, 20, 15]),
        (2, [50, 30, 40])
    ],
    ["id", "scores"]
)

score_df.select(
    "id",
    array_min("scores").alias("minimum"),
    array_max("scores").alias("maximum")
).show()

# COMMAND ----------

# flatten()

nested_df=spark.createDataFrame(
    [
        (1, [["Python", "SQL"], ["Spark", "Databricks"]]),
        (2,[["Excel"], ["PowerBI", "SQL"]])
    ],
    ["id", "skill_groups"]
)

nested_df.show(truncate=False)

nested_df.select(
    "id",
    flatten(col("skill_groups")).alias("skills")
).show(truncate=False)

# COMMAND ----------

# transform()
# upper() transforms a string. transform() applies a function to every element in an array

df.select(
    "name",
    transform(
        col("skills"),
        lambda x: upper(x)
    ).alias("upper_skills")
).show()

# COMMAND ----------

# filter()

df.select(
    "name",
    filter(
        col("skills"),
        lambda x: x.contains("S")
    ).alias("filtered_skills")
).show()

# COMMAND ----------

# exists()- Check whether at least one element satisfies a condition

df.select(
    "name",
    exists(
        col("skills"),
        lambda x: x == "Python"
    ).alias("knows_python")
).show()