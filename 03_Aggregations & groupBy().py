# Databricks notebook source
sales_data = [
    (1, "IT", "Laptop", 50000),
    (2, "IT", "Mouse", 2000),
    (3, "HR", "Laptop", 45000),
    (4, "HR", "Mouse", 1500),
    (5, "IT", "Keyboard", 3000),
    (6, "Finance", "Laptop", 60000)
]

columns = [
    "order_id",
    "department",
    "product",
    "amount"
]

sales_df = spark.createDataFrame(
    sales_data,
    columns
)

sales_df.show()

# COMMAND ----------

# groupBy()- sum()

(sales_df
    .groupBy("department")
    .sum("amount")
    .show()
)

# COMMAND ----------

# groupBy()- count()

sales_df.groupBy("department").count().show()

# COMMAND ----------

# groupBy()- avg()

sales_df.groupBy("department").avg("amount").show()

# COMMAND ----------

# groupBy()- min()

sales_df.groupBy("department").min("amount").show()

# COMMAND ----------

# groupBy()- max()

sales_df.groupBy("department").max("amount").show()

# COMMAND ----------

# multiple aggregations 

from pyspark.sql.functions import(
    sum, avg, min, max, count
)

sales_df.groupBy(
    "department"
).agg(
    sum("amount").alias("total_sales"),
    avg("amount").alias("average_sales"),
    min("amount").alias("minimum_sales"),
    max("amount").alias("maximum_sales"),
    count("*").alias("order_count")
).show()


# COMMAND ----------

# countDistinct()

from pyspark.sql.functions import countDistinct

sales_df.groupBy(
    "department"
).agg(
    countDistinct("product").alias("unique_products")
).show()

# COMMAND ----------

# count("*") → Counts all rows, including rows containing NULL values

# count(column), sum(column), avg(column), min(column), max(column)→ Counts only rows where that particular column is NOT NULL

# COMMAND ----------

# grouping by multiple columns

sales_df.groupBy(
    "department",
    "product"
).sum("amount").show()

# COMMAND ----------

# grouping entire dataframe

sales_df.agg(sum("amount").alias("total_sales")).show()

# COMMAND ----------

# filtering after aggregation

from pyspark.sql.functions import col, sum

dept_sales_df=(
    sales_df
    .groupBy("department")
    .agg(sum("amount").alias("total_sales"))
)

dept_sales_df.filter(
    col("total_sales")>50000
).show()

# COMMAND ----------

# filtering before aggregation

sales_df.filter(
    col("product")=="Laptop"
).groupBy(
    "department"
).agg(
    sum("amount").alias("laptop_sales")
).show()

# COMMAND ----------

# conditional aggregation
# For every department, calculate total sales and Laptop sales separately

from pyspark.sql.functions import when

sales_df.groupBy(
    "department"
).agg(
    sum("amount").alias("total_sales"),
    sum(
        when(
            col("product")=="Laptop",
            col("amount")
        ).otherwise(0)
    ).alias("laptop_sales")
).show()

# COMMAND ----------

# conditional count
# Count Laptop orders by department

sales_df.groupBy(
    "department"
).agg(
    sum(
        when(
            col("product")=="Laptop",
            1
        ).otherwise(0)
    ).alias("laptop_count")
).show()

# COMMAND ----------

# percentage calculation after aggregation
# What percentage of department sales came from Laptop?

from pyspark.sql.functions import round

summary_df=sales_df.groupBy(
    "department"
).agg(
    sum("amount").alias("total_sales"),
    sum(
        when(
            col("product")=="Laptop",
            col("amount")
        ).otherwise(0)
    ).alias("laptop_sales")
)

summary_df.withColumn(
    "laptop_percentage",
    round(
        col("laptop_sales")/
        col("total_sales")*100,
        2
)
).show()

# COMMAND ----------

# collect_list()
# collect values into an array including duplicates

from pyspark.sql.functions import collect_list

sales_df.groupBy(
    "department"
).agg(
    collect_list("product").alias("products")
).show(truncate=False)

# COMMAND ----------

# collect_set()
# returns unique values

from pyspark.sql.functions import collect_set

sales_df.groupBy(
    "department"
).agg(
    collect_set("product").alias("unique_products")
).show(truncate=False)

# COMMAND ----------

# first(), last()
# Aggregation functions can return a first/last encountered value

from pyspark.sql.functions import first, last

sales_df.groupBy(
    "department"
).agg(
    first("product").alias("first_product"),
    last("product").alias("last_product")
).show()