# Databricks notebook source
employee_data = [
    (101, "Arun", "IT", 60000),
    (102, "Sneha", "HR", 45000),
    (103, "Ravi", "IT", 75000),
    (104, "Priya", "Finance", 70000),
    (105, "Kiran", "HR", 50000),
    (106, "Meera", "IT", 75000),
    (107, "Rahul", "Finance", 65000)
]

employees_df = spark.createDataFrame(
    employee_data,
    ["employee_id", "name", "department", "salary"]
)

employees_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# Window.partitionBy()

window_spec=(
    Window
    .partitionBy("department")
    .orderBy(col("salary").desc())
)

# COMMAND ----------

# row_number()

employees_df.withColumn(
    "row_num",
    row_number().over(window_spec)
).show()

# the order between tied rows is not deterministic unleass you add another tie-breaker

# window_spec=(
#     Window
#     .partitionBy("department")
#     .orderBy(
#         col("salary").desc(),
#         col("employee_id").asc()
#     )
# )

# COMMAND ----------

# rank()

employees_df.withColumn(
    "rank",
    rank().over(window_spec)
).show()

# COMMAND ----------

# dense_rank()

employees_df.withColumn(
    "dense_rank",
    dense_rank().over(window_spec)
).show()

# COMMAND ----------

# top n per group

# fin top 2 highest paid employees in each group

employees_df.withColumn(
    "row_number",
    row_number().over(window_spec)
).filter(col("row_number")<=2).show()

# COMMAND ----------

# second highest salary per department

employees_df.withColumn(
    "second_highest_salary",
    dense_rank().over(window_spec)
).filter(col("second_highest_salary")==2).show()

# COMMAND ----------

# lag()

employees_df.withColumn(
    "previous_salary",
    lag(col("salary"),1).over(window_spec)
).show()

# COMMAND ----------

# difference from previous row

employees_df.withColumn(
    "previous_salary",
    lag("salary").over(window_spec)
).withColumn(
    "salary_difference",
    col("salary")-col("previous_salary")
).show()

# COMMAND ----------

# lead()

employees_df.withColumn(
    "next_salary",
    lead(col("salary"),1).over(window_spec)
).show()

# COMMAND ----------

# running total

sales_data = [
    ("2026-01", 10000),
    ("2026-02", 15000),
    ("2026-03", 12000),
    ("2026-04", 18000)
]

sales_df = spark.createDataFrame(
    sales_data,
    ["month", "sales"]
)

running_window=(
    Window
    .orderBy("month")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

sales_df.withColumn(
    "running_total",
    sum("sales").over(running_window)
).show()

# COMMAND ----------

# rolling average

# calculate average sales over current row + previous 2 rows

rolling_window=(
    Window
    .orderBy("month")
    .rowsBetween(-2, 0)
)

sales_df.withColumn(
    "rolling_avg",
    avg("sales").over(rolling_window)
).show()

# COMMAND ----------

# partitioned running total 

# suppose sales are by department and month

dept_sales_data = [
    ("IT", "2026-01", 10000),
    ("IT", "2026-02", 15000),
    ("IT", "2026-03", 12000),
    ("HR", "2026-01", 5000),
    ("HR", "2026-02", 7000)
]

dept_sales_df = spark.createDataFrame(
    dept_sales_data,
    ["department", "month", "sales"]
)

dept_running_window=(
    Window
    .partitionBy("department")
    .orderBy("month")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

dept_sales_df.withColumn(
    "running_total",
    sum("sales").over(dept_running_window)
).show()

# COMMAND ----------

# average salary per department

dept_window=Window.partitionBy("department")

employees_df.withColumn(
    "department_avg_salary",
    avg("salary").over(dept_window)
).show()

# COMMAND ----------

# department total salary

employees_df.withColumn(
    "department_total_salary",
    sum("salary").over(dept_window)
).show()

# COMMAND ----------

# percentage contribution within group 

# what percentage of department payroll does each employee contribute

employees_df.withColumn(
    "department_total",
    sum("salary").over(dept_window)
).withColumn(
    "salary_percentage",
    round(col("salary")/col("department_total")*100,2)
).show()

# COMMAND ----------

# latest record per customer

customer_data = [
    (1, "Bangalore", "2026-01-01"),
    (1, "Mysore", "2026-03-10"),
    (1, "Hubli", "2026-02-05"),
    (2, "Delhi", "2026-01-15"),
    (2, "Mumbai", "2026-04-01")
]

customer_df = spark.createDataFrame(
    customer_data,
    ["customer_id", "city", "updated_at"]
)

w=(
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_at").desc())
)

customer_df.withColumn(
    "latest_update",
    row_number().over(w)
).filter(col("latest_update")==1).show()

# COMMAND ----------

# deduplicate by latest timestamp

customer_df.withColumn(
    "row_number",
    row_number().over(w)
).filter(col("row_number")==1).show()

# COMMAND ----------

# first value within group
# first()- returns the first non-null value in a group, used in groupBy also

employees_df.withColumn(
    "highest_salary",
    first("salary").over(
        Window
        .partitionBy("department")
        .orderBy(col("salary").desc())
    )
).show()

employees_df.withColumn(
    "highest_salary",
    max("salary").over(
        Window
        .partitionBy("department")
        .orderBy(col("salary").desc())
    )
).show()

# COMMAND ----------

# first_value()- in window frame

employees_df.withColumn(
    "highest_salary",
    first_value("salary").over(
        Window
        .partitionBy("department")
        .orderBy(col("salary").desc())
    )
).show()

# COMMAND ----------

# last_value()- mention the window frame because the default frame ends at the current row

employees_df.withColumn(
    "lowest_salary",
    last_value("salary").over(
        Window
        .partitionBy("department")
        .orderBy(col("salary").asc())
        .rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing
        )
    )
).show()

# COMMAND ----------

# ntile()- Splits rows into approximately equal buckets

employees_df.withColumn(
    "quartiel",
    ntile(4).over(
        Window.orderBy(col("salary").desc())
    )
).show()

# COMMAND ----------

# percent_rank()- Shows relative rank from 0 to 1

employees_df.withColumn(
    "percent_rank",
    percent_rank().over(
        Window.orderBy("salary")
    )
).show()

# COMMAND ----------

# cume_dist()- cumulative distribution

employees_df.withColumn(
    "cumulative_distribution",
    cume_dist().over(
        Window.orderBy("salary")
    )
).show()

# COMMAND ----------

# rangebetween()

employees_df.withColumn(
    "total",
    sum("salary").over(
        Window
        .orderBy("salary")
        .rangeBetween(-10000, 0)
    )
).show()