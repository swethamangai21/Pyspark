# Databricks notebook source
employees_data = [
    (101, "Arun", 1),
    (102, "Sneha", 2),
    (103, "Ravi", 1),
    (104, "Priya", 3),
    (105, "Kiran", 5)
]

employees_df = spark.createDataFrame(
    employees_data,
    ["employee_id", "name", "department_id"]
)

employees_df.show()

# COMMAND ----------

departments_data = [
    (1, "IT"),
    (2, "HR"),
    (3, "Finance"),
    (4, "Marketing")
]

departments_df = spark.createDataFrame(
    departments_data,
    ["department_id", "department_name"]
)

departments_df.show()

# COMMAND ----------

# inner join

employees_df.join(
    departments_df,
    on="department_id",
    how="inner"
).show()

# COMMAND ----------

# left join

employees_df.join(
    departments_df,
    on="department_id",
    how="left"
).show()

# COMMAND ----------

# right join

employees_df.join(
    departments_df,
    on="department_id",
    how="right"
).show()

# COMMAND ----------

# full outer join

employees_df.join(
    departments_df,
    on="department_id",
    how="full"
).show()

# COMMAND ----------

# left semi join 
# returns columns only from the LEFT DataFrame, not the RIGHT DataFrame

# Give me employees whose department exists in the department table.  You don't actually need department columns.

employees_df.join(
    departments_df,
    on="department_id",
    how="left_semi"
).show()

# COMMAND ----------

# left anti join

employees_df.join(
    departments_df,
    on="department_id",
    how="left_anti"
).show()

# COMMAND ----------

# join using explicit conditions
# sometimes the columns don't have the same name

employees_df.join(
    departments_df,
    employees_df["department_id"]==departments_df["department_id"],
    "inner"
).show()

# COMMAND ----------

# aliases when joining

emp=employees_df.alias("emp")
dept=departments_df.alias("dept")

emp.join(
    dept,
    emp.department_id==dept.department_id,
    "inner"
).show()

# COMMAND ----------

# joining multiple columns

customers_data = [
    (1, "India", "Arun"),
    (1, "USA", "John"),
    (2, "India", "Sneha"),
    (3, "India", "Ravi")
]

customers_df = spark.createDataFrame(
    customers_data,
    ["customer_id", "country", "customer_name"]
)

customers_df.show()

orders_data = [
    (1, "India", 5000),
    (1, "USA", 7000),
    (2, "India", 3000),
    (2, "USA", 4000)
]

orders_df = spark.createDataFrame(
    orders_data,
    ["customer_id", "country", "amount"]
)

orders_df.show()

joined_df=customers_df.join(
    orders_df,
    (customers_df["customer_id"]==orders_df["customer_id"]) &
    (customers_df["country"]==orders_df["country"]),
    "inner"
)

joined_df.show()

# cleaner version-when both data frames have same names
# joined_df=customers_df.join(
#     orders_df,
#     ["customer_id", "country"],
#     "inner"
# )

# COMMAND ----------

# null join keys

# two null join keys do not normally match using ==
# Spark also has null-safe equality: eqNullSafe()
employees_df.join(
    departments_df,
    employees_df["department_id"].eqNullSafe(departments_df["department_id"]),
    "inner"
).show()

# COMMAND ----------

# cross join

employees_df.join(
    departments_df,
    how="cross"
).show()

# COMMAND ----------

# broadcast join

from pyspark.sql.functions import broadcast

employees_df.join(
    broadcast(departments_df),
    "department_id",
    "left"
).show()