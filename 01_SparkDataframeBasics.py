# Databricks notebook source
# simply a python list containing tuples
data_df = [
    (1, "Arun", "IT", 60000),
    (2, "Sneha", "HR", 45000),
    (3, "Ravi", "IT", 75000),
    (4, "Priya", "Finance", 70000),
    (5, "Kiran", "HR", 50000)
]
display(data_df)

# COMMAND ----------

columns=[
    "employee_id",
    "name",
    "department",
    "salary"
]

# COMMAND ----------

# convert to spark dataframe
df=spark.createDataFrame(data_df, columns)
display(df)

# COMMAND ----------

type(data)

# COMMAND ----------

type(df)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.first()

# COMMAND ----------

row=df.first()
row["name"]
row.name

# COMMAND ----------

df["salary"] # Reference to salary column not the values

# COMMAND ----------

df["salary"]>50000 # For every row, check whether salary is greater than 50000

# But Spark still has not filtered anything yet

# You need to give that condition to:
df.filter(df["salary"]>50000).show()

# COMMAND ----------

# instead of df["salary"] we can also write:

from pyspark.sql.functions import col
col("salary")

df.filter(col("salary")>50000).show()

# COMMAND ----------

# If I only want name and salary
df.select("name","salary").show()

# COMMAND ----------

# col() becomes much more useful when you want to do calculations
df.select(
    col("name"),
    col("salary")+5000
).show()

df.select(
    col("name"),
    (col("salary")+5000).alias("new_salary")
).show()

# it will not modify the original salary

# COMMAND ----------

# Selecting all columns
df.select("*").show()

# COMMAND ----------

# keeps only rows where the condition is True-- filter and where
df.where(df["salary"]>50000).show()

# same as:
# df.filter(df["salary"]>50000).show()

# COMMAND ----------

# equality condition
df.filter(df["department"]=="IT").show()

# ==, !=, >, <, >=, <=

# COMMAND ----------

# multiple conditions
df.filter((df["department"]=="IT") & (df["salary"]>60000)).show()

# &, !, ~

# COMMAND ----------

df.filter((col("department")=="IT") | (col("department")=="Finance")).show()

df.filter(col("department").isin("IT","Finance")).show()

dept=["IT", "Finance"]
df.filter(col("department").isin(dept)).show()

# COMMAND ----------

# SQL style filter
df.filter("salary > 50000").show()

df.filter("department='IT'").show()

# COMMAND ----------

# withColumn-add new column
df2=df.withColumn(
    "annual_salary",
    col("salary")*12
)

df2.show()

# withColumn-replace a column
df2=df.withColumn(
    "salary",
    col("salary")+5000
)

df2.show()

# COMMAND ----------

# withColumnRenamed()-rename a column without changing its values
df2=df.withColumnRenamed(
    "salary",
    "monthly_salary"
)

df2.show()

# COMMAND ----------

# Renaming multiple columns
df2=(
    df
    .withColumnRenamed("id","employee_id")
    .withColumnRenamed("name","employee_name")
    .withColumnRenamed("salary","monthly_salary")
)

df2.show()

# COMMAND ----------

# drop()
df3=df.drop("salary")
df3.show()

# COMMAND ----------

# Dropping multiple columns
df3=df3.drop(
    "name",
    "department"
)
df3.show()

# COMMAND ----------

data = [
    (1, "Arun", "IT"),
    (2, "Sneha", "HR"),
    (1, "Arun", "IT"),
    (4, "Ravi", "IT"),
    (5, "Sneha", "HR")
]

columns = ["id", "name", "department"]

data= spark.createDataFrame(data, columns)

# COMMAND ----------

# distinct()-removes rows that are completely identical across all columns
data.distinct().show()

# COMMAND ----------

# dropDuplicates-With no arguments, it also removes completely identical rows

data.dropDuplicates().show()

# COMMAND ----------

# on specific columns

data.dropDuplicates(["name","department"]).show()

# COMMAND ----------

# distinct()-unique entire rows
# dropDuplicates()-unique entire rows or unique based on selected columns

# COMMAND ----------

# sorting
df.orderBy("salary").show() # ascending

df.orderBy(col("salary").asc()).show()

df.orderBy(col("salary").desc()).show() 

df.sort("salary").show()



# COMMAND ----------

# Sorting by multiple columns
df.orderBy(
    col("department").asc(),
    col("salary").desc()
).show()

# COMMAND ----------

# limit()

df.limit(3).show()

# COMMAND ----------

# null handling
df.filter(
    col("salary").isNull()
).show()

df.filter(
    col("salary").isNotNull()
).show()

# COMMAND ----------

# filling nulls 

df.fillna(
    {
        "salary": 0
    }
).show()

df.fillna(
    {
        "salary": 0,
        "department": "Unknown",
        "name": "Missing"
    }
).show()

df.na.fill(
    {
        "salary": 0
    }
).show()

# COMMAND ----------

# Dropping rows with nulls
df.dropna().show()

# COMMAND ----------

# Drop rows where salary is null
df.dropna(
    subset=["salary"]
).show()

# either name or department is null
df.dropna(
    subset=["name","department"]
).show()

# COMMAND ----------

# Drop the row if any selected column contains null
df.dropna(how="any")

# Drop the row only if all selected columns are null
df.dropna(how="all")

# Drop the row only when both name and department are null
df.dropna(
    how="all",
    subset=["name","department"]
)

# COMMAND ----------

