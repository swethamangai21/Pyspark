# Databricks notebook source
# employee dataframe

from pyspark.sql.functions import*
from pyspark.sql.types import *

employee_data=[
    (101, "  arun kumar  ", "IT", "60000.50", "2021-06-15", True),
    (102, "SNEHA", "HR", "45000.75", "2023-01-10", False),
    (103, "ravi sharma", "IT", "75000.90", "2020-09-20", True),
    (104, "  priya  ", "Finance", "52000.25", "2022-11-05", False),
    (105, "KIRAN", None, None, "2024-03-12", True)
]

employee_columns=[
    "employee_id",
    "name",
    "department",
    "salary",
    "joining_date",
    "is_active"
]

employees_df=spark.createDataFrame(
    employee_data,
    employee_columns
)

employees_df.show()

# COMMAND ----------

# orders dataframe

order_data = [
    (1001, 101, "Laptop", "74999.99", "2026-10-01", "2026-10-01 10:30:00"),
    (1002, 102, "Mouse", "599.50", "2026-10-05", "2026-10-05 14:15:00"),
    (1003, 101, "Keyboard", "1299.00", "2026-10-10", "2026-10-10 18:45:00"),
    (1004, 103, "Monitor", "18999.75", "2026-09-25", "2026-09-25 09:20:00")
]

order_columns = [
    "order_id",
    "employee_id",
    "product",
    "amount",
    "order_date",
    "created_at"
]

orders_df = spark.createDataFrame(
    order_data,
    order_columns
)

orders_df.show()

# COMMAND ----------

# when() otherwise()

employees_df.withColumn(
    "salary_category",
    when(col("salary").cast("double") >=70000, "High")
    .when(col("salary").cast("double") >=50000, "Medium")
).show()

# COMMAND ----------

# when() otherwise()

employees_df.withColumn(
    "salary_category",
    when(col("salary").cast("double") >=70000, "High")
    .when(col("salary").cast("double") >=50000, "Medium")
    .otherwise("low")
).show()

# COMMAND ----------

# multiple conditions inside when()

employees_df.withColumn(
    "bonus_eligible",
    when(
        (col("department")=="IT") & (col("salary").cast("double")>60000), "Yes"
    ).otherwise("No")
).show()

# COMMAND ----------

# Returning another column
# If salary is above 70000, add 10000. Otherwise keep salary unchanged

employees_df.withColumn(
    "revised_salary",
    when(col("salary").cast("double")>70000, col("salary").cast("double")+10000)
    .otherwise(col("salary"))
).show()

# COMMAND ----------

employees_df.printSchema()

# COMMAND ----------

# common cast types

# string
# int/integer
# long/bigint
# double
# float
# boolean
# date
# timestamp
# decimal(...)

# COMMAND ----------

# cast()

employees_cast_df=employees_df.withColumn(
    "salary",
    col("salary").cast("double")
)

employees_cast_df.printSchema()

# COMMAND ----------

# string functions

#upper()

employees_df.select(
    "name",
    upper(col("name")).alias("upper_name")
).show()

# COMMAND ----------

# lower() 

employees_df.select(
    "name",
    lower(col("name")).alias("lower_name")
).show()

# COMMAND ----------

# trim(), ltrim(), rtrim()

employees_df.select(
    "name",
    trim(col("name")).alias("clean_name")
).show()

# COMMAND ----------

# combining trim(), lower(), initcap()

employees_df.select(
    "name",
    initcap(
        lower(
            trim(col("name"))
        )
    ).alias("clean_name")
).show()

# COMMAND ----------

# length()

employees_df.select(
    trim(col("name")).alias("name"),
    length(trim(col("name"))).alias("name_length")
).show()

# COMMAND ----------

# concat()

employees_df.withColumn(
    "combined",
    concat(col("employee_id"), col("department"))
).show()

# COMMAND ----------

# concat_ws()

employees_df.withColumn(
    "employee_department",
    concat_ws(
        "-",
        col("employee_id"),
        col("department")
        )
).show()

# COMMAND ----------

# substring()

orders_df.withColumn(
    "order_date",
    substring(col("order_date"),1,4).alias("year")
).show()

# COMMAND ----------

# split()

orders_df.withColumn(
    "order_date",
    split(col("order_date"),"-")
).show()

# COMMAND ----------

# contains()

orders_df.filter(
    col("product").contains("top")
).show()

# COMMAND ----------

# startswith()

orders_df.filter(
    col("product").startswith("M")
).show()

# COMMAND ----------

# endswith()

orders_df.filter(
    col("product").endswith("top")
).show()

# COMMAND ----------

# regexp_replace()

employees_df.withColumn(
    "name",
    regexp_replace(
        trim(col("name")),
        " ",
        ""
    )
).show()

# COMMAND ----------

# regexp_extract()

orders_df.withColumn(
    "order_date",
    regexp_extract(
        col("order_date"),
        r"(\d{4})",
        1
    )
).show()

# 1 means return the first capturing group
# 2 means return the second capturing group
# 0 means return the entire match

# COMMAND ----------

employees_df=employees_df.withColumn(
    "salary",
    col("salary").cast("double")
)

# COMMAND ----------

# DBTITLE 1,addition
# addition

employees_df.withColumn(
    "salary",
    (col("salary")+5000)
).show()

# COMMAND ----------

# round()

employees_df.select(
    "salary",
    round(col("salary"), 0)
).show()

# COMMAND ----------

df1=spark.createDataFrame(
    [
        (101,-5000),
        (102,2000),
        (103,-1000)
    ],
    ["employee_id","salary"]
)

df1.withColumn(
    "salary",
    abs(col("salary"))
).show()

# COMMAND ----------

# ceil()

employees_df.withColumn(
    "salary",
    ceil(col("salary"))
).show()

# COMMAND ----------

# floor()

employees_df.withColumn(
    "salary",
    floor(col("salary"))
).show()

# COMMAND ----------

# pow()

employees_df.withColumn(
    "salary",
    pow(col("salary"),2)
).show()

# COMMAND ----------

# sqrt()

employees_df.withColumn(
    "salary",
    sqrt(col("salary"))
).show()

# COMMAND ----------

# greatest()

sales_df = spark.createDataFrame(
    [
        (101, 10000, 15000, 12000),
        (102, 8000, 7000, 9000),
        (103, 20000, 18000, 22000)
    ],
    ["employee_id", "jan_sales", "feb_sales", "mar_sales"]
)

sales_df.withColumn(
    "highest_sales",
    greatest(
        col("jan_sales"),
        col("feb_sales"),
        col("mar_sales")
    )
).show()

# least

sales_df.withColumn(
    "lowest_sales",
    least(
        col("jan_sales"),
        col("feb_sales"),
        col("mar_sales")
    )
).show()

# COMMAND ----------

# modulo

employees_df.filter(
    col("employee_id")%2==0
).show()

# COMMAND ----------

# log()

employees_df.withColumn(
    "salary",
    log(col("salary"))
).show()

# COMMAND ----------

# to_date()

employees_df.withColumn(
    "joining_date",
    to_date(col("joining_date"), "yyyy-MM-dd")
).show()

# COMMAND ----------

# to_timestamp()

orders_df.withColumn(
    "created_at",
    to_timestamp(
        col("created_at"), "yyyy-MM-dd HH:mm:ss"
    )
).show()

# COMMAND ----------

# current_date()

employees_df.select(
    "employee_id",
    current_date().alias("today")
).show()

# COMMAND ----------

# current_timestamp()

employees_df.withColumn(
    "processing_time",
    current_timestamp()
).show()

# COMMAND ----------

# year()

employees_df.select(
    "employee_id",
    year(col("joining_date")).alias("joining_year")
).show()

# COMMAND ----------

# month()

employees_df.select(
    "employee_id",
    month(col("joining_date")).alias("joining_month")
).show()

# COMMAND ----------

# dayofmonth()

employees_df.withColumn(
    "day",
    dayofmonth(col("joining_date"))
).show()

# COMMAND ----------

# dayofweek()

# 1	Sunday
# 2	Monday
# 3	Tuesday
# 4	Wednesday
# 5	Thursday
# 6	Friday
# 7	Saturday

employees_df.withColumn(
    "weekday_number",
    dayofweek(col("joining_date"))
).show()

# COMMAND ----------

# date_format()- MMMM

employees_df.select(
    "joining_date",
    date_format(col("joining_date"), "MMMM").alias("month_name")
).show()

# COMMAND ----------

# date_format()- EEEE

employees_df.select(
    "joining_date",
    date_format(col("joining_date"), "EEEE").alias("month_name")
).show()

# COMMAND ----------

# date_format()- yyyy-MM

employees_df.select(
    "joining_date",
    date_format(col("joining_date"), "yyyy-MM").alias("month_name")
).show()

# COMMAND ----------

# datediff(end_date, start_date)
# Finds the difference between two dates

# lit() allows you to insert a fixed value into a DataFrame

employees_df.select(
    "employee_id", "joining_date",
    datediff(
        to_date(lit("2026-10-23")),
        col("joining_date")
    ).alias("days_since_joining")
).show()

# COMMAND ----------

# date_add()

orders_df.select(
    "order_date",
    date_add(col("order_date"), 30).alias("after_30_days")
).show()

# COMMAND ----------

# date_sub()
# Subtracts a specified number of days from a date

orders_df.select(
    "order_date",
    date_sub(col("order_date"), 7).alias("seven_days_before")
).show()

# COMMAND ----------

# add_months()

employees_df.select(
    "joining_date",
    add_months(col("joining_date"), 6).alias("six_months_later")
).show()

# COMMAND ----------

# months_between()

employees_df.select(
    "employee_id",
    "joining_date",
    months_between(
        to_date(lit("2026-10-23")),
        col("joining_date")
    ).alias("tenure_month")
).show()

# COMMAND ----------

# late_date()
# returns the final calendar date of the month

orders_df.select(
    "order_date",
    last_day(
        col("order_date")
    ).alias("month_end")
).show()

# COMMAND ----------

# lit()
# means literal value

employees_df.withColumn(
    "country",
    lit("India")
).show()

# COMMAND ----------

employees_df.withColumn(
    "bonus_percent",
    lit(10)
).show()

# COMMAND ----------

# coalesce()

employees_df.select(
    "employee_id",
    "department",
    coalesce(
        col("department"),
        lit("unknown")
    ).alias("department_clean")
).show()

# COMMAND ----------

# isNull()

employees_df.filter(
    col("department").isNull()
).show()

# COMMAND ----------

# iNotNull()

employees_df.filter(
    col("department").isNotNull()
).show()

# COMMAND ----------

# mixed transformation example 

clean_df=(employees_df
        .withColumn(
            "name",
            initcap(lower(trim(col("name"))))
        )
        .withColumn(
            "salary",
            col("salary").cast("double")
        )
        .withColumn(
            "department",
            coalesce(
                col("department"),
                lit("Unknown")
            )
        )
)

clean_df.show()