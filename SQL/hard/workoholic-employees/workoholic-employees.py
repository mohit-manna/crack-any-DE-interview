import findspark
findspark.init()
import os
import pytest
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import glob
from functools import reduce
from pyspark.sql import DataFrame
import re

def get_spark():
    return SparkSession.builder.master("local[*]").appName("Pytest-PySpark").getOrCreate()

def read_csv(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform_with_sql(spark, dfs):
    current_file = os.path.splitext(os.path.basename(__file__))[0]
    input_files = sorted(glob.glob(f"SQL/hard/{current_file}/input-*.csv"))
    for file, df in zip(input_files, dfs):
        match = re.search(r'input-(.*)\.csv', os.path.basename(file))
        if match:
            view_name = match.group(1)
            df.createOrReplaceTempView(view_name)

    result_df = spark.sql("""
        with hours_worked as (
            select emp_id,login,logout,cast((unix_timestamp(logout) - unix_timestamp(login))/3600 as int) as hours,
            weekofyear(login) as week
            from employees
        ),
        x as (
            select emp_id,week,
            sum(case 
                when (hours >=8 and hours <10)then 1
                else 0
            end) as 8_hour_shift,
            sum(case 
                when (hours >=10 and hours <12) then 1
                else 0
            end) as 10_hour_shift
            from hours_worked
            group by emp_id,week
        )
        select emp_id,
        case when (8_hour_shift > 0 and 10_hour_shift = 0) then 1
             when (8_hour_shift = 0 and 10_hour_shift > 0) then 2
             when (8_hour_shift > 0 and 10_hour_shift > 0 ) then 'both'
             else 0
        end as criterian
        from x
    """)
    result_df.show()
    return result_df

def transform_with_pyspark(dfs):
    employee_df = dfs[0]
    emp_df = employee_df\
        .withColumn("hours", ((F.unix_timestamp(F.col("logout")) - F.unix_timestamp(F.col("login"))) / 3600).cast("int"))\
        .withColumn("week",F.weekofyear(employee_df.login))\
    
    emp_df_agg = emp_df.groupBy("emp_id","week")\
        .agg(
            F.sum(F.when(((F.col("hours") > 8) & (F.col("hours")<10)),1).otherwise(0)).alias("8_hour_shifts"),\
            F.sum(F.when((F.col("hours")>= 10 ),1).otherwise(0)).alias("10_hour_shifts")
        )\
        .withColumn("criterian",
            F.when((F.col("8_hour_shifts") > 0) & (F.col("10_hour_shifts") == 0), "1")
            .when((F.col("8_hour_shifts") == 0) & (F.col("10_hour_shifts") > 0), "2")
            .when((F.col("8_hour_shifts") > 0) & (F.col("10_hour_shifts") > 0), "both")
            .otherwise("0")
        )\
        .select("emp_id", "criterian")
    emp_df_agg.show()
    return emp_df_agg


def compare_dataframes(expected_df, actual_df):
    expected_cols = set(expected_df.columns)
    actual_cols = set(actual_df.columns)
    if expected_cols != actual_cols:
        print(f"Column mismatch: expected {expected_cols}, actual {actual_cols}")
        return False

    sort_columns = list(expected_df.columns)
    expected_sorted = expected_df.orderBy(*sort_columns)
    actual_sorted = actual_df.orderBy(*sort_columns)

    expected_rows = expected_sorted.collect()
    actual_rows = actual_sorted.collect()

    if len(expected_rows) != len(actual_rows):
        print(f"Row count mismatch: expected {len(expected_rows)}, actual {len(actual_rows)}")
        return False

    for idx, (exp_row, act_row) in enumerate(zip(expected_rows, actual_rows)):
        for col in sort_columns:
            if exp_row[col] != act_row[col]:
                print(f"Mismatch at row {idx}, column '{col}': expected {exp_row[col]}, actual {act_row[col]}")
                return False
    return True

def test_transformations(spark):
    current_file = os.path.splitext(os.path.basename(__file__))[0]
    input_files = sorted(glob.glob(f"SQL/hard/{current_file}/input-*.csv"))
    output_file = f"SQL/hard/{current_file}/output.csv"

    input_dfs = [read_csv(spark, file) for file in input_files]
    expected_output_df = read_csv(spark, output_file)

    sql_result_df = transform_with_sql(spark, input_dfs)
    pyspark_result_df = transform_with_pyspark(input_dfs)
    try:
        sort_columns = expected_output_df.columns
        expected_output_df = expected_output_df.orderBy(*sort_columns)
        sql_result_df = sql_result_df.orderBy(*sort_columns)
        pyspark_result_df = pyspark_result_df.orderBy(*sort_columns)
    except Exception as e:
        print(e)
        pass

    # Assert that the SQL transformation matches the expected output
    assert compare_dataframes(expected_output_df,sql_result_df)

    # Assert that the PySpark transformation matches the expected output
    assert compare_dataframes(expected_output_df,pyspark_result_df)

if __name__=="__main__":
    test_transformations(get_spark())