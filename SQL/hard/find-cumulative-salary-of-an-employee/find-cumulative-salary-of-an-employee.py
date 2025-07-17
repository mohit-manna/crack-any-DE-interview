import findspark
findspark.init()
import os
import pytest
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def get_spark():
    return SparkSession.builder.master("local[2]").appName("Pytest-PySpark").getOrCreate()

def read_csv(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform_with_sql(spark, df):
    df.createOrReplaceTempView("employee")

    result_df = spark.sql("""
        with month_rn as
        (
            select 
                Id,Month,Salary,
                row_number() over(partition by Id order by Month desc) as rn
            from employee
        ),
        cum_sum as (
            select 
                Id,Month,Salary,rn,
                sum(Salary) over(partition by Id order by Month asc) as Sal_Sum
            from month_rn where rn!=1
        )
        select Id,Month,Sal_Sum as Salary
        from cum_sum order by Id asc, Month desc
    """)
    # result_df.show()
    return result_df

def transform_with_pyspark(df):
    tdf = df \
        .withColumn("rn", F.row_number().over(Window.partitionBy("Id").orderBy(F.col("Month").desc()))) \
        .filter(F.col("rn") != 1) \
        .withColumn("sal_sum", F.sum("Salary").over(Window.partitionBy("Id").orderBy(F.col("Month").asc()))) \
        .select("Id", "Month", F.col("sal_sum").alias("Salary")) \
        .orderBy(F.col("Id").asc(), F.col("Month").desc())
    tdf.show()
    result_df = tdf
    return result_df

def test_transformations(spark):
    current_file = os.path.splitext(os.path.basename(__file__))[0]
    input_file = f"SQL/hard/{current_file}/input.csv"
    output_file = f"SQL/hard/{current_file}/output.csv"

    input_df = read_csv(spark, input_file)
    expected_output_df = read_csv(spark, output_file)

    sql_result_df = transform_with_sql(spark, input_df)

    pyspark_result_df = transform_with_pyspark(input_df)
    sort_columns = expected_output_df.columns
    expected_output_df = expected_output_df.orderBy(*sort_columns)
    sql_result_df = sql_result_df.orderBy(*sort_columns)
    pyspark_result_df = pyspark_result_df.orderBy(*sort_columns)

    # Assert that the SQL transformation matches the expected output
    assert sql_result_df.collect() == expected_output_df.collect()

    # Assert that the PySpark transformation matches the expected output
    assert pyspark_result_df.collect() == expected_output_df.collect()

if __name__=="__main__":
    test_transformations(get_spark())