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
    df.createOrReplaceTempView("salarydf")

    result_df = spark.sql("""
        with windowed as (
        select Id,Company,Salary,
        row_number() over (partition by Company order by Salary) as rn,
        count(*) over (partition by Company) as cnt
        from salarydf
    ),
    windowed_count as (
        select Id,Company,Salary,rn,cnt,cnt/2 as min_cnt,cnt/2+1 as max_cnt
        from windowed
    )
    select 
    Id,Company,Salary 
    from windowed_count
    where rn between min_cnt and max_cnt
    """)
    result_df.show()
    return result_df

def transform_with_pyspark(df):
    tdf = df \
    .withColumn("rn",F.row_number().over(Window.partitionBy("Company").orderBy("Salary"))) \
    .withColumn("cnt",F.count("Id").over(Window.partitionBy("Company"))) \
    .withColumn("min_cnt",F.col("cnt")/2) \
    .withColumn("max_cnt",F.col("cnt")/2+1) \
    .filter(F.col("rn").between(F.col("min_cnt"),F.col("max_cnt"))) \
    .select("Id","Company","Salary")
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