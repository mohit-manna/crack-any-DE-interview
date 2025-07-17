import findspark
findspark.init()
import os
import pytest
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def get_spark():
    return SparkSession.builder.master("local[*]").appName("Pytest-PySpark").getOrCreate()

def read_csv(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def transform_with_sql(spark, df):
    df.createOrReplaceTempView("freq_table")

    result_df = spark.sql("""
        with summed_freq as 
        (
            select Number,Frequency,sum(frequency) over(order by Number) as freq_cnt            
            from freq_table
        ),
        mid_freq as (
            select sum(Frequency)/2 as min_freq, (sum(Frequency)/2)+1 as max_freq 
            from freq_table
        )
        select avg(s.Number) as `median` from summed_freq s,mid_freq m
        where s.freq_cnt = m.min_freq or s.freq_cnt = m.max_freq
    """)
    result_df.show()
    return result_df

def transform_with_pyspark(df):
    total_freq = df.agg(F.sum("Frequency").alias("total")).collect()[0]["total"]
    min_freq = total_freq / 2
    max_freq = min_freq + 1

    tdf = df.withColumn("freq_cnt", F.sum("Frequency").over(Window.orderBy(F.col("Number").asc()))) \
        .filter((F.col("freq_cnt") == F.lit(min_freq)) | (F.col("freq_cnt") == F.lit(max_freq)))
    result_df = tdf.agg(F.avg(F.col("Number")).alias("median"))
    result_df.show()
    return result_df

def test_transformations(spark):
    current_file = os.path.splitext(os.path.basename(__file__))[0]
    input_file = f"SampleData/csv_files/{current_file}/input.csv"
    output_file = f"SampleData/csv_files/{current_file}/output.csv"

    input_df = read_csv(spark, input_file)
    expected_output_df = read_csv(spark, output_file)

    sql_result_df = transform_with_sql(spark, input_df)
    pyspark_result_df = transform_with_pyspark(input_df)
    try:
        sort_columns = expected_output_df.columns
        expected_output_df = expected_output_df.orderBy(*sort_columns)
        sql_result_df = sql_result_df.orderBy(*sort_columns)
        pyspark_result_df = pyspark_result_df.orderBy(*sort_columns)
    except Exception as e:
        print(e)
        pass

    # Assert that the SQL transformation matches the expected output
    assert sql_result_df.collect() == expected_output_df.collect()

    # Assert that the PySpark transformation matches the expected output
    assert pyspark_result_df.collect() == expected_output_df.collect()

if __name__=="__main__":
    test_transformations(get_spark())