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
        with company_monthly_avg as (
            select substr(pay_date,1,7) as mnth,avg(amount) as avg_sal
            from salary 
            group by 1
        ),
        dept_mnthly_avg as (
            select department_id,substr(pay_date,1,7) as mnth,avg(amount) as avg_sal
            from employee e left join salary s
            on e.employee_id = s.employee_id
            group by all
        )
        select cast(d.mnth as timestamp) as pay_month,department_id,
        case 
            when (c.avg_sal > d.avg_sal) then 'lower'
            when (c.avg_sal = d.avg_sal) then 'same'
            else 'higher'
        end as comparison
        from dept_mnthly_avg  d left join company_monthly_avg c
        on d.mnth = c.mnth
        order by 1 desc,2 asc
    """)
    result_df.show()
    return result_df

def transform_with_pyspark(dfs):
    emp_df = dfs[0]
    salary_df = dfs[1]
    cmp_mnth_avg = salary_df.groupBy(F.substring("pay_date", 1, 7).alias("mnth")) \
        .agg(F.avg("amount").alias("avg_sal")) 
    cmp_mnth_avg.show()
    dept_mnth_avg = emp_df.join(salary_df, emp_df.employee_id == salary_df.employee_id, "left") \
        .groupBy("department_id", F.substring("pay_date", 1, 7).alias("mnth")) \
        .agg(F.avg("amount").alias("avg_sal"))
    dept_mnth_avg.show()
    res_df = dept_mnth_avg.join(cmp_mnth_avg,dept_mnth_avg.mnth == cmp_mnth_avg.mnth, "left") \
        .withColumn("comparison",
            F.when(dept_mnth_avg.avg_sal > cmp_mnth_avg.avg_sal, "higher") \
            .when(dept_mnth_avg.avg_sal < cmp_mnth_avg.avg_sal, "lower") \
            .otherwise("same")) \
        .withColumn("pay_month", dept_mnth_avg.mnth.cast("timestamp")) \
        .select("pay_month", "department_id", "comparison")
    res_df.show()
    return res_df


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