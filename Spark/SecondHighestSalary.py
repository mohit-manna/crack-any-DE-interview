import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


# Problem: https://leetcode.com/problems/second-highest-salary/

employee=spark.read.csv("SampleData/employee.csv",header=True,sep=",")
employee.show()

#sql way
employee.createOrReplaceTempView("employee")
spark.sql("""
with x as (
select salary,dense_rank() over(order by salary desc) as rn from employee
)
select max(salary) as SecondHighestSalary from x where rn=2
          """).show()

# using pyspark functions
df=employee.withColumn("rn",F.dense_rank().over(Window.orderBy(F.desc("salary"))))
second_high_sal_df=df.filter(F.col("rn")==2).select(F.col("salary"))
second_high_sal_df.agg(F.max(F.col("salary")).alias("SecondHighestSalary")).show()
# close spark
spark.sparkContext._gateway.close()
spark.stop()