# EXL

## First Round Interview Questions

### Actual Interview Questions

1. Explain your project

2. What is Map Reduce?

3. What is Spark?

4. What is RDD?

5. How big is your data?

5. SQL Question and Do the same in Python

Question: Find details of employee where average departmental salary is greater than 1 Million
```
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
print('PySpark Version :'+spark.version)
#close spark
df=spark.read.csv("SampleData/exl-employee.csv",inferSchema =True,header=True)
df.createOrReplaceTempView("employee")
# Way 1
spark.sql("""
with d as (
    select dept,avg(salary) as avg_salary from employee 
    group by dept
)
select * from employee e where exists( select 1 from d where e.dept=d.dept and d.avg_salary > 1000000 )
          """).show()
# Way 2
df.groupBy(F.col("dept")).agg(F.avg(F.col("salary"))).show() #can't be added in withColumn
#My Ans(Wrong):  df2=df.withColumn("avg_salary",F.agg(avg(F.col("salary")).groupBy(F.col("dept"))))
df2=df.withColumn("avg_salary",F.avg('salary').over(Window.partitionBy(F.col('dept'))).alias('avg_salary'))
df2.filter(F.col("avg_salary")>1000000).select("emp_id","emp_name","salary").show()
# emp_id,emp_name,salary,dept,avg_salary

```
6. SQL Question 

Question: Find the id and name of employees where 

```
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
print('PySpark Version :'+spark.version)
#close spark
df=spark.read.csv("SampleData/exl-employee.csv",inferSchema =True,header=True)
df.createOrReplaceTempView("employee")
# Way 1
spark.sql("""
select distinct a.emp_id,a.emp_name 
          from employee a
          join employee b
          on a.emp_id=b.emp_id
          and a.emp_name!=b.emp_name
          """).show()
# Way 2
spark.sql("""
select distinct a.emp_id,a.emp_name 
          from employee a where exists(
          select 1 from employee b
          where a.emp_id=b.emp_id
          and a.emp_name!=b.emp_name)
          """).show()

# Pyspark Way
df2=df
df.alias('a').join(df2.alias('b'),[df.emp_id == df2.emp_id,df.emp_name != df2.emp_name] ).select("a.emp_id","a.emp_name").distinct().show()

```

## Second Round Interview Questions