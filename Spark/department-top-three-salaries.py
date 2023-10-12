# https://leetcode.com/problems/department-top-three-salaries
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
dept=spark.read.csv("SampleData/department-top-three-salaries-dept.csv",header=True)
emp=spark.read.csv("SampleData/department-top-three-salaries-employee.csv",header=True)
dept.createOrReplaceTempView("department")
emp.createOrReplaceTempView("employee")
spark.sql("""
select department,name as employee,salary from (
select d.name as department,e.name,e.salary,e.departmentId, dense_rank() over(partition by e.departmentId order by e.salary desc) as rnk
from employee e
join department d
on e.departmentId = d.id) x
where x.rnk in (1,2,3) """).show()

emp.withColumn('rnk',F.dense_rank().over(Window.partitionBy("departmentId").orderBy(F.desc("salary")))) \
.filter("rnk in (1,2,3) ") \
.join(dept,emp.departmentId == dept.id) \
.select(dept.name.alias("department"),emp.name.alias("employee"),emp.salary).show()
#close spark
