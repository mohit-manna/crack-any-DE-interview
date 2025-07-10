import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
dept=spark.read.csv("SampleData/department-top-three-salaries-employee.csv",header=True)
dept.createOrReplaceTempView("employee")
spark.sql("""
with windowed as (
    select id,name,salary,departmentId,
           row_number() over (partition by departmentId order by salary desc) as row_num,
           rank() over (partition by departmentId order by salary desc) as rank_num,
           dense_rank() over (partition by departmentId order by salary desc) as dense_rank_num
    from employee
)
select id,name,salary,departmentId, row_num, rank_num, dense_rank_num
from windowed
 """).show()