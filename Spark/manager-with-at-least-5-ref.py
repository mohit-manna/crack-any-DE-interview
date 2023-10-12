import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)

# https://leetcode.com/problems/managers-with-at-least-5-direct-reports

df=spark.read.csv("SampleData/manager-with-at-least-5-ref.csv",header=True)
df.createOrReplaceTempView("employee")
spark.sql("""
with x as(
select managerId as id
from Employee
group by managerId 
having count(*)>=5
)
select name from Employee where id in (select id from x)
""").show()
mdf=df.groupBy("managerId").count().filter("count >= 5").select("managerId")
df.join(mdf,df.id==mdf.managerId).select("name").show()
#close spark
