import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
df=spark.read.csv("SampleData/self-inner-join.csv",header=True)
df.show()
df.createOrReplaceTempView("tbl")
spark.sql(
"""
select x.A,y.B
from tbl x inner join tbl y
on x.A=y.B
"""
).show()
df.alias('x').join(
	df.alias('y'),
	F.col('x.A') == F.col('y.B')
).select(F.col('x.A'), F.col('y.B')).show()
