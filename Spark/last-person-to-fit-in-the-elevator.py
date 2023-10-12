import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
df=spark.read.csv("SampleData/last-person-to-fit-in-the-elevator.csv",header=True)
df.createOrReplaceTempView("queue")
# The maximum weight the elevator can hold is 1000.
# Write an SQL query to find the person_name of the last person who will fit in the elevator without exceeding the weight limit.
# It is guaranteed that the person who is first in the queue can fit in the elevator.
df.show()
#approach: add a new column running sum
df2=spark.sql("""
select a.turn,a.weight,a.person_name,sum(b.weight)
    from queue a join queue b 
    on a.turn >= b.turn
    group by a.turn,a.weight,a.person_name
    having sum(b.weight) <= 1000
    order by a.turn desc
    limit 1
""")
df2.show()
df2.select("person_name").show()
df.alias("a").join(df.alias("b"),F.col("a.turn") >= F.col("b.turn"))\
    .groupBy(F.col("a.turn"),F.col("a.weight"),F.col("a.person_name")) \
    .agg(F.sum(F.col("b.weight")).alias("SumOfWeights")) \
    .select("a.person_name")\
    .filter(F.col("SumOfWeights") <= 1000 ) \
    .orderBy("SumOfWeights", ascending=False) \
    .limit(1) \
    .show()
#close spark
