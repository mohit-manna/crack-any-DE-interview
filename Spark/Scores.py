import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("mohit-manna").getOrCreate()
print('PySpark Version :'+spark.version)


# Problem: https://www.codingninjas.com/studio/problems/running-total-for-different-genders_2188769

df=spark.read.csv("SampleData/scores.csv",header=True,sep=",")
df.show()

#sql way
df.createOrReplaceTempView("scores")
spark.sql("""
select gender,day,sum(score_points) over(partition by gender order by day) as total from scores s
order by gender
          """).show()

# using pyspark functions
tdf=df.withColumn('total',F.sum('score_points').over(Window.partitionBy(F.col('gender')).orderBy(F.col('day'))))
tdf.select('gender','day','total').show()
# close spark
