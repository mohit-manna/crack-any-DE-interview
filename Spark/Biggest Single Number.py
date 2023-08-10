import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
#close spark
# https://www.codingninjas.com/studio/problems/biggest-single-number_2111955
df=spark.read.csv("SampleData/biggest-single-number.csv",header=True)
df.createOrReplaceTempView("my_numbers")
spark.sql("""
select num from my_numbers
          group by num
          having count(*)=1
          order by num desc 
          limit 1
          """).show()
df.groupBy("num").count().filter("count == 1 ").orderBy("num", ascending=False).select("num").limit(1).show()
spark.sparkContext._gateway.close()
spark.stop()