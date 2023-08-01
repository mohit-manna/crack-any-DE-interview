import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
df=spark.read.csv("SampleData/a1.csv",header=True)
df1=spark.read.csv("SampleData/b1.csv",header=True)
print(df.join(df1,df.A1 == df1.B1,"inner").count())
print(df.join(df1,df.A1 == df1.B1,"left").count()) # I completely forgot the logic of left join I answered 12 thinking NULLs will be counted
print(df.join(df1,df.A1 == df1.B1,"right").count())
#close spark
spark.sparkContext._gateway.close()
spark.stop()