import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
df=spark.read.csv("SampleData/a1.csv",header=True)
df1=spark.read.csv("SampleData/b1.csv",header=True)
df.show()
df1.show()
print("inner",df.join(df1,df.A1 == df1.B1,"inner").count())
print("left",df.join(df1,df.A1 == df1.B1,"left").count()) # I completely forgot the logic of left join I answered 12 thinking NULLs will be counted
print("right",df.join(df1,df.A1 == df1.B1,"right").count())
print("outer",df.join(df1,df.A1 == df1.B1,"outer").count())
print("left outer",df.join(df1,df.A1 == df1.B1,"leftouter").count())
print("right outer",df.join(df1,df.A1 == df1.B1,"rightouter").count())
print("left anti",df.join(df1,df.A1 == df1.B1,"leftanti").count())
print("left semi",df.join(df1,df.A1 == df1.B1,"leftsemi").count())
#close spark
spark.sparkContext._gateway.close()
spark.stop()