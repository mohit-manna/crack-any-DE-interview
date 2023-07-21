import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[5]").appName("Manna").getOrCreate()
# read
df=spark.read.csv("SampleData/exl-employee.csv",header=True)
df = df.repartition(2)
# filter
df2 = df.filter(F.col("dept")!=F.lit('HR')).select("dept",F.col("salary").cast(IntegerType()).alias("salary"))
# group By
df3 = df2.groupBy("dept").sum("salary")
# Write
df3.write.mode("overwrite").csv("output/exl-dept-wise-total-salary")
#close spark
input("Press Enter to stop")
spark.sparkContext._gateway.close()
spark.stop()