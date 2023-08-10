import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
df=spark.read.csv("SampleData",header=True)
#close spark
spark.sparkContext._gateway.close()
spark.stop()