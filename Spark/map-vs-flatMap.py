import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x:  x * 2) # gives same number of output as input dataset
# result contains [2, 4, 6, 8, 10]
print(result.collect())

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
result = rdd.flatMap(lambda x: (x, x * 2)) # one-to-many mapping, variable output for each input
# result contains [1, 2, 2, 4, 3, 6, 4, 8, 5, 10]
print(result.collect())
