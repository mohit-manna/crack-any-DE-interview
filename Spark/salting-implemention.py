import findspark
from random import random 
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("Manna")\
    .config("spark.sql.shuffle.partitions", 3)\
    .config("spark.sql.adaptive.enable", "false")\
    .getOrCreate()
print('PySpark Version :'+spark.version)

# big table
id_values = [1] * 10000000 + [2] * 5 + [3] * 5 #ultra max pro skewed data 1 lakh 1s, five 2s and five 3s
table1 = spark.createDataFrame(id_values,"int").toDF("id")
# table1.show(5)
# small table
id_values = [1] * 100 + [2] * 5 + [3] * 2 #skewed data of 100 1s, five 2s and two 3s
table2 = spark.createDataFrame(id_values,"int").toDF("id")
# table2.show(5)
# join without salting
data = table1.join(table2,["id"],"inner")
print(data.count())
input("Press any key to stop")

# add salt to table 1
df_with_random = table1.withColumn("random_num",(F.rand() * 10 + 1).cast("int"))
table1 = df_with_random.withColumn("salted_key",F.concat(F.col("id"),F.lit("-"),F.col("random_num")))
# table1.show(5)

# add salt to table 2
table2_replicated = table2.withColumn("sequence",F.array([F.lit(i) for i in range(1,11)]))
table2 = table2_replicated.withColumn("exploded_col",F.explode("sequence"))\
                            .withColumn("salted_key",F.concat(F.col("id"),F.lit("-"),F.col("exploded_col")))\
                            .drop("exploded_col","sequence")
# table2.show(5)
data = table1.join(table2,["salted_key"],"inner")
print("Count: ",data.count())
input("Press any key to stop")