import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()

# Problem Statement: We have old data and New data. We need to comapre each row and 
# then add one new column delta_value for each row which will have 
# I : Insert, NC : No change, U : Update 
# and so on
old_df=spark.read.csv("SampleData/walmart-old-data.csv",header=True)
delta_df=spark.read.csv("SampleData/walmart-delta-data.csv",header=True)
old_df=old_df.withColumn('hash_id',F.md5(F.concat_ws('|',F.coalesce('name',F.lit('')),F.coalesce('city',F.lit('')),F.coalesce('address',F.lit('')))))
old_df.show()
delta_df=delta_df.withColumn('hash_id',F.md5(F.concat_ws('|',F.coalesce('name',F.lit('')),F.coalesce('city',F.lit('')),F.coalesce('address',F.lit('')))))
delta_df.show()
df=old_df.join(delta_df,old_df.id == delta_df.id,'outer') \
.withColumn("delta_value",
            F.when(old_df.hash_id != delta_df.hash_id , F.lit('U'))
            .when(old_df.hash_id == delta_df.hash_id , F.lit('NC'))
            .when(old_df.hash_id.isNull(),F.lit('I'))
            .when(delta_df.hash_id.isNull(),F.lit('NC'))
            .otherwise(F.lit('Error')))
df.show()
df.select(
    F.coalesce(delta_df.id,old_df.id).alias("id"),
    F.coalesce(delta_df.name,old_df.name).alias("name"),
    F.coalesce(delta_df.address,old_df.address).alias("address"),
    F.coalesce(delta_df.city,old_df.city).alias("city"),
    "delta_value"
        ).show()
#close spark
spark.sparkContext._gateway.close()
spark.stop()