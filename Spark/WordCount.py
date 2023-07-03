import findspark
findspark.init()
import pyspark 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# Way 1: Using DataFrame
text=spark.read.text("SampleData/wordcount.txt")
df = text.filter(F.col("value") != "") # remove empty rows
wdf= df.withColumn("word",F.explode(F.split(F.col("value"),"\s+"))) #add new column by exploding lines
wdf2=wdf.withColumn("word",F.regexp_replace("word","[^\w]","")) #replace all except words by empty char
word_count_df=wdf2.groupBy("word").count().sort("count",ascending=False)
word_count_df.show()

# What is the total number of words?
word_count_df.agg(F.sum("count").alias("total_words")).show()

# How many times word fiction has occured in the test?
word="fiction"
word_count_df.filter(F.lower(F.col("word")).rlike(word)).agg(F.sum("count").alias(f"{word}_count")).show()

#close spark
spark.sparkContext._gateway.close()
spark.stop()