import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Delete Emails").getOrCreate()
#close spark
# https://www.codingninjas.com/studio/problems/delete-duplicate-emails_2111947
df=spark.read.csv("SampleData/delete-duplicate-emails.csv",header=True)
df.createOrReplaceTempView("Person")
dup_records=spark.sql("""
select * from Person
where id not in ( select min(id) from Person group by email)       
""")
dup_records.show()
spark.sparkContext._gateway.close()
spark.stop()
