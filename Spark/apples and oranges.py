# https://www.codingninjas.com/studio/problems/apples-oranges_2122060

import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
df=spark.read.csv("SampleData/apple-and-oranges.csv",header=True)
df.show()
df.createOrReplaceTempView("sales")
#way 1:
spark.sql("""
select a.sale_date,
case when (a.fruit='apples' and b.fruit='oranges')  
then (a.sold_num-b.sold_num) end as diff
from sales a
join sales b
on a.sale_date=b.sale_date and a.fruit!=b.fruit and a.fruit='apples'
""").show()

df1=df.filter("fruit ='apples'").alias("a")
df2=df.filter("fruit ='oranges'").alias("b")
# df3=df1.join(df2,(F.col("a.sale_date")==F.col("b.sale_date"))).withColumn("diff",F.col("a.sold_num")-F.col("b.sold_num"))\
# .select("a.sale_date","diff")
df3=df.filter("fruit ='apples'").alias("a").join(df.filter("fruit ='oranges'").alias("b"),(F.col("a.sale_date")==F.col("b.sale_date"))).withColumn("diff",F.col("a.sold_num")-F.col("b.sold_num"))\
.select("a.sale_date","diff")
df3.show()

# Way 2:
spark.sql(
"""
with x as (select *, sold_num - lead(sold_num,1) over(partition by sale_date order by sale_date) as diff
from sales)
select sale_date,diff from x
where fruit = 'apples'
"""
).show()

#Way 2 Pyspark:
df.withColumn("diff",F.col("sold_num") - F.lead("sold_num",1).over(Window.partitionBy("sale_date").orderBy('sale_date'))).filter(" fruit = 'apples' ").select("sale_date","diff").show()
#close spark
# spark.sparkContext._gateway.close()
# spark.stop()