import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
df=spark.read.csv("SampleData/merkel_orders_data.csv",header=True)
df.createOrReplaceTempView("orders")
df.show()
spark.sql(
"""
select 
    product_id,
    date_format(order_date,'yyyy-MM'),
    avg(order_id) as order_avg,
    sum(order_id)/count(order_id) as avg_order
from orders
group by 1,2
"""
).show()
df.select("product_id",\
          F.date_format("order_date",'yyyy-MM').alias("order_month"),\
            F.col("order_id").cast("int"))\
    .groupBy("product_id","order_month")\
    .avg("order_id").alias("order_avg")\
    .show()
#close spark
spark.sparkContext._gateway.close()
spark.stop()