import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
df=spark.read.csv("SampleData/sales-data.csv",header=True)
df.createOrReplaceTempView("sales_data")
df.show()
spark.sql(
    """
 SELECT
     employee_id,
     sales_quarter,
     sales,
     LEAD(sales) OVER ( ORDER BY sales_quarter) AS next_quarter_sales,
     LEAD(sales) OVER (PARTITION BY employee_id ORDER BY sales_quarter) - sales AS diff
 FROM
     sales_data
"""
).show()
tdf=df.withColumn("next_quarter_sales",F.lead(df.sales).over(Window.partitionBy(df.employee_id).orderBy(df.sales_quarter)))
tdf.withColumn("diff",(tdf.next_quarter_sales - tdf.sales)).show()
#close spark
spark.sparkContext._gateway.close()
spark.stop()