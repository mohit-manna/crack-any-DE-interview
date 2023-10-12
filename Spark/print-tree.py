import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.master('local[*]').appName("manna").getOrCreate()
print('PySpark Version :'+spark.version)

df = spark.read.csv("SampleData/print-tree.csv",header = True)
df.createOrReplaceTempView("nodes")

spark.sql(
    """
select node,
case 
    when a.parent is null 
        then 'Root'
    when a.parent > a.node and b.parent is not null
        then 'Inner'
    else
        'Leaf'
    end as type
from nodes a left join  (select distinct parent from nodes) b
on a.node = b.parent
order by node desc
"""
).show()

a=df.alias('a')
b=df.select('parent').distinct().alias('b')
a.join(b, a.node == b.parent,'left')\
    .withColumn("type",\
                 F.when(a.parent.isNull(),'Root')\
                 .when((a.parent > a.node) & F.col("b.parent").isNotNull(),'Inner')\
                 .otherwise('Leaf'))\
    .select(a.node,'type')\
    .orderBy(a.node,ascending=False)\
.show()