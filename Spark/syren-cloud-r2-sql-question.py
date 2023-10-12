import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
df=spark.read.csv("SampleData/syren-cloud-data.csv",header=True)
#Problem Statement: data represents user, subscription start and end date. We need to find if subscription is matching or not with other.
df.createOrReplaceTempView("subs")
# I m moron, couldn't write complete code
spark.sql("""
with x as (
select a.user,
    case when ((a.user != b.user) and ((date(a.start_date) between date(b.start_date) and date(b.end_date)) or 
    (
    (date(b.start_date) between date(a.start_date) and date(a.end_date))
    )))
    then 1
    else 0
    end as subs
    from 
          subs a
    cross join 
          subs b
  )      
  select x.user,case when sum(subs)>=1
  then 'matching'
  else 'Not Matching'
  end as subscriptin
  from x
  group by x.user   
""").show()
#close spark


