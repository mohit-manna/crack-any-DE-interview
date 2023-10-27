import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[5]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
# read
df=spark.read.csv("SampleData/a1.csv",header=True) #job 0
df1=spark.read.csv("SampleData/b1.csv",header=True) # job 1
joined_df=df.join(df1,df.A1 == df1.B1,"inner") # job 2
joined_df.wi
print(joined_df.count()) # job 3 Total 4 jobs, 5 jobs, 5 tasks
#close spark
input("Press Enter to stop")
