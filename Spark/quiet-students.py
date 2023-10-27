# https://www.codingninjas.com/studio/problems/find-the-quiet-students-in-all-exams_2196172
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
df=spark.read.csv("SampleData/quiet-students-data.csv",header=True)
df2=spark.read.csv("SampleData/quiet-students-exam.csv",header=True)
df.createOrReplaceTempView("student")
df2.createOrReplaceTempView("exam")
# 2,Jade Expected Output
spark.sql(
"""
with x as (
    select *,row_number() over (partition by exam_id order by score) as row_num
    from exam
),
y as (
    select student_id,
    max(row_num) as min_row_num,
    min(row_num) as max_row_num
    from x
    group by student_id
)
select y.student_id,s.student_name 
from y join student s on
y.student_id=s.student_id
where y.min_row_num=2 and y.max_row_num=2
"""
).show()

df2_with_row_num =  df2.withColumn("row_num",F.row_number().over(Window.partitionBy("exam_id").orderBy(F.desc("score"))))
studnet_min_max_row = df2_with_row_num.groupBy(F.col("student_id"))\
    .agg(F.max(F.col("row_num")).alias("max_row_num"),F.min(F.col("row_num")).alias("min_row_num"))
result_df = studnet_min_max_row.join(df,studnet_min_max_row.student_id==df.student_id)\
    .where((studnet_min_max_row.max_row_num == 2) & (studnet_min_max_row.min_row_num == 2))\
    .select(studnet_min_max_row.student_id,df.student_name)
result_df.show()