# Read data from employee table/csv file, department table/csv file
# Join them by dept no And
# Write them to new table which is partitioned by dept no and mode is overwrite 

# import findspark
# findspark.init()
# import pyspark

# emp_df=spark.read.csv("employee.csv",header=True)
# dept_df=...

# outputDf=emp_df.join(dept_df,emp_df.dept_no == dept_df.dept_no)
# outputDf.partitionBy(F.col("emp_df.dept_no")).mode("overwrite").format("csv").write("output")

import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
emp_df=spark.read.csv("SampleData/tredence-employee.csv",header=True)
dept_df=spark.read.csv("SampleData/tredence-dept.csv",header=True)
outputDf=emp_df.join(dept_df,emp_df.dept_no == dept_df.dept_no).select(emp_df.id,emp_df.name,emp_df.dept_no.alias("emp_df_dept_no"),dept_df.dept_no,dept_df.dept_name)
outputDf.show()
# outputDf.part(F.col("emp_df.dept_no")).mode("overwrite").format("csv").write("output/tredence-out") completely incorrect line
outputDf.write.partitionBy("dept_no").option("header", "true").mode("overwrite").format("csv").save("output/tredence-out")
#close spark
