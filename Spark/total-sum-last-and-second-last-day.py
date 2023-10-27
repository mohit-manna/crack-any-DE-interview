import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("Manna").getOrCreate()
print('PySpark Version :'+spark.version)
df=spark.read.csv("SampleData/orders.csv",header=True) #job 0
df.createOrReplaceTempView("orders")
spark.sql("""
with x as (
    select product_id,sale_date,sum(sales_amount) over(partition by product_id order by sale_date)  as total_sales,
    row_number() over(partition by product_id order by sale_date desc) as row_num
    from orders
),
z as (select product_id,
    case when row_num=1   
    then sale_date 
    end as last_sale_date,
    case when row_num=1   
    then total_sales
    end as last_day_sales,
    case when row_num=2   
    then sale_date 
    end as second_last_sale_date,
    case when row_num=2   
    then total_sales
    end as second_last_day_sales
from x
where row_num in (1,2)
)
select product_id,
          max(last_sale_date) as last_sale_date,
          max(last_day_sales) as last_day_sales,
          max(second_last_sale_date) as second_last_sale_date,
          max(second_last_day_sales) as second_last_day_sales
          from z group by product_id
""").show()
input("Press Enter to stop")