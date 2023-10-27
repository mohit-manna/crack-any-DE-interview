import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
# https://app.codility.com/c/feedback/HRJM8Q-DY8/
class CouncilsJob:

    def __init__(self):
        findspark.init()
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("EnglandCouncilsJob")
                                          .getOrCreate())
        self.input_directory = "data"

    def extract_councils(self):
        a=self.spark_session.read.csv(f"SampleData/csv_files/{self.input_directory}/england_councils/district_councils.csv",header=True)
        b=self.spark_session.read.csv(f"SampleData/csv_files/{self.input_directory}/england_councils/london_boroughs.csv",header=True)
        c=self.spark_session.read.csv(f"SampleData/csv_files/{self.input_directory}/england_councils/metropolitan_districts.csv",header=True)
        d=self.spark_session.read.csv(f"SampleData/csv_files/{self.input_directory}/england_councils/unitary_authorities.csv",header=True)
        df1=a.withColumn("council_type",F.lit("District Council"))
        df2=b.withColumn("council_type",F.lit("London Borough"))
        df3=c.withColumn("council_type",F.lit("Metropolitan District"))
        df4=d.withColumn("council_type",F.lit("Unitary Authority"))
        df=df1.union(df2).union(df3).union(df4)
        print(df.count())
        return df
        pass

    def extract_avg_price(self):
        a=self.spark_session.read.csv(f"SampleData/csv_files/{self.input_directory}/property_avg_price.csv",header=True)
        df=a.withColumnRenamed("local_authority", "council").select("council","avg_price_nov_2019")
        print(df.count())
        print(df.schema)
        return df
        pass

    def extract_sales_volume(self):
        a=self.spark_session.read.csv(f"SampleData/csv_files/{self.input_directory}/property_sales_volume.csv",header=True)
        df=a.withColumnRenamed("local_authority", "council").select("council","sales_volume_sep_2019")
        print(df.count())
        print(df.schema)
        return df
        pass

    # def transform(self, councils_df, avg_price_df, sales_volume_df):
    #     df=councils_df.join(avg_price_df,councils_df.council==avg_price_df.council,"left")\
    #         .join(sales_volume_df,councils_df.council==sales_volume_df.council,"left")\
    #         .select(councils_df.council,councils_df.county,councils_df.council_type,avg_price_df.avg_price_nov_2019.cast(DecimalType(10, 1)),sales_volume_df.sales_volume_sep_2019.cast(DecimalType(10, 1)))
    #     df.show()
    #     return df
    #     pass
    def transform(self, councils_df, avg_price_df, sales_volume_df):
        transform=councils_df.join(avg_price_df,councils_df.council==avg_price_df.council,'left')\
            .join(sales_volume_df,councils_df.council==sales_volume_df.council,'left')\
                .select(councils_df.council.cast(DecimalType(10, 1)),councils_df.county.cast(DecimalType(10, 1)),councils_df.council_type.cast(DecimalType(10, 1)),avg_price_df.avg_price_nov_2019.cast(DecimalType(10, 1)),sales_volume_df.sales_volume_sep_2019.cast(DecimalType(10, 1)))
        return transform

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())

if __name__=="__main__":
    cj = CouncilsJob()
    cj.run()