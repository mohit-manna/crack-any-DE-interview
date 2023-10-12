## Installation
 Installation should be done according to recommended way. I have installed both scala spark and pyspark. 

 I have two environments. 

 1. Global python environment
 2. virtualenv

However, pyspark libraries lie in a separate folder. It has no relation with your python installation. 

Code to test pyspark installation: 

```
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
print('PySpark Version :'+spark.version)
#close spark

```
We just need to install `findspark` in our environment in which we want to work. I recommend using virtualenv. 

It can be done using `pip install findspark`. Its important to note that there is no need to install `pyspark` again. 

In many cases after installing spark, setting the path above code snippet works fine. 

In Windows we can check if path is set correctly or not using. 

In Powershell
```
> echo $ENV:SPARK_HOME
```

If your intellisense and code auto complete is not working properly for pyspark. 
Just add a `settings.json` file in folder and add below content according to your `SPARK_HOME`

```
{
    "python.autoComplete.extraPaths": [
        "C:/apps/opt/spark-3.2.3-bin-hadoop3.2/python"
    ],
    "python.analysis.extraPaths": [
        "C:/apps/opt/spark-3.2.3-bin-hadoop3.2/python"
    ]
}
```
After adding the `settings.json` file inside `.vscode` folder. Intellisense should start suggesting the functions and parameters.