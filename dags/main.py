
import pyspark
from transformation import *

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Airflow") \
    .config('spark.driver.extraClassPath', "/opt/spark/jars/postgresql-42.2.22.jar") \
    .getOrCreate()




def load_df(df, task_id):
    mode= "overwrite"
    url="jdbc:postgresql://localhost:5432/mytest"
    properties = {"user":"postgres",
                    "password":"qw3rty123",
                    "driver":"org.postgresql.Driver"    
                    
                    }

    df.write.jdbc(url=url,
                  table="fuse_task{task_id}".format(task_id=task_id),
                  mode=mode,
                  properties= properties)