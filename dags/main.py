
import pyspark
from transformation import *

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Airflow")\
    .config("spark.driver.maxResultSize", "2g")\
    .config('spark.driver.extraClassPath', "<path to postgres driver jar folder>") \
    .getOrCreate()
#spark.conf.set("spark.driver.maxResultSize", "2g")




def load_df(df, task_id):
    mode= "overwrite"
    url="jdbc:postgresql://localhost:5432/<database-name>"
    properties = {"user":"<postgres username>",
                    "password":"<password>",
                    "driver":"org.postgresql.Driver"    
                    
                    }

    df.write.jdbc(url=url,
                  table="fuse_task{task_id}".format(task_id=task_id),
                  mode=mode,
                  properties= properties) 
                  
                  
 # my  <path to postgres driver jar folder> is /opt/spark/jars/postgresql-42.2.22.jar
