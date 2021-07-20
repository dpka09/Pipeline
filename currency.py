from pyspark import SparkContext, SQLContext

from pyspark.sql.functions import * 
from pyspark.sql.types import *
import pyspark.sql.functions as F

sc= SparkContext()
spark = SQLContext(sc)

Cost= spark.read.csv("/home/deepika/Documents/Zomato_project/Cost.csv", header = True)
#print(Cost.show())

from currency_converter import CurrencyConverter

cc= CurrencyConverter()


c=cc.convert(10, "USD", "EUR")
print (c)





def Costs1(cost, currency):
    c= ["USD" , "GBP" , "TRY" , "INR" , "IDR" , "NZD" , "ZAR" , "BRL", "PHP"]
    for i in range(9554):
        if currency in c :
            cost= cc.convert(cost, currency, "USD")
            return cost

        elif currency== "AED":
        
            cost=float(cost)*0.27
            return cost
    
        elif currency == "QAR":
        
            cost =float(cost)*0.27
            return cost

        elif currency == "LKR":
        
            cost =float(cost) *0.0050
            return cost
    
        else:
            pass
    

Costs=F.udf(Costs1, FloatType())
Cost=Cost.withColumn("Cost_USD", Costs("Cost per person", "Currency"))
print(Cost.show())    
       

#Costs=F.udf(Costs1, FloatType())
#Cost=Cost.withColumn("Cost_USD_new", Costs("Cost_USD", "Currency"))

#print(Cost.show())

Cost.write.mode("overwrite").csv("/home/deepika/Downloads/Cost_new", header =True)

