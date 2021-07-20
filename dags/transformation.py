
import findspark
findspark.init()


import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
spark =  pyspark.sql.SparkSession.builder.getOrCreate()

from main import *


from pyspark.sql.functions import *
from pyspark.sql.types import *

restaurant_schema = StructType([
    StructField('Restaurant_Name', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Restaurant_ID', IntegerType(), False),
    StructField('Has_Table_booking', StringType(), True),
    StructField('Has_Online_delivery', StringType(), True),
    StructField('Is_delivering_now', StringType(), True),
    StructField('Country_Code', IntegerType(), False)
            ])

foods_schema = StructType([
    StructField('Country_Code', IntegerType(), False),
    StructField('Cuisines', StringType(), False),
    StructField('Rating', FloatType(), False),
    StructField('Rating_text', StringType(), False),
    StructField('Votes', IntegerType(), False)
    
                          ])


cost_schema = StructType([
    StructField('Cost_per_person', IntegerType(), False),
    StructField('Currency', StringType(), True),
    StructField('Price_range', IntegerType(), False),
    StructField('Country_Code', IntegerType(), False)
                          ])

country_schema = StructType([
    StructField('Country_Code', IntegerType(), False),
    StructField('Country', StringType(), True),
    StructField('Per_Capita_Income(USD)', IntegerType(), False)
                ])

#Restaurant.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save("hdfs://localhost:9000/fuse_project/Restaurant.csv")
#print("Data Written")

Restaurant=spark.read.csv("hdfs://localhost:9000/fuse_project/Zomato_project/Restaurant.csv", header=True, schema = restaurant_schema )
Foods=spark.read.csv("hdfs://localhost:9000/fuse_project/Zomato_project/Foods.csv", header=True, schema = foods_schema)
Cost=spark.read.csv("hdfs://localhost:9000/fuse_project/Zomato_project/Cost.csv", header=True, schema = cost_schema)
Country=spark.read.csv("hdfs://localhost:9000/fuse_project/Zomato_project/Country.csv" , header=True, schema = country_schema)



Foods=Foods.select("Country_Code","Rating","Rating_text","Votes",split(col("Cuisines"),",").alias("Cuisine"))
#Foods.show()


#Loading dataframes into POstgres database




# # 1. Cities with maximum Restaurants
def task1():
    task1= Restaurant.groupBy("City").count().sort(col("count").desc())
    return task1



# # 2. Which cuisine is famous (most ordered) in Ahmedabad city?

def task2():
    task2= Restaurant.join(Foods, Restaurant.Country_Code== Foods.Country_Code,"inner").select("City","Cuisine","Rating_text")

    task21=task2.filter(task2.City=="Ahmedabad")
#task21.show(2,truncate=False)

    task22 = task21.select(task21.City,explode(task21.Cuisine)).withColumnRenamed("col","Cuisine")


    task23=task22.groupBy("Cuisine").count().distinct().sort(col("count").desc())
    return task23
# OR

# task_22=task21.filter(task21.Rating_text == "Excellent")
#task_22.show()


#task_23 = task_22.select(task_22.Rating_text,explode(task_22.Cuisine)).withColumnRenamed("col","Cuisine")
#task_23.groupBy("Cuisine").count().sort(col("count").desc()).show()



# # 3.how per capita income affects food ordering behaviour?
def task3():
    task3= Foods.join(Country,"Country_Code","inner")
    task31= Country.select(max("Per_Capita_Income(USD)"), min("Per_Capita_Income(USD)"))
#task31.show()

#Analysing behaviour of customers having high per capita income i.e 68309, what kind of foods do they order

    task3= task3.withColumnRenamed("Per_Capita_Income(USD)","PCI")

    task33= task3.filter(task3.PCI==68309).select(task3.Country,task3.PCI,task3.Rating,explode(task3.Cuisine)).withColumnRenamed("col","Cuisine")
    return task33

# # 4. Resturants with maximum (high) ratings (Excellent rating and > 4.9) ?

def task4():
    task4= Restaurant.join(Foods, "Country_Code","inner").select("Restaurant_Name","Rating", "Rating_text")
#task4.show()


    task41=task4.filter(task4.Rating_text=="Excellent").distinct()


    task42=task41.filter(task41.Rating >= 4.9).distinct()
    return task42


# # 5. How votes affects the price range ?

def task5():
    task5= Foods.join(Cost, "Country_Code","inner")
#task5.show()

    task52=task5.select(max("Votes"), min("Votes"))
    task53=task5.filter(task5.Votes==10934).select("Votes","Price_range")

    return task53


# # 6.Top 5 popular ratings per counts?

def task6():
    task6=Foods.select("Rating")
    task6= task6.filter(task6.Rating<=5.0).groupBy("Rating").count().sort(col("count").desc())
#task6.show(5)
    return task6


 
# # 7. How Table booking and  online delievery increases or decreases food ordering?

def task7():
    task7= Restaurant.join(Foods, "Country_Code", "inner").select("Cuisine", "Has_Table_booking","Has_Online_delivery")
#task7.show()

    task71= task7.filter(task7.Has_Table_booking == "Yes").groupBy("Has_Table_booking").count()
    task72= task7.filter(task7.Has_Table_booking == "No").groupBy("Has_Table_booking").count()
    task73= task7.filter(task7.Has_Online_delivery == "Yes").groupBy("Has_Online_delivery").count()
    task74= task7.filter(task7.Has_Online_delivery == "No").groupBy("Has_Online_delivery").count()

    task99= task71.union(task72)
    task999=task73.union(task74)
    task999=task999.select("Has_Online_delivery","count").withColumnRenamed("count","Counts")

    task75=task99.join(task999) ###  Job aborted due to stage failure: Total size of serialized 					results of 110794 tasks (1024.0 MiB) is bigger than 					spark.driver.maxResultSize(1024.0 MiB) ###

    #task75=task71.join(task73)
    #task76= task73.join(task74)

    return task75    
    
#task71= task7.filter(task7.Has_Table_booking == "Yes").groupBy("Has_Table_booking","Cuisine").count().show(50,truncate=False)
#task72= task7.filter(task7.Has_Table_booking == "No").groupBy("Has_Table_booking","Cuisine").count().show(50,truncate=False)


#task73= task7.filter(task7.Has_Online_delivery == "Yes").groupBy("Has_Online_delivery","Cuisine").count().show(50,truncate=False)
#task74= task7.filter(task7.Has_Online_delivery == "No").groupBy("Has_Online_delivery","Cuisine").count().show(50,truncate=False)



# # 8. Which is most liked table booking or online delievery?

def task8():
    task8= Restaurant.select("Has_Table_booking","Has_Online_delivery")


    task81= task8.filter(task8.Has_Table_booking == "Yes").groupBy("Has_Table_booking").count()

    task82= task8.filter(task8.Has_Online_delivery == "Yes").groupBy("Has_Online_delivery").count()
    task82=task82.select("Has_Online_delivery","count").withColumnRenamed("count","Counts")
    task83=task81.join(task82)
    return task83
# # 9. Display cuisine having price rating 2 and food rating above 4?

def task9():
    task9= Foods.join(Cost, "Country_Code").select("Cuisine","Price_range","Rating")


    task92= task9.select("Rating","Price_range",explode(task9.Cuisine)).withColumnRenamed("col","Cuisine")

    task93= task92.filter((task92.Price_range<2)& (task92.Rating>4.5))  
    return task93


# # 10. Count Resturants having no table booking and online delievery  but excellent ratings.

def task10():
    task10= Restaurant.join(Foods, "Country_Code")
#task10.show(2)

    task11= task10.filter((task10.Has_Table_booking == "No") & (task10.Has_Online_delivery == "No") & (task10.Rating_text == "Excellent"))
    task12=task11.select(count("Restaurant_Name"))
    return task12

# # 11. Top 10 resturants according to their expensiveness / high price.



