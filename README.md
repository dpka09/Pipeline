# Data Storage:

The collected data has been stored in the Comma Separated Value file Zomato.csv. Each restaurant in the dataset is uniquely identified by its Restaurant Id.

• Restaurant Id: Unique id of every restaurant across various cities of the world

• Restaurant Name: Name of the restaurant

• Country Code: Country in which restaurant is located

• City: City in which restaurant is located

• Cuisines: Cuisines offered by the restaurant

• Cost per person: Cost for people in different currencies

• Currency: Currency of the country

• Has Table booking: yes/no

• Has Online delivery: yes/ no

• Is delivering: yes/ no

• Price range: range of price of food

• Rating: Average rating out of 5

• Rating text: text on the basis of rating of rating

• Votes: Number of ratings casted by people

---

### Working Mechanism

![image](https://github.com/user-attachments/assets/0a78469e-82cb-49d2-92f5-a4cb2ef940fa)

***Download Dataset:*** The process begins with downloading a dataset from Kaggle.

***Normalize Dataset:*** The dataset undergoes normalization to standardize and prepare the data for further processing.

***Load to HDFS:*** The normalized dataset is then loaded into Hadoop Distributed File System (HDFS) for storage and processing.

***Use Airflow:***

- Airflow orchestrates the workflow:

          -Reads data from HDFS using PySpark.
          -Processes the data.
          -Loads the processed results into a Postgres database.


---
***Install Airflow, Hadoop, Spark***

>git clone [https://gitlab.com/fusedataengineering/pipe_one/pipe_deepika_zomato.git](https://github.com/dpka09/Zomato-DataAnalysis-Using-Pyspark.git)

>Set environment variable for airflow :

        export AIRFLOW_HOME=~/airflow

>Initiate airflow database:

         airflow db init

>Create user on airflow :

        airflow users create /
    
        --username  /
        --firstname  /
        --lastname  /
        --role Admin /
        --email 




start airflow web UI on daemon mode : 

         airflow webserver -D 

start airflow scheduler on daemon mode : 
        
        airflow scheduler -D 


# Questions:


1. Cities with maximum resturants?

2. which cuisine famous in certain country/city?

3. how per capita income affects food ordering behaviour?

4. Resturants with maximum ratings?

5. How votes affects the price rating(range) ?

6. Top 5 popular ratings per counts?

7. How Table booking and  online delievery increases or decreases food ordering?

8. Which is most liked table booking or online delievery?

9. Display cuisine having price rating 2 and food rating above 4?

10. Count Resturants having no table booking and online delievery  but excellent ratings.

11. Food rating and cost per person of countries having per capita income below 5000 .

12. Which location/ city in a country is most profitable (has high orders) ?

13. Restaurants having most number of branches.

14. Top 10 resturants according to their expensiveness / high price.



***Fuse folder contains the output from Airflow***
