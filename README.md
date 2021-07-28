# pipe_deepika_zomato

Data Storage:
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


Install Airflow, Hadoop, Spark 

>> git clone https://gitlab.com/fusedataengineering/pipe_one/pipe_deepika_zomato.git
>> Set environment variable for airflow :  export AIRFLOW_HOME=~/airflow
>> Initiate airflow database: airflow db init
>> Create user on airflow : 
    airflow users create \
    --username <username> \
    --firstname <firstname> \
    --lastname <lastname> \
    --role Admin \
    --email <email>

>> start airflow web UI on daemon mode : airflow webserver -D
>> start airflow scheduler on daemon mode : airflow scheduler -D
 




Questions:

Cities with maximum resturants?

which cuisine famous in certain country/city?

how per capita income affects food ordering behaviour?

Resturants with maximum ratings?

How votes affects the price rating(range) ?

Top 5 popular ratings per counts?

How Table booking and  online delievery increases or decreases food ordering?

Which is most liked table booking or online delievery?

Display cuisine having price rating 2 and food rating above 4?

Count Resturants having no table booking and online delievery  but excellent ratings.

