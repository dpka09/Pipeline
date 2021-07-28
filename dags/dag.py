import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transformation import *
from main import *

def fuse_task1():
    load_df(task1(), 1)
    
def fuse_task2():
    load_df(task2(), 2)

def fuse_task3():
    load_df(task3(), 3)

def fuse_task4():
    load_df(task4(), 4)

def fuse_task5():
    load_df(task5(), 5)

def fuse_task6():
    load_df(task6(), 6)

def fuse_task7():
    load_df(task7(), 7)

def fuse_task8():
    load_df(task8(), 8)

def fuse_task9():
    load_df(task9(), 9)

def fuse_task10():
    load_df(task10(), 10)

def fuse_task12():
    load_df(task12(), 12)

def fuse_task13():
    load_df(task13(), 13)

def fuse_task14():
    load_df(task14(), 14)

default_args={

            'owner':'postgres',
            'start_date': airflow.utils.dates.days_ago(1),
            'depends_on_past': True,
            'email':['sthadpka93@gmail.com'],
            'email_on_failure':True,
            'email_on_retry': False,
            'retries': 7,
            'retry_delay': timedelta(minutes=3)
}

dag = DAG (dag_id= "Fuse",
            default_args= default_args,
            schedule_interval= "@hourly"  )

task_1= PythonOperator(task_id="fuse1",
                      python_callable=fuse_task1,
                      dag =dag       
                     )


task_2= PythonOperator(task_id="fuse2",
                      python_callable=fuse_task2,
                      dag =dag       
                     )

task_3= PythonOperator(task_id="fuse3",
                      python_callable=fuse_task3,
                      dag =dag       
                     )

task_4= PythonOperator(task_id="fuse4",
                      python_callable=fuse_task4,
                      dag =dag       
                     )

task_5= PythonOperator(task_id="fuse5",
                      python_callable=fuse_task5,
                      dag =dag       
                     )

task_6= PythonOperator(task_id="fuse6",
                      python_callable=fuse_task6,
                      dag =dag       
                     )

task_7= PythonOperator(task_id="fuse7",
                      python_callable=fuse_task7,
                      dag =dag       
                     )

task_8= PythonOperator(task_id="fuse8",
                      python_callable=fuse_task8,
                      dag =dag       
                     )

task_9= PythonOperator(task_id="fuse9",
                      python_callable=fuse_task9,
                      dag =dag       
                     )

task_10= PythonOperator(task_id="fuse10",
                      python_callable=fuse_task10,
                      dag =dag       
                     )

task_12= PythonOperator(task_id="fuse12",
                      python_callable=fuse_task12,
                      dag =dag       
                     )
    
task_13= PythonOperator(task_id="fuse13",
                      python_callable=fuse_task13,
                      dag =dag       
                     )

task_14= PythonOperator(task_id="fuse14",
                      python_callable=fuse_task14,
                      dag =dag       
                     )


task_1 >>   task_2 >>   task_3 >>   task_4 >>   task_5 >>   task_6 >>   task_7 >>   task_8 >>   task_9 >>   task_10  >> task_12 >> task_13 >> task_14