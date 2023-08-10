# creating custom dag instead of auto generated one

# importing all the required modules
from datatime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import bashScript
from airflow.utils.dates import days_ago


import pandas as pd
import sqlite3
import os

# get our current working direc (get dag direc path)
dag_path = os.getcwd()
print(dag_path)

def transform_data():
    booking = pd.read(f"{dag_path}/raw_data/booking.csv", low_memory=False)
    client = pd.read(f"{dag_path}/raw_data/client.csv", low_memory=False)
    hotel = pd.read(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

   
# merge the booking.csv and client .csv
    data = pd.merge(booking,client,on ='client_id')
    data.rename(Columns = {'name':'client_name', 'type':'client_type'}, inplace=True) #inplace=true it will chnage perminently
 # merge data the hotel.csv 
    data = pd.merge(data,hotel,on ='hotel_id')
    data.rename(Columns = {'name':'hotel_name'}, inplace=True) #inplace=true it will chnage perminently

# make data consistent
data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

# make currency consistent

# remove unnecessary columns

# load our process data



# initializing the default arrguments that we will pass to our dag
default_args = {
    'owner': 'vishal',
    'star_date': days_ago(5) #default args we can pass like this

}

# initializing the data
ingestion_task = DAG(
    'booking_ingestion',
    default_args= default_args,
    description= 'createad an etl pipeline for toursim data',
    scheduler_interval= timedelta(days=1),
    catchup=False
)

# tasktask1
task_1 = pythonOperator(
    task_id= 'transform_data',
    python_callback=transform_data,
    dag = ingestion_dag
)


task_2 = pythonOperator(
    task_id= 'load_data',
    python_callback=load_data,
    dag = ingestion_dag
)

# depend on
task_1>>task_2