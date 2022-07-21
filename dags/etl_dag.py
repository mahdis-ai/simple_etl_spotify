from airflow import DAG
import pandas as pd 
import requests
from pathlib import Path  
import json


import datetime
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator

import boto3
import os


USER_ID = "" # your Spotify username 
TOKEN = "" # your Spotify token
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            pass
    return True
def extract():
    print("extracting")
    # Extract part of the ETL process
 
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }
    return song_dict
    
    
def transform(ti):
    song_dict =ti.xcom_pull(task_ids=["extract"])[0]
    print(song_dict)
    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    # Validate
 
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
        return song_dict
    return None

    # Load

def load(ti):
    song_dict=ti.xcom_pull(task_ids=["transform"])[0]
    if song_dict is not None:
        song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
        BUCKET_NAME ='' #bucket name
   
        current_directory=os.getcwd()
        file_path=os.path.join(current_directory,'song_data.csv')
        song_df.to_csv(file_path)
        session = boto3.Session(
            aws_access_key_id='',
            aws_secret_access_key='',
        )
        s3=session.client("s3")
        s3.upload_file(file_path,BUCKET_NAME, 'Mahdis/song_data.csv')



with DAG("etl_dag",start_date=datetime.datetime(2022,1,1),
    schedule_interval="@daily",catchup=False) as dag:
    extract_process=PythonOperator(
        task_id="extract",
        python_callable=extract
    )
    sleep_bash=BashOperator(
        task_id="sleep",
        bash_command="sleep 3"
    )
    transform_process=PythonOperator(
        task_id="transform",
        python_callable=transform
    )
    load_process=PythonOperator(
        task_id="load",
        python_callable=load
    )
    extract_process >> sleep_bash >> transform_process >> load_process