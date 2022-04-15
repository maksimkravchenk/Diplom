#student

#Importing required modules
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import boto3
from io import StringIO
import io
import pandas as pd
import numpy as np
import os
from clickhouse_driver import Client as ClickClient
from datetime import datetime


#Importing the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
os.environ["AWS_ACCESS_KEY_ID"] = "AKIAUYMIUKYSLYI6NCKV"
os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

#Procedure to read the file from the Bucket.
def read_file(Bucket,Key):
    """
    Reading the file from the AWS S3 Bucket and store it to the DataFrame. Also the Year and Month of the Data in the csv file.
    The input parameters:
    - Bucket: Bucket name {String},
    - Key: File to read {String}

    The return:
    - DataFrame,
    - Year{int},
    - Month{int}
    """

    print("in 'read_file' procedure")
    response3 = boto3.client('s3').get_object(Bucket=Bucket, Key=Key)
    print("response done")
    df = pd.read_csv(io.BytesIO(response3['Body'].read()))
    print("pd.read_csv done")
    year = int(df['started_at'][1][:4])
    print(year)
    month = int(df['started_at'][1][5:7])
    print(month)
    print(f"out 'read_file' procedure. Return df, year: {year}, month: {month}")
    return (df, year, month)


#Procedure to write the DataFrame to the S3 Bucket.
def write_file(DataFrame,Bucket,Key):
    """
    Write DataFrame to the AWS S3 Bucket.
    The input parameters:
    - DataFrame: Dataframe
    - Bucket: Bucket name {String},
    - Key: File to read {String}

    The return: None
    """

    print("in 'write_file' procedure")
#    DataFrame = df_time_rides
    s3_client = boto3.client('s3')
    csv_buf = StringIO()
    DataFrame.to_csv(csv_buf, header=True, index=False)
    response = s3_client.put_object(Bucket='netology-output', Body=csv_buf.getvalue(), Key=Key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful write file: {Key} into S3 {Bucket}. Status - {status}")
        print("out 'write_file' procedure")
    else:
        print(f"Errorwrite file: {Key} into S3 {Bucket}. Status - {status}")
        print("out 'write_file' procedure")
    return


#Procedure to validate the DataFrame.
def validate_dataframe(Dataframe):
    """
    Validate the DataFrame for the gender column. If doesn't exist add the column 'gender' = 0 <int>.
    The input parameters:
    - Dataframe: Dataframe

    The return:
    - Dataframe
    """

    print("in 'validate_dataframe' procedure")
    if 'gender' in Dataframe.columns:
        print("Dataframe contains 'gender' comlumn. No need to update")
    else:
        print("Dataframe doesn't contain 'gender' comlumn. Adding 'gender' column with value: 0")
        Dataframe['gender'] = 0
    print("out 'validate_dataframe' procedure. Return DataFrame")
    return Dataframe


#Procedure to insert the data to the Ckickhouse DataBase
def insert_df_to_CH(DataFrame, Database, Table, IP):
    """
    Insert DataFrame to ClickHouse.
    The input parameters:
    - DataFrame: Dataframe
    - Database: Database name {String}
    - Table: Table name {String}

    The return: None
    """

    print("in 'insert_df_to_CH' procedure")
    client = ClickClient(host=IP, settings={'use_numpy': True})
    try:
        client.insert_dataframe("INSERT INTO " + Database + "." + Table + " VALUES", DataFrame)
        print("Data insert successful")
        print("out 'insert_df_to_CH' procedure")
    except:
        print(f"Data insert Error into {Database}.{Table} table")
        print("out 'insert_df_to_CH' procedure")
    return


#Procedure to do the SQL query to prepare the report.
def sql_request(query,IP):
    """
    Query request to the clickhouse Database. Store the result in the DataFrame
    The input parameters:
    - query: query to the Database {String}
    - IP: IP address of the Clickhouse server {String}

    The return:
    - output_df: Dataframe
    """

    print("in 'sql_request' procedure")
    client = ClickClient(host=IP, settings={'use_numpy': True})
    output_df = pd.DataFrame.from_records(client.execute(query))
    print("out 'sql_request' procedure. Return DataFrame, which will be stored as file.csv")
    return output_df


#Main Procedure. Which will be triggered by DAG
def main(**context):
    Bucket_in = "netology-input"
    Bucket_out = "netology-output"
    Database = "netology"
    Table = "rides"
    public_ip = 'localhost'
    Key = context['dag_run'].conf['file']
    print(Key)
    df, year, month = read_file(Bucket_in,Key)
    df = validate_dataframe(df)
    insert_df_to_CH(df, Database, Table, public_ip)

    #make a SQL query and store the for the Total Number of Rides per day
    df_number_rides = sql_request(f"select toYYYYMMDD(started_at) AS date, count(ride_id) FROM netology.rides where toYear(started_at) = {year} AND toMonth(started_at) = {month} GROUP BY
date ORDER BY date",public_ip)
    df_number_rides = df_number_rides.rename(columns={0: 'Date', 1: 'Total_Number_of_Rides'})
    path = (f"{year}_{month}_report_number_of_rides.csv")
    #write DataFrame to the output Bucket as <year>_<month>_report_number_of_rides.csv
    write_file(df_number_rides,Bucket_out,path)

    #make a SQL query and store the for the Average Ride Time per day
    df_time_rides = sql_request(f"select toYYYYMMDD(started_at) AS date, round(sum(dateDiff('second', started_at, ended_at))/count(ride_id),0) from netology.rides where toYear(started_at)
 = {year} AND toMonth(started_at) = {month} GROUP BY date ORDER BY date",public_ip)
    df_time_rides = df_time_rides.rename(columns={0: 'Date', 1: 'Average_Ride_Time'})
    path = (f"{year}_{month}_report_average_ride_time.csv")
    #write DataFrame to the output Bucket as <year>_<month>_report_average_ride_time.csv
    write_file(df_time_rides,Bucket_out,path)

    #make a SQL query and store the for the Total Number of Rides per day per gender
    df_rides_gender = sql_request(f"select toYYYYMMDD(started_at) AS date, gender, count(ride_id) FROM netology.rides where toYear(started_at) = {year} AND toMonth(started_at) = {month} G
ROUP BY date, gender ORDER BY date, gender",public_ip)
    df_rides_gender = df_rides_gender.rename(columns={0: 'Date', 1: 'gender', 2: 'Totel Rides'})
    path = (f"{year}_{month}_report_rides_gender.csv")
    #write DataFrame to the output Bucket as <year>_<month>_report_rides_gender.csv
    write_file(df_rides_gender,Bucket_out,path)

    print("------------------------------------------------------")
    print("----------------------END-----------------------------")
    print("------------------------------------------------------")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['maksim_kravchenk@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,

}


with DAG(
    dag_id='my_etl_v1',
    default_args=default_args,
    description='The DAG to process the csv file and store to the S3 netology-output ',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=2,
) as dag:

    etl = PythonOperator(
        task_id='ETL',
        python_callable=main,
        provide_context=True,
#        op_kwargs={'key1': 'JC-202203-citibike-tripdata.csv'},
        dag=dag,
    )
