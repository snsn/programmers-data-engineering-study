from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

import psycopg2
import requests
import csv

dag_second_assignment = DAG(
        dag_id = 'second_assignment',
        start_date = datetime(2020,8,1), # 적당히 조절
        schedule_interval = '0 * * * *')  # 적당히 조절

# Redshift connection 함수
def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "peter"
    redshift_pass = "PeterWoW1!"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    reader = csv.DictReader(text.splitlines())
    return list(reader)

def load(lines):
    cur = get_Redshift_connection()
    truncate = 'TRUNCATE TABLE peter.name_gender;'

    values = []
    for line in lines:
        values.append( f" ('{line['name']}', '{line['gender']}')")
    values = ', '.join(values)
    print(values)
    sql = f"INSERT INTO peter.name_gender VALUES {values} ;"

    sql_with_transaction = f"BEGIN; {truncate} {sql} END;"
    print(sql_with_transaction)
    cur.execute(sql_with_transaction)
    return


def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"

    data = extract(link)
    reader = transform(data)
    load(reader)


task = PythonOperator(
        task_id = 'etl',
        python_callable = etl,
        dag = dag_second_assignment)

