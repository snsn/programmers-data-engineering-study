# -*- coding: utf-8 -*-
"""week_4-assignment_1.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1flEe8Jz6wqcWt1TeoOu8N3MDCBcZR_vl
"""

import psycopg2

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

import requests
import csv

def extract(url):
    f = requests.get(link)
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

link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"

data = extract(link)

reader = transform(data)

load(reader)
