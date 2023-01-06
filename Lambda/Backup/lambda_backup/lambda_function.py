import json
from fastavro import writer, parse_schema
import awswrangler as wr
import pandas as pd
import io
import boto3
import pymysql
import os
import time
import datetime

def mysql_query_consult(con, query):
	cursor = con.cursor()
	cursor.execute(query)
 
	return cursor.fetchall()
	
def mysql_query_commit(con, query):
	cursor = con.cursor()
	cursor.execute(query)
	con.commit()

def generate_avro_file(dframe, schema):
    buffer = io.BytesIO()
    writer(buffer, parse_schema(schema), dframe.to_dict('records'))
    buffer.seek(0)
    return buffer
     
def backup_departments(con):
    result = mysql_query_consult(con, 'select * from departments')
    if len(result) == 0:
        return 'Error. Empty table'
    
    df = pd.DataFrame(result, columns=['id', 'department'], index=None) 
    df['department'] = df['department'].fillna(value='') 
    
    
    avro_schema = {
    "name": "Department",
    "type": "record",
    "namespace": "department",
    "fields": [
    {"name": "id", "type": "int"}, 
    {"name": "department", "type": "string"}]}
    
    s3.upload_fileobj(generate_avro_file(df, avro_schema), 'globant-container', f'output/department_{date_name}.avro')
    
    return f'{len(result)} departments records saved successfully.'
    
def backup_jobs(con):
    result = mysql_query_consult(con, 'select * from jobs')
    if len(result) == 0:
        return 'Error. Empty table'
    
    df = pd.DataFrame(result, columns=['id', 'job'], index=None) 
    df['job'] = df['job'].fillna(value='') 
    
    avro_schema = {
    "name": "Job",
    "type": "record",
    "namespace": "job",
    "fields": [
    {"name": "id", "type": "int"}, 
    {"name": "job", "type": "string"}]}
    
    s3.upload_fileobj(generate_avro_file(df, avro_schema), 'globant-container', f'output/job_{date_name}.avro')
    
    return f'{len(result)} jobs records saved successfully.'
    
def backup_hiredemployees(con):
    result = mysql_query_consult(con, 'select * from hired_employees')
    if len(result) == 0:
        return 'Error. Empty table'
    
    df = pd.DataFrame(result, columns=['id', 'name', 'datetime', 'department_id', 'job_id'], index=None)
    df['department_id'] = df['department_id'].fillna(0)
    df['job_id'] = df['job_id'].fillna(0)
    df['name'] = df['name'].fillna(value='') 
    df['datetime'] = df['datetime'].fillna(value='') 
    df = df.astype({'id': 'int32', 'name': 'string', 'datetime': 'string', 'department_id': 'int', 'job_id': 'int'})
    
    avro_schema = {
    "name": "Hiredemployee",
    "type": "record",
    "namespace": "hiredemployee",
    "fields": [
    {"name": "id", "type": "int"}, 
    {"name": "name", "type": "string"},
    {"name": "datetime", "type": "string"},
    {"name": "department_id", "type": "int"},
    {"name": "job_id", "type": "int"}]}
    
    s3.upload_fileobj(generate_avro_file(df, avro_schema), 'globant-container', f'output/hiredemployee_{date_name}.avro')
    
    return f'{len(result)} hired employees records saved successfully.'
	
#Credenciales de conexión	

mysql_endpoint = os.environ['mysql_host']
mysql_username = os.environ['mysql_user']
mysql_password = os.environ['mysql_pw']
mysql_db_name = os.environ['mysql_db']

s3 = boto3.client("s3")

date_name = datetime.datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d')

def lambda_handler(event, context):
    
    mysql_connection = pymysql.connect(host = mysql_endpoint, user = mysql_username, passwd = mysql_password, db = mysql_db_name)

    if event['table'] == 'departments':

        return backup_departments(mysql_connection)

    elif event['table'] == 'jobs':
        
        return backup_jobs(mysql_connection)

    elif event['table'] == 'hired_employees':
        
        return backup_hiredemployees(mysql_connection)
            
    

        

    
    
    
    
    