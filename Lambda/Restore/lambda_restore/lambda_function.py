import json
import avro
from avro.datafile import DataFileReader
from fastavro import reader, schemaless_reader
import io
import boto3
import pymysql
import os


def mysql_query_consult(con, query):
	cursor = con.cursor()
	cursor.execute(query)
 
	return cursor.fetchall()
	
def mysql_query_commit(con, query):
	cursor = con.cursor()
	cursor.execute(query)
	con.commit()

     
def restore_departments(path):
    path = path.replace('s3://','').split('/')
    bucket = path[0]
    key = f'{path[1]}/{path[2]}'
    response = s3.get_object(Bucket=bucket, Key= key,ResponseContentEncoding='utf-8')
    a = response['Body'].read()
    avro_bytes = io.BytesIO(a)
    reader = DataFileReader(avro_bytes, avro.io.DatumReader())
    values = ''
    count = 0
    for line in reader:

    

        id = line['id']
        department = line['department']
        if department == '':
            department = 'NULL'
            
        if department != 'NULL':
            department = f"'{department}'"
            
        try:
            id = int(id)
            department = str(department)
            values += f"({id},{department}), "
            count += 1
        except:
            continue    
        
    if len(values) == 0:
        return 'Data type error',count
        
    query = f"insert into departments (id, department) values{values[:-2]}"
    
    return query,count
    
    
def restore_jobs(path):
    path = path.replace('s3://','').split('/')
    bucket = path[0]
    key = f'{path[1]}/{path[2]}'
    response = s3.get_object(Bucket=bucket, Key= key,ResponseContentEncoding='utf-8')
    a = response['Body'].read()
    avro_bytes = io.BytesIO(a)
    reader = DataFileReader(avro_bytes, avro.io.DatumReader())
    values = ''
    count = 0
    for line in reader:

        id = line['id']
        job = line['job']
        if job == '':
            job = 'NULL'
            
        if job != 'NULL':
            job = f"'{job}'"
            
        try:
            id = int(id)
            job = str(job)
            values += f"({id},{job}), "
            count += 1
        except:
            continue    
        
    if len(values) == 0:
        return 'Data type error',count
        
    query = f"insert into jobs (id, job) values{values[:-2]}"
    
    return query,count
    
def restore_hiredemployees(path):
    path = path.replace('s3://','').split('/')
    bucket = path[0]
    key = f'{path[1]}/{path[2]}'
    response = s3.get_object(Bucket=bucket, Key= key,ResponseContentEncoding='utf-8')
    a = response['Body'].read()
    avro_bytes = io.BytesIO(a)
    reader = DataFileReader(avro_bytes, avro.io.DatumReader())
    values = r''
    count = 0
    for line in reader:

        id = line['id']
        name = line['name'].replace("'","''")
        datetime = line['datetime']
        job_id = line['job_id']
        department_id = line['department_id']
        
        if job_id == 0:
            job_id = 'NULL'
            
        if department_id == 0:
            department_id = 'NULL'
        
        if name == '':
            name = 'NULL'
            
        if name != 'NULL':
            name = f"'{name}'"
            
        if datetime == '':
            datetime = 'NULL'
            
        if datetime != 'NULL':
            datetime = f"'{datetime}'"
        
            
        try:
            id = int(id)
            name = str(name)
            if department_id != 'NULL':
                department_id = int(department_id)
            if job_id != 'NULL':
                job_id = int(job_id)
            values += f"({id},{name},{datetime},{department_id},{job_id}), "
            count += 1
        except:
            continue    
    
    
    if len(values) == 0:
        return 'Data type error',count
        
    query = r"insert into hired_employees (id, name, datetime, department_id, job_id) values" + values[:-2]
    
    return query,count
	
#Credenciales de conexión	

mysql_endpoint = os.environ['mysql_host']
mysql_username = os.environ['mysql_user']
mysql_password = os.environ['mysql_pw']
mysql_db_name = os.environ['mysql_db']

s3 = boto3.client("s3")

def lambda_handler(event, context):
    
    
    mysql_connection = pymysql.connect(host = mysql_endpoint, user = mysql_username, passwd = mysql_password, db = mysql_db_name)

    if event['table'] == 'department':

        result = restore_departments(event['file'])

        if result[0] == 'Data type error':
            return result[0]
        try:
            mysql_query_commit(mysql_connection, 'truncate table departments')
            mysql_query_commit(mysql_connection, result[0])
            return f"{result[1]} records successfully uploaded to Departments"
            
        except pymysql.Error as e:
            return f"Error {e.args[0]}: {e.args[1]}."

            
    elif event['table'] == 'job':
        
        result = restore_jobs(event['file'])

        if result[0] == 'Data type error':
            return result[0]
        try:
            mysql_query_commit(mysql_connection, 'truncate table jobs')
            mysql_query_commit(mysql_connection, result[0])
            return f"{result[1]} records successfully uploaded to Jobs"
            
        except pymysql.Error as e:
            return f"Error {e.args[0]}: {e.args[1]}."

    elif event['table'] == 'hired_employee':
        
        result = restore_hiredemployees(event['file'])

        if result[0] == 'Data type error':
            return result[0]
        try:
            mysql_query_commit(mysql_connection, 'truncate table hired_employees')
            mysql_query_commit(mysql_connection, result[0])
            return f"{result[1]} records successfully uploaded to Hired_employees"
            
        except pymysql.Error as e:
            return f"Error {e.args[0]}: {e.args[1]}."
    

        

    
    
    
    
    