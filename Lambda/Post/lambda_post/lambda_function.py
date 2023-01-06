import json
import awswrangler as wr
import pandas as pd
import pymysql
import os
import time
import datetime
import dateutil.parser as dp

def mysql_query_consult(con, query):
	cursor = con.cursor()
	cursor.execute(query)
 
	return cursor.fetchall()
	
def mysql_query_commit(con, query):
	cursor = con.cursor()
	cursor.execute(query)
	con.commit()

#Credenciales de conexión	

mysql_endpoint = os.environ['mysql_host']
mysql_username = os.environ['mysql_user']
mysql_password = os.environ['mysql_pw']
mysql_db_name = os.environ['mysql_db']

def save_file(body,table): 
    date_name = datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    list = []
    data = body.split('\n')
    for row in data:
        list.append(row.split(','))
        
    if table == 'departments':
    
        df = pd.DataFrame(list, columns=['id', 'department'])
        
    elif table == 'jobs':
        df = pd.DataFrame(list, columns=['id', 'job'])
        
    elif table == 'hired_employees':
        df = pd.DataFrame(list, columns=['id', 'name', 'datetime', 'department_id', 'job_id'])
        
    wr.s3.to_csv(df, f's3://globant-container/api_calls/{table}_{date_name}.csv',index=False)
    
def departments(body):
    values = ''
    count = 0
    data = body.split('\n')
    for row in data:
        parsed_row = row.split(',')
        id = parsed_row[0]
        department = parsed_row[1].strip()
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
        
    query = "insert into departments (id, department) values" + values[:-2]
    
    return query,count
    
def hired_employees(body):
    values = r''
    count = 0
    data = body.split('\n')
    for row in data:
        parsed_row = row.split(',')
        id = parsed_row[0]
        name = parsed_row[1].strip().replace("'","''")
        datetime = parsed_row[2].strip()
        department_id = parsed_row[3].strip()
        job_id = parsed_row[4].strip()
        
        if name == '':
            name = 'NULL'
            
        if name != 'NULL':
            name = f"'{name}'"
            
        if datetime == '':
            datetime = 'NULL'
            
        if datetime != 'NULL':
            datetime = f"'{datetime}'"
            
        if department_id == '':
            department_id = 'NULL'
            
        if job_id == '':
            job_id = 'NULL'
        
        try:
            id = int(id)
            name = str(name)
            if datetime != 'NULL':
                dp.parse(datetime)
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
    
def jobs(body):
    values = ''
    count = 0
    data = body.split('\n')
    for row in data:
        parsed_row = row.split(',')
        id = parsed_row[0]
        job = parsed_row[1].strip()
        
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
        
    query = "insert into jobs (id, job) values" + values[:-2]
    
    return query,count

def lambda_handler(event, context):
    
    try:
        table = event['params']['header']['table'] 
    except:
        return "Please set a correct table (departments, jobs or hired_employees)"

    mysql_connection = pymysql.connect(host = mysql_endpoint, user = mysql_username, passwd = mysql_password, db = mysql_db_name)


    if table == 'departments':
        save_file(event['body'],'departments')
        result = departments(event['body'])
        if result[0] == 'Data type error':
            return result[0]
        try:
            mysql_query_commit(mysql_connection, result[0])
            return f"{result[1]} records successfully uploaded to Departments"
            
        except pymysql.Error as e:
            return f"Error {e.args[0]}: {e.args[1]}."
            
    
    elif table == 'hired_employees':
        save_file(event['body'],'hired_employees')
        
        result = hired_employees(event['body'])
        if result[0] == 'Data type error':
            return result[0]
        try:
            mysql_query_commit(mysql_connection, result[0])
            return f"{result[1]} records successfully uploaded to Jobs"
            
        except pymysql.Error as e:
            return f"Error {e.args[0]}: {e.args[1]}."
    
    elif table == 'jobs':
        save_file(event['body'],'jobs')
        result = jobs(event['body'])
        if result[0] == 'Data type error':
            return result[0]
        try:
            mysql_query_commit(mysql_connection, result[0])
            return f"{result[1]} records successfully uploaded to Jobs"
            
        except pymysql.Error as e:
            return f"Error {e.args[0]}: {e.args[1]}."
    
    else:
        return "Please set a correct table (departments, jobs or hired_employees)"
    

