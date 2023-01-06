import json
import pymysql
import awswrangler as wr
import pandas as pd
import os

def mysql_query_consult(con, query):
	cursor = con.cursor()
	cursor.execute(query)
 
	return cursor.fetchall()
	
def mysql_query_commit(con, query):
	cursor = con.cursor()
	cursor.execute(query)
	con.commit()

def department_query(df):
    id = []
    department = []
    
    for index, row in df.iterrows():
        id.append(row['Id'])
        department.append(row['Department'])
    
    values = ""
    size = len(id)
    
    
    for i in range(len(id)):
        
        values += "(" + str(id[i]) + ", '" + str(department[i]) + "'), "

    
    query = "insert into departments (id, department) values" + values[:-2]
    
    return query
    
def job_query(df):
    id = []
    job = []
    
    for index, row in df.iterrows():
        id.append(row['Id'])
        job.append(row['Job'])
    
    values = ""
    size = len(id)
    
    
    for i in range(len(id)):
        
        values += "(" + str(id[i]) + ", '" + str(job[i]) + "'), "

    
    query = "insert into jobs (id, job) values" + values[:-2]
    
    return query
    
def hired_employees_query(df):
    id = []
    name = []
    datetime = []
    department_id = []
    job_id = []
    
    for index, row in df.iterrows():
        id.append(row['Id'])
        if str(row['Name']) != 'nan':
            name.append(row['Name'].replace("'","''"))
        else:
            name.append('NULL')
        if str(row['Datetime']) != 'nan':
            datetime.append(row['Datetime'])
        else: 
            datetime.append('NULL')
        if str(row['Department_id']) != 'nan':    
            department_id.append(row['Department_id'])
        else:
            department_id.append('NULL')
        if str(row['Job_id']) != 'nan':
            job_id.append(row['Job_id'])
        else:
            job_id.append('NULL')
    
    values = r""
    size = len(id)
    
    
    for i in range(len(id)):

        if str(name[i]) != 'NULL':
            _name = "'" + str(name[i]) + "'"
        else:
            _name = str(name[i])
            
        if str(datetime[i]) != 'NULL':
            _datetime = "'" + str(datetime[i]) + "'"
        else:
            _datetime = str(datetime[i])
        
        values += "(" + str(id[i]) + ", " + _name + ", "  + _datetime + ", "  + str(department_id[i]) + ", "  + str(job_id[i]) + "), "

    
    query = r"insert into hired_employees (id, name, datetime, department_id, job_id) values" + values[:-2]
    
    return query

#Credenciales de conexión	

mysql_endpoint = os.environ['mysql_host']
mysql_username = os.environ['mysql_user']
mysql_password = os.environ['mysql_pw']
mysql_db_name = os.environ['mysql_db']


def lambda_handler(event, context):
    mysql_connection = pymysql.connect(host = mysql_endpoint, user = mysql_username, passwd = mysql_password, db = mysql_db_name)
    
    df_departments = wr.s3.read_csv('s3://globant-container/history/departments.csv',index_col=False, header = None, names = ['Id', 'Department'])
    mysql_query_commit(mysql_connection, department_query(df_departments))
    
    df_jobs = wr.s3.read_csv('s3://globant-container/history/jobs.csv',index_col=False, header = None, names = ['Id', 'Job'])
    mysql_query_commit(mysql_connection, job_query(df_jobs))
    
    df_hired_employees = wr.s3.read_csv('s3://globant-container/history/hired_employees.csv',index_col=False, header = None, names = ['Id', 'Name', 'Datetime', 'Department_id', 'Job_id'])
    mysql_query_commit(mysql_connection,hired_employees_query(df_hired_employees))
    
    return 'All files were uploaded'

 