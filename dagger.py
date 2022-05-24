try:
  from ast import operator
  from datetime import datetime, timedelta
  from textwrap import dedent
  import smtplib, ssl
# Import modules

  from airflow import DAG  
# Dag object to instantiate a DAG

# Operators to operate
  from airflow.operators.bash import BashOperator
  from airflow.operators.python_operator import PythonOperator
except Exception as e:
  print('error {}')

# Args to pass on to operators
default_args = {
  'owner' : 'airflow',
  'depends_on_past' : True,
  'email' : ['example@gmail.com'],
  'email_on_failure' : True,
  'email_on_retry' : True,
  'retries' : 1,
  'retry_delay' : timedelta(minutes=5),
  'priority_weight' : 10,
  'execution_timeout' : timedelta(seconds=120),
  'trigger_rule': 'all_success'
  }
  
#!/usr/bin/python
import psycopg2
import config
  
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
  
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
          
        # create a cursor
        cur = conn.cursor()
          
    # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
  
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
         
    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
  
  
if __name__ == '__main__':
    connect() 
        
port = 465  # For SSL
password = "examplepass"

context = ssl.create_default_context()

#First job, conniecting to postgres and data fetch/push to other jobs
def t1():
  psycopg.connect()
  cursor.execute("Select email FROM documents WHERE customer='regular' AND aproval ='yes' ")
  context['ti'].xcom_push(key='key1', value=cursor.fetchall())
  cursor.execute("Select accountno FROM  WHERE customer='regular' AND aproval ='no' ")
  context['ti'].xcom_push(key='key2',value=cursor.fetchall())
  #Second job, deploys mail to the approved mass  
def t2():
  pending= context.get('ti').xcom_pull(key='key1')
  with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
    message="Your account has been approved"
  server.login("my@gmail.com", password)    
  server.sendmail("my@gmail.com", "your@gmail.com", message)   
    
def t3(): 
  list=context.get('ti').xcom_pull(key='key2')                       
  with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
    message=list
  server.login("my@gmail.com", password)    
  server.sendmail("my@gmail.com", "employee@gmail.com", message)

  with DAG (
    dag_id='FLOWER',
    default_args=default_args,
    description='A simple workflow DAG',
    schedule_interval='@daily',
    start_date=datetime(2022,3,12),
    tags=['example'],
    ) as dag :
  
    first_function_execute = BashOperator(
        task_id='print_date',
        python_callable=t1
    )

    second_function_execute = BashOperator(
        task_id='tries',
        retries=3,
        python_callable=t2
    )
    
    third_function_execute = BashOperator(
        task_id='templated',
        params={'my_param': 'Parameter I passed in'},
        python_callable=t2
    )

    t1 >> [t2, t3]
