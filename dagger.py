from email import message


try:
  import airflow
  import os
  import sys

  from datetime import timedelta,datetime
  from airflow import DAG

  # Operators
  from airflow.operators.python_operator import PythonOperator
  from airflow.operators.bash_operator import BashOperator
  from airflow.operators.email_operator import EmailOperator
  from airflow.utils.trigger_rule import TriggerRule

  from airflow.utils.task_group import TaskGroup


except Exception as e:
    print("Error  {} ".format(e))


default_args = {
    "owner": "airflow",
    'start_date': airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'email': ['rajeshunique.31gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}
dag  = DAG(dag_id="project", schedule_interval="@once", default_args=default_args, catchup=False)


  
#!/usr/bin/python
#import psycopg2
#import config
  
#def connect():
#    """ Connect to the PostgreSQL database server """
#    conn = None
#    try:
#        # read connection parameters
#        params = config()
  
        # connect to the PostgreSQL server
#        print('Connecting to the PostgreSQL database...')
#        conn = psycopg2.connect(**params)
          
#        # create a cursor
#        cur = conn.cursor()
          
#    # execute a statement
#        print('PostgreSQL database version:')
#        cur.execute('SELECT version()')
#  
#        # display the PostgreSQL database server version
#        db_version = cur.fetchone()
#        print(db_version)
         
    # close the communication with the PostgreSQL
#        cur.close()
#    except (Exception, psycopg2.DatabaseError) as error:
#        print(error)
#    finally:
#        if conn is not None:
#            conn.close()
#            print('Database connection closed.')
  
  
#if __name__ == '__main__':
#    connect() 
        
#port = 465  # For SSL
#password = "examplepass"

#context = ssl.create_default_context()

#First job, connecting to postgres and data fetch/push to other jobs
def t1():
  psycopg.connect()
  cursor.execute("Select email FROM documents WHERE customer='regular' AND aproval ='yes' ")
  context['ti'].xcom_push(key='key1', value=cursor.fetchall())
  cursor.execute("Select accountno FROM  WHERE customer='regular' AND aproval ='no' ")
  context['ti'].xcom_push(key='key2',value=cursor.fetchall())
  #Second job, deploys mail to the approved mass  
def t2():
  pending= context.get('ti').xcom_pull(key='key1')
  task_id='send_email',
  to='rajeshunique.31gmail.com',
  subject='approved',
  html_content=pending,  
    
def t3(): 
  message= context.get('ti').xcom_pull(key='key2')
  task_id='send_email',
  to='rajeshunique.31gmail.com',
  subject='Check approval',
  html_content=message,

  with DAG (
    dag_id='FLOWER',
    default_args=default_args,
    description='A simple workflow DAG',
    schedule_interval='@daily',
    start_date=datetime(2022,3,12),
    tags=['example'],
    ) as dag :
  
    first_function_execute = BashOperator(
        
        python_callable=t1
    )

    second_function_execute = BashOperator(
        
        python_callable=t2
    )
    
    third_function_execute = BashOperator(
        
        python_callable=t2
    )

    t1 >> [t2, t3]
