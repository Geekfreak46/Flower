from datetime import timedelta
from textwrap import dedent
import psycopg2
from psycopg2 import Error
import smtplib, ssl
# Import modules

from airflow import DAG  
# Dag object to instantiate a DAG

# Operators to operate
from airflow.operators.bash import BashOperator
from airflow.utils.dates import now

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
  
  try:
    # Connect to an existing database
    connection = psycopg2.connect(user='postgres', password="pynative@#29",host="127.0.0.1",port="5432",database="postgres_db")

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT version();")
    # Fetch result
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

  except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")  
        
  port = 465  # For SSL
password = "examplepass"

context = ssl.create_default_context()

  def t1:
    psycopg.connect()
    cursor.execute("Select * FROM documents WHERE customer='regular' AND aproval ='yes' ")
    context['ti'].xcom_push(key='key1', value=cursor.fetchall()]
    
  def t2:
    pending= context.get('ti').xcom_pull(key='key1')
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
      message="Your account has been approved"
      server.login("my@gmail.com", password)    
      server.sendmail("my@gmail.com", "your@gmail.com", message)      
  
  with DAG(
    dag_id='FLOW'
    default_args=default_args,
    description='A simple workflow DAG',
    schedule_interval='@daily',
    start_date=now,
    tags=['example'],
) as dag:
  
    t1 = BashOperator(
        task_id='print_date',
        python_callable=t1
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
        python_callable=t2
    )
    
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    t1 >> [t2, t3]