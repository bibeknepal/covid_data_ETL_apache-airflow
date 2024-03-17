from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import timedelta
from minio import Minio
import pendulum,requests,os,math
import pandas as pd
import mysql.connector



local_timezone  = pendulum.timezone("Asia/Kathmandu")


default_args = {
    'owner':'bibek',
    'retries':'1',
    'retry_delay':timedelta(minutes=1),
    'start_date':local_timezone.datetime(2024,3,13)
}

def extract_data():
    url = "https://covid-193.p.rapidapi.com/statistics"

    headers = {
        "X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
        "X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
    }
    # headers = {
	# "X-RapidAPI-Key": "6a139b3d89msh87b55137953649ap113f04jsncc85666413b5",
	# "X-RapidAPI-Host": "covid-193.p.rapidapi.com"
    # }
    response = requests.get(url, headers=headers)
    data = response.json()
    data = data.get('response')

    covid_data = [{
                "Date": entry.get('day'),
                "Continent": entry.get('continent'),
                "Country": entry.get('country'),
                "Population": entry.get('population'),
                "Total Tests": entry.get('tests').get('total'),
                "Total Cases": entry.get('cases').get('total'),
                "Active Cases": entry.get('cases').get('active'),
                "Critical Cases": entry.get('cases').get('critical'),
                "Recovered": entry.get('cases').get('recovered'),
                "Total Deaths": entry.get('deaths').get('total')
            }for entry in data]
        
    df = pd.DataFrame(covid_data)
    print(df)
    df.to_csv('data/covid_data.csv',index=False)   
    print("Data Extracted Successfully!!!")
    
    
def convert_to_int(x):
    if not isinstance(x,int) and x != "UNKNOWN":
        return int(math.floor(float(x)))
    else:
        return x


def clean_text(x):
    x = x.replace('-',' ')
    return x


def transform_data():
    df = pd.read_csv("data/covid_data.csv")
    df = df.fillna("UNKNOWN")
    df['Continent'] = df['Continent'].apply(clean_text)
    df['Country'] = df['Country'].apply(clean_text)
    columns_to_convert = ["Population","Total Tests","Total Cases","Active Cases","Critical Cases","Recovered","Total Deaths"]
    for col in columns_to_convert:
        df[col] = df[col].apply(convert_to_int)
    print(df)
    df.to_csv("data/covid_data.csv",index=False)
    print("Data Transformed Successfully!!!")
    
def get_database_credentials(ti):
    mysql_config ={
        'host':os.getenv("MYSQL_HOST"),
        'user':os.getenv("MYSQL_ROOT_USER"),
        'password':os.getenv("MYSQL_ROOT_PASSWORD"),
        'database':os.getenv("MYSQL_DATABASE")
        
    }
    ti.xcom_push(key = 'mysql_config',value = mysql_config)
    
    
def create_table(ti):
    mysql_config = ti.xcom_pull(task_ids = "get_database_credentials",key = 'mysql_config')
    connection = mysql.connector.connect(**mysql_config)
    connection.autocommit = True
    cursor = connection.cursor() 
    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS covid_data (
                    Date DATE,
                    Continent VARCHAR(30),
                    Country VARCHAR(30),
                    Population VARCHAR(30),
                    TotalTests VARCHAR(30),
                    TotalCases VARCHAR(30),
                    ActiveCases VARCHAR(30),
                    CriticalCases VARCHAR(30),
                    Recovered VARCHAR(30),
                    TotalDeaths VARCHAR(30),
                    PRIMARY KEY (Date, Country)
                    );
                    """)
    connection.close()
    print("Table created Successfully.")
    

def load_data_mysql(ti):
    mysql_config = ti.xcom_pull(task_ids = "get_database_credentials",key = 'mysql_config')
    connection = mysql.connector.connect(**mysql_config)
    connection.autocommit = True
    cursor = connection.cursor()

    df = pd.read_csv('data/covid_data.csv')
    table_name = "covid_data"
    
    for index, row in df.iterrows():
        # Construct SQL query to insert row into MySQL with INSERT IGNORE
        sql_query = f"""
            INSERT IGNORE INTO {table_name} (Date, Continent, Country, Population, TotalTests, TotalCases, ActiveCases, CriticalCases, Recovered, TotalDeaths)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Parameters for the query
        params = (
            row['Date'], row['Continent'], row['Country'], row['Population'], row['Total Tests'], row['Total Cases'],
            row['Active Cases'], row['Critical Cases'], row['Recovered'], row['Total Deaths']
        )
        cursor.execute(sql_query, params)

    connection.close()
    print("Data inserted successfully.")
    
    
def load_data_minio():
    client = Minio(
        endpoint = os.getenv("MINIO_ENDPOINT"),
        secure=False,
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD")
    )
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
        
    else:
        print(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")
        
    with open("data/covid_data.csv", "rb") as file_data:
            # Get the length of the file data
            file_length = os.fstat(file_data.fileno()).st_size

            # Upload CSV data to MinIO bucket
            client.put_object(
                bucket_name=MINIO_BUCKET_NAME,
                object_name="covid_data.csv",
                data=file_data,
                length=file_length,
                content_type='application/csv'
            )

            print("Data uploaded successfully to MinIO.")        
        
with DAG(
    dag_id = 'etl_covid-data_api',
    default_args=default_args,
    schedule_interval= "10 * * * *",
    catchup = False
) as dag:
    task1 = PythonOperator(
        task_id = 'extract_data_from_api',
        python_callable= extract_data
    )
    task2 = PythonOperator(
        task_id = "transform_data",
        python_callable= transform_data
    )
    task3 = PythonOperator(
        task_id = "get_database_credentials",
        python_callable= get_database_credentials
    )
    
    task4 = PythonOperator(
        task_id = 'create_table_to_insert_data',
        python_callable=create_table
    )
    
    task5 = PythonOperator(
        task_id = 'Load_data_in_mysql',
        python_callable=load_data_mysql
    )
    task6 = PythonOperator(
        task_id= 'Load_data_in_minio_bucket',
        python_callable= load_data_minio
    )
    
    task1.set_downstream(task2)
    task2.set_downstream(task3)
    task2.set_downstream(task6)
    task3.set_downstream(task4)
    task4.set_downstream(task5)

