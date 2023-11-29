from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import json

cities = {'Lviv': [49.842957, 24.031111], 
          'Kyiv': [50.450001, 30.523333], 
          'Kharkiv': [49.988358, 36.232845], 
          'Odesa': [46.482952, 30.712481], 
          'Zhmerynka': [49.03705, 28.11201]}

def _process_weather(ti, city):
    info = ti.xcom_pull("extract_data{}".format(city)) 
    timestamp = info["dt"]
    temp = info["main"]["temp"]
    humidity = info["main"]["humidity"]
    cloudiness = info["main"]["clouds"]
    wind_speed = info["main"]["wind_speed"]
    return timestamp, temp, city, humidity, cloudiness, wind_speed
    
with DAG(dag_id="homework1_dag", schedule_interval="@daily", start_date=datetime(2023, 11, 28), catchup=True) as dag:
    db_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures
        (
        timestamp TIMESTAMP,
        temp FLOAT,
        city TEXT
        humidity FLOAT,
        cloudiness TEXT,
        wind_speed FLOAT
        );"""
    )

    check_api = HttpSensor(task_id="check_api",
        http_conn_id="weather_conn",
        endpoint="data/3.0/onecall",
        request_params={"appid": Variable.get("WEATHER_API_KEY"), 
                        "lat": 49.842957,
                        "lon": 24.031111})
    
    for city in cities:

        extract_data = SimpleHttpOperator(
            task_id="extract_data_{}".format(city),
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall",
            data={"appid": Variable.get("WEATHER_API_KEY"), 
                  "lat": city[0],
                  "lon": city[1]},
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True)
        
        process_data = PythonOperator(
            task_id="process_data_{}".format(city),
            python_callable=_process_weather,
            op_args=[city])
        
        inject_data = SqliteOperator(
            task_id="inject_data_{}".format(city), 
            sqlite_conn_id="airflow_conn", 
            sql= """
            INSERT INTO measures (timestamp, temp, city, humidity, cloudiness, wind_speed) VALUES (
                {{ti.xcom_pull(task_ids='process_data')[0]}}, 
                {{ti.xcom_pull(task_ids='process_data')[1]}},
                {{ti.xcom_pull(task_ids='process_data')[2]}},
                {{ti.xcom_pull(task_ids='process_data')[3]}},
                {{ti.xcom_pull(task_ids='process_data')[4]}},
                {{ti.xcom_pull(task_ids='process_data')[5]}});
            """, )
        
        db_create >> check_api >> extract_data >> process_data >> inject_data
