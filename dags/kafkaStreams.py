from airflow.models.dag import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import json
import requests
from dotenv import load_dotenv
import os
import secrets
# Load environment variables from the .env file
load_dotenv()

apikey = os.getenv("WEATHER_API_KEY")


#cape town coordinates
coords = {"lat": -33.9258, "lon": 18.4232}

#weather url
url = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={apikey}".format(
    lat=coords["lat"],
    lon=coords["lon"],
    apikey=apikey
)

default_args = {
    "owner":"airflow",
    "start_date": datetime(2024,8,22,12,00)
}


#fetches weather data for cape town
def get_data():
    import logging 


    try:
        res = requests.get(url)

        if res.status_code == 200:
            data = res.json()
            return data
        else:
            logging.info(res.status_code)

    except Exception as e:
        logging.error(f"this is the error: {e}")
def format_data(res):
    import uuid
    data = {}
    data["id"] = str(uuid.uuid4())
    data['type_weather'] = res['weather'][0]['main']
    data['description'] = res['weather'][0]['description']
    data['temp'] =res['main']['temp']
    data['feels_like'] =res['main']['feels_like']
    data['temp_min'] =res['main']['temp_min']
    data['temp_max'] =res['main']['temp_max']
    data['pressure'] =res['main']['pressure']
    data['humidity'] =res['main']['humidity']
    data['visibility'] =res['visibility']
    data['wind_speed'] =res['wind']['speed']
    data['wind_deg'] =res['wind']['deg']
    data['country'] = res['sys']['country']
    data['city_name'] = res['name']

    return data


#sends data to kafka broker
def stream_data():
    from kafka import KafkaProducer

    import logging
    import time
    

    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            
        
            if res  is None:
                formatted_data = {}
                
            else:
        
                formatted_data = format_data(res)
                
                producer.send('weather_update',json.dumps(formatted_data).encode('utf-8'))
                

        except Exception as e:
            logging.error(f"an error has occurred: {e}")
            continue

    
#calls stream data task
with DAG('automation_user',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(task_id = 'stream_data_from_api',
        python_callable=stream_data
    )