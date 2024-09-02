# ☀️🌪️ Weather Data Streaming - Apache Airflow and Apache Kafka

## Introduction
This project focusses on fetching weather data from OpenWeather API, more specifically Cape Town's weather data, sending it to Apache Kafka platofrm which is then later Streamed using Spark 
streaming to an Apache Cassandra db.

## Architecture
![Project Architecture Flow diagram AWS.](AWS Architecture.jpeg)

## Technology Used
1. Programming Language: Python
2. Docker Containerization
   - confluentinc/cp-kafka image used (official)
3. Apache
   - Airflow
   - Kafka
   - ZooKeeper
   - Spark
   - Cassandra
4. Control Center and Schema Registry
5. PostgreSQL
     

## Dataset
The data is outputted in a JSON format from the OpenWeather API I then formatted it to my requirements these are the fields in my dictionary for the weather data:
(definitions taken from OpenWeather Documentation)

> weather.main - Group of weather parameters (Rain, Snow, Clouds etc.)
> weather.description - Weather condition within the group
> weather.temp - Temperature. Unit Default: Celsius
> feels_like - Temperature. This temperature parameter accounts for the human perception of weather
> main.pressure - Atmospheric pressure on the sea level, hPa
> main.humidity - Humidity, %
> main.temp_min - Minimum temperature at the moment. Unit Default: Celsius
> main.temp_max - Maximum temperature at the moment. Unit Default: Celsius
> visibility - Visibility, meter. The maximum value of the visibility is 10 km
> wind.speed - Wind speed. Unit Default: meter/sec, Metric: meter/sec, Imperial: miles/hour
> wind.deg - Wind direction, degrees (meteorological)
> wind.gust - Wind gust. Unit Default: meter/sec, Metric: meter/sec
> id - City ID
> name - City name.

## Takeways
This was a pretty simple project to execute, however I encountered some issues setting up the Spark streaming, as encountered some compatibilty issues when using the latest versions of packages.

