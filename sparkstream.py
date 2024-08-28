import logging
from datetime import datetime

# cassandra imports
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy

#pyspark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def create_keyspace(session):

    #CREATE KEYSPACE
    session.execute(
        """CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
    
    print('keyspace created successfully!')

#creates table for weather data in cassandra
def create_table(session):
    session.execute(
        """CREATE TABLE IF NOT EXISTS spark_streams.weather_data (
        id TEXT  PRIMARY KEY,
        type_weather TEXT,
        description TEXT,
        temp FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure FLOAT,
        humidity FLOAT,
        visibility FLOAT,
        wind_speed FLOAT,
        wind_deg INT,
        country  TEXT,
        city_name TEXT  
                    );
        """)
    print("table created successfully")


#function to insert data manually into table
def insert_data(session,**kwargs):
    import uuid
    
    print('inserting data...')

    id = str(uuid.uuid4())
    type_weather = kwargs.get('type_weather')
    description = kwargs.get('description')
    temp = kwargs.get('temp')
    feels_like = kwargs.get('feels_like')
    temp_min = kwargs.get('temp_min')
    temp_max = kwargs.get('temp_max')
    pressure = kwargs.get('pressure')
    humidity = kwargs.get('humidity')
    visibility = kwargs.get('visibility')
    wind_speed = kwargs.get('wind_speed')
    wind_deg = kwargs.get('wind_deg')
    country  = kwargs.get('country')
    city_name = kwargs.get('city_name')

    try:
        session.execute(
            """ INSERT INTO spark_streams.weather_data(
                id,type_weather,description,temp,feels_like,temp_min,temp_max,
                pressure,humidity,visibility,wind_speed,wind_deg,country,city_name)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (id,type_weather,description,temp,feels_like,temp_min,temp_max,
                pressure,humidity,visibility,wind_speed,wind_deg,country,city_name))
        logging.info(f"weather data inserted for city: {city_name}")
    
    except Exception as e:
        logging.error(f"could not not insert data, reason:{e}")


#create spark connection
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', '127.0.0.1') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

#creates cassandra connection 
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(contact_points=['localhost'], protocol_version = 4)

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        print(e)
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


#creates stream between kafka and spark
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather_update') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def handle_query_error(query):
    try:
        query.awaitTermination()
    except Exception as e:
        print(f"Streaming query failed: {e}")
        # Log more details or handle the error as needed


#deserialize data
def create_selection_df_from_kafka(spark_df):


    schema = StructType([
        StructField("id", StringType(), False),
        StructField("type_weather", StringType(), False),
        StructField("description", StringType(), False),
        StructField("temp", FloatType(), False),
        StructField("feels_like", FloatType(), False),
        StructField("temp_min", FloatType(), False),
        StructField("temp_max", FloatType(), False),
        StructField("pressure", FloatType(), False),
        StructField("humidity", FloatType(), False),
        StructField("visibility", FloatType(), False),
        StructField("wind_speed", FloatType(), False),
        StructField("wind_deg", FloatType(), False),
        StructField("country", StringType(), False),
        StructField("city_name", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    print(sel)

    return sel

#main
if __name__ == '__main__':

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn=spark_conn)
        
        selection_df = create_selection_df_from_kafka(spark_df=spark_df)
        session = create_cassandra_connection()

        if session is not None:

            create_keyspace(session=session)
            create_table(session=session)
            
            selection_df.printSchema()

            logging.info("Streaming is being started...")

            

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                            .option('checkpointLocation', '/tmp/checkpoint')
                            .option('keyspace', 'spark_streams')
                            .option('table', 'weather_data')
                            .start())

            handle_query_error(streaming_query)
