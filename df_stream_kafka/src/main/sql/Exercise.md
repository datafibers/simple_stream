# KSQL Exercise
## 1. Data Description
The [Citi Bike trip dataset from March 2017](https://s3.amazonaws.com/tripdata/201703-citibike-tripdata.csv.zip) is used as the source data. It contains basic details such as trip duration, 
ride start time, ride end time, station ID, station name, station latitude, and station longitude etc.

The [Citi Bike station dataset](https://feeds.citibikenyc.com/stations/stations.json) is used to enrich trip details for further analysis after data consumption. 
It contains basic details such as availableBikes, availableDocks, statusValue, and totalDocks etc.

## 2. Purpose
Enrich Citi Bike trip data in real time using joining and aggregation concepts.
Find the number of trips on a day to and from a particular station.
View trip details with station details and aggregate the trip count of each station.

## 3. Steps
### 3.1 Produce station details
Create the trip-details and station-details topics in Kafka using the below commands:
```
kafka-topics --create --zookeeper localhost:2181 --topic station-details --replication-factor 1 --partitions 1
kafka-topics --create --zookeeper localhost:2181 --topic trip-details --replication-factor 1 --partitions 1
```

Then, run the main method in [Producer.scala](https://github.com/datafibers/simple_stream/blob/master/df_stream_kafka/src/main/scala/com/datafibers/kafka/streams/Producer.scala) to send the trip history data csv and station json to the two topics above.
To run the producer, make sure proper version of scala is configured.
* Go to **File | Other Settings | Default Project Structure | Global Libraries**
* Click the + button at the top left hand side of the Window
* Select Scala **SDK 2.11.8**

To verify the data to be populated, use following console consumer commands.
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic station-details --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic trip-details --from-beginning
```
### 3.2 Join stream data and table data
To join the stream and table data, perform the following.

1. In the KSQL console, create a table for the station details to join it with the trip details while producing the stream using the below commands:
    ```$sql
    CREATE TABLE station_details_table (
    id BIGINT,
    stationName VARCHAR,
    availableDocks BIGINT,
    totalDocks BIGINT,
    latitude DOUBLE,
    longitude DOUBLE,
    statusValue VARCHAR,
    statusKey BIGINT,
    availableBikes BIGINT,
    stAddress1 VARCHAR,
    stAddress2 VARCHAR,
    city VARCHAR,
    postalCode VARCHAR,
    location VARCHAR,
    altitude VARCHAR,
    testStation BOOLEAN,
    lastCommunicationTime VARCHAR,
    landMark VARCHAR
    ) WITH (
    kafka_topic='station-details',
    value_format='JSON'
    )
    ```
1. In the KSQL console, create a stream for the trip details to enrich the data with the start station details and to 
find the trip count of each station for the day using the below commands:
    ```roomsql
    CREATE STREAM trip_details_stream (
    tripduration BIGINT,
    starttime VARCHAR,
    stoptime VARCHAR,
    start_station_id BIGINT,
    start_station_name VARCHAR,
    start_station_latitude DOUBLE,
    start_station_longitude DOUBLE,
    end_station_id BIGINT,
    end_station_name VARCHAR,
    end_station_latitude DOUBLE,
    end_station_longitude DOUBLE,
    bikeid INT,
    usertype VARCHAR,
    birth_year VARCHAR,
    gender VARCHAR
    ) WITH (
    kafka_topic='trip-details',
    value_format='DELIMITED'
    );
    ```





Group data.
Produce trip details.
View output.
View trip details with station details.
View aggregate trip count for each station.
