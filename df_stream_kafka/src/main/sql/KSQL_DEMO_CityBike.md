# City Bike KSQL Demo
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
$kafka-topics --create --zookeeper localhost:2181 --topic station-details --replication-factor 1 --partitions 1
$kafka-topics --create --zookeeper localhost:2181 --topic trip-details --replication-factor 1 --partitions 1
```

Then, run the _main()_ method in [Producer.scala](https://github.com/datafibers/simple_stream/blob/master/df_stream_kafka/src/main/scala/com/datafibers/kafka/streams/Producer.scala) to send the trip history data csv and station json to the two topics above.
To run the producer, make sure proper version of scala is configured.
* Go to **File | Other Settings | Default Project Structure | Global Libraries**
* Click the + button at the top left hand side of the Window
* Select Scala **SDK 2.11.8**

To verify the data to be populated, use following console consumer commands.
```
$kafka-console-consumer --bootstrap-server localhost:9092 --topic station-details --from-beginning
$kafka-console-consumer --bootstrap-server localhost:9092 --topic trip-details --from-beginning
```
### 3.2 Join stream data and table data
To join the stream and table data, perform the following.

1. Enter KSQL console by starting KSQL server and client.
    ```roomsql
    $ksql-server-start /opt/confluent/etc/ksql/ksql-server.properties
    $ksql http://localhost:9088
    ```

1. In the KSQL console, create a table for the station details to join it with the trip details while producing the stream using the below KSQL commands. 
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
find the trip count of each station for the day using the below KSQL commands:
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
    
1. Join the stream with the station details table to get fields such as availableBikes, totalDocks, and 
availableDocks, using the station ID as the key. Then, extract the start time in date format as the timestamp 
to get only the day from the start time and get the started trip count details in the day. Join the streamed trip 
details with the station details table since KSQL **DOES NOT** allow joining of two streams or two tables.
    ```roomsql
    CREATE STREAM citibike_trip_start_station_details WITH (value_format='JSON') 
    AS
    SELECT
    a.tripduration,
    a.starttime,
    STRINGTOTIMESTAMP(a.starttime, 'yyyy-MM-dd HH:mm:ss') AS startime_timestamp,
    a.start_station_id,
    a.start_station_name,
    a.start_station_latitude,
    a.start_station_longitude,
    a.bikeid,
    a.usertype,
    a.birth_year,
    a.gender,
    b.availableDocks AS start_station_availableDocks,
    b.totalDocks AS start_station_totalDocks,
    b.availableBikes AS start_station_availableBikes,
    b.statusValue AS start_station_service_value
    FROM
    trip_details_stream a
    LEFT JOIN station_details_table b ON a.start_station_id=b.id
    ```
### 3.3 Group data.
To group data based on the station details and date, perform the following.

1. Format the date as YYYY-MM-DD from the long timestamp to group by date in the start trip details using the below KSQL commands:
    ```roomsql
    CREATE STREAM citibike_trip_start_station_details_with_date 
    AS
    SELECT
    TIMESTAMPTOSTRING(startime_timestamp, 'yyyy-MM-dd') AS DATE,
    starttime,
    start_station_id,
    start_station_name
    FROM
    citibike_trip_start_station_details
    ```
1. Create a table by grouping the data based on the date and the stations for finding the started trip counts and the ended trip counts of each station for the day using the below KSQL commands:
   ```roomsql
   CREATE TABLE start_trip_count_by_stations 
   AS
   SELECT
   DATE,
   start_station_id,
   start_station_name,
   COUNT(*) AS trip_count
   FROM
   citibike_trip_start_station_details_with_date
   GROUP BY
   DATE,
   start_station_name,
   start_station_id
   ;

   CREATE TABLE end_trip_count_by_stations 
   AS
   SELECT
   DATE,
   end_station_id,
   end_station_name,
   COUNT(*) AS trip_count
   FROM
   citibike_trip_end_station_details_with_date
   GROUP BY
   DATE,
   end_station_name,
   end_station_id;
   ```
   
   List the topics to check whether they're created for persistent queries in KSQL console
   ```
   show topics;
   ```
### 3.4 View Trip Details
To view the trip details with the station details, perform the following:

1. Consume the message using the topic CITIBIKE_TRIP_START_STATION_DETAILS to view the extra fields added to the trip 
details from the station details table and to extract the long timestamp field from the start and end times using the 
below commands:
    ```roomsql
    $kafka-console-consumer --bootstrap-server localhost:9092 --topic CITIBIKE_TRIP_START_STATION_DETAILS --from-beginning
    ```

1. Consume the message using the topic CITIBIKE_TRIP_END_STATION_DETAILS using the below commands:
    ```roomsql
    $kafka-console-consumer --bootstrap-server localhost:9092 --topic CITIBIKE_TRIP_END_STATION_DETAILS --from-beginning
    ```

From the above console output, it is evident that the fields of the station details are added to the trip while producing the trip details.

### 3.5 Viewing Aggregate Trip Count of Each Station  

To view the aggregate trip count of each station based on the date, perform the following:

1. Consume the message via the console to check the trip counts obtained on the stream using the below commands:
    ```
    $kafka-console-consumer --bootstrap-server localhost:9092 --topic START_TRIP_COUNT_BY_STATIONS --from-beginning
    ```

    From the above console output, it is evident that the trip counts are updated and added to the topic for each day when producing the message. This data can be filtered to the latest trip count in consumer for further analysis.

1. Obtain the end trip count details based on the stations using the below commands:
    ```
    $kafka-console-consumer --bootstrap-server localhost:9092 --topic END_TRIP_COUNT_BY_STATIONS --from-beginning
    ```

## Reference
1. [KSQL Syntax Reference](https://github.com/confluentinc/ksql/blob/0.1.x/docs/syntax-reference.md#syntax-reference)