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
### 3.1 Produce station details.
Create the trip-details and station-details topics in Kafka using the below commands:
```
./bin/kafka-topics --create --zookeeper localhost:2181 --topic station-details --replication-factor 1 --partitions 1
./bin/kafka-topics --create --zookeeper localhost:2181 --topic trip-details --replication-factor 1 --partitions 1
```
Join stream data and table data.
Group data.
Produce trip details.
View output.
View trip details with station details.
View aggregate trip count for each station.
