# Kafka Stream
This repository contains example of using KStream to read and write with Kafka data.
# Concept
## Stream vs. Table
![](https://www.rittmanmead.com/blog/content/images/2017/09/StreamVSTableAcc.gif)

The KStream DSL uses three main abstractions. 
* A **KStream** is an abstraction of a record stream, where each data record represents a self-contained datum in the unbounded data set. 
* A **KTable** is an abstraction of a changelog stream, where each data record represents an update. More precisely, the value in a data record is considered to be an update of the last value for the same record key, if any (if a corresponding key doesn't exist yet, the update will be considered a create).
* a **GlobalKTable** is an abstraction of a changelog stream, where each data record represents an update. However, a GlobalKTable is different from a KTable in that it is fully replicated on each KafkaStreams instance. GlobalKTable also provides the ability to look up current values of data records by keys. This table-lookup functionality is available through join operations.

A KTable shardes the data between all running Kafka Streams instances, while a GlobalKTable has a full copy of all data on each instance. KTables are the way to go when working with state in Kafka Streams. Basically they are a materialized view on a topic where every message is an upsert of the last record with the same key. KTables eventually contains every change published to the underlying topic.

Normal KTables only contain the data of the partitions consumed by the Kafka Streams application. If you run N instances of your application, a KTable will contain roughly (total entries)/ N entries. Every instance consumes a different set of Kafka partitions resulting in different KTable content.

# Stream Windows
All the windowing operations output results at the end of the window. The output of the window will be single event based on the aggregate function used. The output event will have the time stamp of the end of the window and all window functions are defined with a fixed length.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-conceptual.png)
### Tumbling window
Tumbling window functions are used to segment a data stream into distinct time segments and perform a function against them, such as the example below. The key differentiators of a Tumbling window are that they repeat, do not overlap, and an event cannot belong to more than one tumbling window.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-tumbling-intro.png)
In Java
```
TimeWindows.of(windowSizeMs);

// The above is equivalent to the following code:
TimeWindows.of(windowSizeMs).advanceBy(windowSizeMs);
```
### Hopping window
Hopping window functions hop forward in time by a fixed period. It may be easy to think of them as Tumbling windows that can overlap, so events can belong to more than one Hopping window result set. To make a Hopping window the same as a Tumbling window, specify the hop size to be the same as the window size.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-hopping-intro.png)
In Java
```
TimeWindows.of(windowSizeMs).advanceBy(advanceMs);
```
### Sliding window
Sliding window functions, unlike Tumbling or Hopping windows, produce an output only when an event occurs. Every window will have at least one event and the window continuously moves forward along with data record timestamps. Like hopping windows, events can belong to more than one sliding window. In Kafka Streams, sliding windows are used only for join operations, and can be specified through the JoinWindows class.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-sliding-intro.png)
In Java, it is same to hopping window
```
TimeWindows.of(windowSizeMs).advanceBy(advanceMs);
```
### Session window 
Session window functions group events that arrive at similar times, filtering out periods of time where there is no data. It has three main parameters: timeout, maximum duration, and partitioning key (optional).

A session window begins when the first event occurs. If another event occurs within the specified timeout from the last ingested event, then the window extends to include the new event. Otherwise if no events occur within the timeout, then the window is closed at the timeout.

If events keep occurring within the specified timeout, the session window will keep extending until maximum duration is reached. The maximum duration checking intervals are set to be the same size as the specified max duration. For example, if the max duration is 10, then the checks on if the window exceed maximum duration will happen at t = 0, 10, 20, 30, etc.

When a partition key is provided, the events are grouped together by the key and session window is applied to each group independently. This partitioning is useful for cases where you need different session windows for different users or devices.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-session-intro.png)
In Java
```
SessionWindows.with(TimeUnit.MINUTES.toMillis(5));
```
## Excercise
1. Create schema with below command in Linux
```
curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data '{"schema": "{ \"type\": \"record\",\"namespace\":\"com.datafibers.kafka.streams.avro\",\"name\": \"Stock\",\"fields\":[{\"name\":\"refresh_time\",\"type\":\"string\"},{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"company_name\",\"type\":\"string\"},{\"name\":\"exchange\",\"type\":\"string\"},{\"name\":\"open_price\",\"type\":\"double\"},{\"name\":\"ask_price\",\"type\":\"double\"},{\"name\":\"ask_size\",\"type\":\"int\"},{\"name\":\"bid_price\",\"type\":\"double\"},{\"name\":\"bid_size\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"double\"}]}"}' \
 http://localhost:8081/subjects/stock_test1/versions
 ```
2. Start test environment with ```ops start mask0110000```
3. Add a finance stock source connect and send data to topic **stock_test1**
```
curl -X POST \
  http://localhost:8083/connectors/ \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: f8700637-6872-409b-9c09-d28c42b162cb' \
  -d '{
    "name": "finace_source_connector_01",
    "config": {
    	"cuid": "finace_source_connector_01",
        "connector.class": "com.datafibers.kafka.connect.FinanceSourceConnector",
        "tasks.max": "1",
        "topic": "stock_test1",
        "portfolio": "Top 10 IT Service",
        "interval": "5",
        "spoof": "RAND",
        "schema.registry.uri": "http://localhost:8081"
    }
}'
```
4. Run the _main()_ in [StockAvroExample.java](https://github.com/datafibers/simple_stream/blob/master/df_stream_kafka/src/main/java/com/datafibers/kafka/streams/StockAvroExample.java) with regular message format.

5. Verify in VM with below command
```
kafka-console-consumer --topic stock_out --from-beginning \
--zookeeper localhost:2181 \
--property print.key=true \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```
6. Run the _main()_ in [StockAvroExample2.java](https://github.com/datafibers/simple_stream/blob/master/df_stream_kafka/src/main/java/com/datafibers/kafka/streams/StockAvroExample2.java) with AVRO message format.

7. Verify in VM with below command
```
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic stock_out2 --from-beginning
```

## Reference
1. [KStream SerDe Guide](https://docs.confluent.io/current/streams/developer-guide/datatypes.html#streams-developer-guide-serdes)
2. [Confluent Wire Format](https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format)
3. [KStream DSL Windows](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#windowing)
4. [KSQL Overview](https://www.rittmanmead.com/blog/2017/10/ksql-streaming-sql-for-apache-kafka/)
