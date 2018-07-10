# Stock Analysis KSQL Demo

## 1. Setup environment
Once Kafka is started, start ksql server and ksql client.
```
$ksql-server-start /opt/confluent/etc/ksql/ksql-server.properties
$ksql http://localhost:9088
```

## 2. KSQL Console

### 2.1 Basic commands
Set properties
```
set 'auto.offset.reset' = 'earliest';
```
Show commands
```
show streams;
show tables;
show topics;
list topics;
show properties;
```
List current running streaming queries. Queries will continuously run as KSQL applications until they are manually terminated. 
Exiting KSQL client does not terminate persistent queries.
```roomsql
show queries;
```
To stop a query, use terminate commands. First, from the output of SHOW QUERIES,
identify a query ID you would like to terminate. For example, if you wish to terminate query ID 2:
```
terminate 2;
```
### 2.2 DDL commands
1. Create a stream mapping to stock_source topic and show its details.
    ```roomsql
    create stream s_stock_source with (kafka_topic='stock_source',value_format='avro');
    
    describe s_stock_source;
    describe extended s_stock_source;
    ```
1. Create a new stream by repartition the s_stock_source.
    ```roomsql
    create stream s_stock_rekeyed_by_symbol as select * from s_stock_source partition by symbol;
    
    describe extended s_stock_rekeyed_by_symbol;
    ```
1. Create a new table derived from S_STOCK_REKEYED_BY_SYMBOL. Note, topic is case sensitive. The topic created by stream is always in big letters.
For table, we must specify key and has key defined in the stream. We can also rekey from above.
    ```
    create table t_stock_source with (kafka_topic='S_STOCK_REKEYED_BY_SYMBOL',value_format='avro',key='symbol'); 
    
    select * from t_stock_source limit 5;
    
    describe extended t_stock_source;
    ```
1. Create another stream filtering symbol = 'ACN'.
    ```
    select * from s_stock_source limit 5;
    select * from s_stock_source where symbol = 'ACN' limit 5; -- in does not support yet
    
    create stream s_stock_source_acn as select * from s_stock_source where symbol = 'ACN'; --ctas
    create table t_max_open_price_by_symbol as select max(open_price) as max_open_price, symbol from S_STOCK_SOURCE group by symbol;
    ```
1. Perform joins. For now, onl.y left join is suppoorted and join a table only
    ```roomsql
    select s.symbol, s.open_price, t.max_open_price 
    from S_STOCK_SOURCE s left join t_max_open_price_by_symbol t on s.symbol = t.symbol
    where s.symbol='ACN';
    ```
1. Perform window operations. Note that WINDOW, GROUP BY and HAVING clauses can only be used if the from_item is a stream.
    ```
    create table t_win_t_stock_source
    as
    select count(symbol) as cnt_symbol, max(open_price) as max_open_price, symbol 
    from S_STOCK_SOURCE WINDOW TUMBLING (SIZE 30 SECONDS) 
    group by symbol having count(symbol) > 1;
    
    create table t_win_h_stock_source 
    as 
    select count(symbol) as cnt_symbol, max(open_price) as max_open_price, symbol 
    from S_STOCK_SOURCE WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS) 
    group by symbol;
    
    create table t_win_s_stock_source 
    as 
    select count(symbol) as cnt_symbol, max(open_price) as max_open_price, symbol 
    from S_STOCK_SOURCE WINDOW SESSION (30 SECONDS) 
    group by symbol;
    ```
### 2.4 Other commands
```roomsql
drop table t_stock_source;

print 'topic_name' from begining;
```
Note, SQL grammar defaults to uppercase formatting. You can use quotations (") to print topics that contain lowercase characters.

## 3. Limitations
The following functionality is not supported yet by KSQL.
* Message keys in Avro format are not supported. 
* Message keys in KSQL are always interpreted as STRING format, which means KSQL will ignore Avro schemas that have been registered for message keys and the key will be read using StringDeserializer.
* Avro schemas with nested fields are not supported yet. This is because KSQL does not yet support nested columns. This functionality is coming soon.

## 4. Other Requirement
1. Count the message received
2. Get the max open price and close price within 1hr window
3. Filter stock symbol = ACN
4. Filter volumn < 10
5. For price = 0 use average in windows 1hrs for mockup -- may not support by ksql
6. Join live stream with table value in 1hr window

## 5. Reference
1. [A kafka connect and stream example togerther](https://www.confluent.io/blog/building-real-time-streaming-etl-pipeline-20-minutes/)
1. [KSQL in Action](https://www.confluent.io/blog/ksql-in-action-real-time-streaming-etl-from-oracle-transactional-data)
1. [KSQL Windows](https://www.rittmanmead.com/blog/2017/10/ksql-streaming-sql-for-apache-kafka/)
1. [Schema Registry Overview and Compatibility Rules](https://www.infoq.com/presentations/contracts-streaming-microservices)
1. [Another KSQL Demo](https://www.rittmanmead.com/blog/2017/10/ksql-streaming-sql-for-apache-kafka/)






