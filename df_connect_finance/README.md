# kafka-connect-finance-stock
This package offers a Kafka Connect finance stock connector that converts Yahoo finance stock data into viable Kafka Connect
SourceRecords.

### Features TODO
- [x] Support fetch API call from other API endpoint

### Config Values ###
| property       | example      | comments                                            |
|----------------|--------------|-----------------------------------------------------|
| __name__   |yahoo-stock-source            |The name of the connect. |
| __cuid__   |yahoo-stock-source            |The id of the connect. |
| __tasks.max__|1|Number of tasks running in paralle|
| __topic__      |source_stock  |The topic where the connect publishes the data.                         |
| __symbols__    |FB,TSLA       |List of stock symbols to process, seprated by commar.|
| __portfolio__  |Top 10 IT Service|list of predefined symbols, such as Top 10 Technology/US Banks/US Telecom/Life Insurance. This will overwrite __symbols__ setting.|
| __interval__   |20            |How often to check for new data, default 10 seconds. |
| __spoof__      |PAST          |If data comes from fake past data (PAST) or random (RAND) or NONE"|
| __schema.registry.uri__   |http://localhost:8081            |The URI to the Schema Registry. Create a new schema or use exist schema with same name to __topic__ for data validation.|


### Command Reference
* Add the connector through Kafka connect REST API.
```
curl -X POST \
  http://localhost:8083/connectors/ \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 682f7c97-bf2f-4a4c-893a-509655a43c6c' \
  -d '{
    "name": "finace_source_connector_01",
    "config": {
    	"cuid": "finace_source_connector_01",
        "connector.class": "com.datafibers.kafka.connect.FinanceSourceConnector",
        "tasks.max": "1",
        "topic": "stock_source",
        "portfolio": "Top 10 IT Service",
        "interval": "10",
        "spoof": "RAND",
        "schema.registry.uri": "http://localhost:8081"
    }
}'
```
* Consume the stock message.
```
kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic stock_source
```
* Use console avro producer to send message. (Note, do not input an _Enter_ which will cause producer exception)
```
kafka-avro-console-producer \
--broker-list localhost:9092 --topic test_source \
--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
```
