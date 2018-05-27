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
| __topic__      |source_stock  |The topic to publish data to                         |
| __symbols__    |FB,TSLA       |List of stock symbols to process, seprated by commar.|
| __portfolio__  |Top 10 IT Service|list of predefined symbols, such as Top 10 Technology/US Banks/US Telecom/Life Insurance. This will overwrite __symbols__ setting|
| __interval__   |20            |How often to check for new data, default 10 seconds. |
| __spoof__      |PAST          |If data comes from fake past data (PAST) or random (RAND) or NONE"|
| __chema.registry.uri__   |http://localhost:8081            |The URI to the Schema Registry. |
| __schema.subject__   |topic_stock            |The subject used to validate avro schema. When it is not specified or "n/a", create a new schema with same name to __topic__|


### Rest API Reference
