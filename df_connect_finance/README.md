# kafka-connect-finance-stock
This package offers a Kafka Connect finance stock connector that converts Yahoo finance stock data into viable Kafka Connect
SourceRecords.

### Features TODO
- [x] Support fetch API call from other API endpoint

### Config Values ###
* __topic__ - The topic to publish data to
* __symbols__ - How often to check for new file(s) to be processed
* __interval__ - If a file is modified should it be republished to kafka __(default : 10 seconds)__
* __spoofFlag__ - [PAST|NONE|OTHER] **true**, all below schema information is ignored.
The schema registry will create a subject called topic_value with Schema.STRING. __(default : false)__ 
* __schema.registry.uri__ - The URI to the Schema Registry  
* __schema.subject__ - The subject used to validate avro schema __(default : topic)__
