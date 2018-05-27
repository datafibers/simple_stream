# kafka-connect-generic
This package offers a Kafka Connect source connector that converts flat files with
consistent, simple-to-parse files, such as CSV or JSON, into viable Kafka Connect
SourceRecords. 

The corresponding sink connector takes SinkRecords
from a Kafka topic and saves them to CSV or JSON files, preserving
the available schema (if any).

### Features TODO
- [x] Support CSV, JSON, TXT (string schema) by looking at the file extentions
- [ ] Add sink
- [ ] Add test cases
- [ ] Add file extension override
- [ ] Fix length mainframe files
- [ ] Customize file deliminators
- [ ] File validations, such as row count and date
- [ ] Schema validation on data precision
- [ ] Rename processed files

### Config Values ###
* __topic__ - The topic to publish data to  
* __file.location__ - The location of the file(s) to process  
* __file.glob__ - The glob criteria  
* file.glob.interval - How often to check for new file(s) to be processed __(default : 10 seconds)__
* file.overwrite - If a file is modified should it be republished to kafka __(default : false)__ 
* schema.ignored - If a file schema is ignored. When this property is **true**, all below schema information is ignored. 
The schema registry will create a subject called topic_value with Schema.STRING. __(default : false)__ 
* __schema.registry.uri__ - The URI to the Schema Registry  
* schema.subject - The subject used to validate avro schema __(default : topic)__
* schema.version - The version of the subject to be used for schema validation __(default : latest version of the subject)__  

