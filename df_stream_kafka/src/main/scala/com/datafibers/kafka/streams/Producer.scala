package com.datafibers.kafka.streams

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.io.Source
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.Callback
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.collection.mutable.ListBuffer
import util.control.Breaks._

object Producer {

  def main(args: Array[String]) {

    val topic_trip = "trip-details"
    val topic_station = "station-details"
    val broker = "localhost:9092"
    var producer_trip: KafkaProducer[String, String] = null
    var producer_station: KafkaProducer[String, String] = null
    val startTime = System.currentTimeMillis()
    val csvPath = "/Users/will/Documents/Coding/GitHub/simple_stream/df_stream_kafka/src/main/resources/201703-citibike-tripdata.csv";

    println("Kafka producer initiated ...... ")
    println("Kafka producer started ...... ")
    println("Producing with broker list " + broker)

    val props = new Properties()
    props.put("bootstrap.servers", broker)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //Set acknowledgement for producer requests.      
    props.put("acks", "all");
    //If the request fails, the producer can automatically retry,
    props.put("retries", "0");
    props.put("producer.type", "async")

    //Reduce the no of requests less than 0   
    props.put("linger.ms", "1");

    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
    props.put("buffer.memory", "33554432");
    props.put("request.timeout.ms", "250000");

    producer_trip = new KafkaProducer[String, String](props)
    producer_station = new KafkaProducer[String, String](props)

    println("started trip_data producer " + "in topic " + topic_trip)
    produceTripData(producer_trip, topic_trip, csvPath);
    producer_trip.close;
    producer_trip.flush;

    println("started station_data producer " + "in topic " + topic_station)
    producerStationData(producer_station, topic_station);

    producer_station.close
    producer_station.flush
  }

  /**
   * Produces trip details to topic by reading data from the file
   */
  def produceTripData(producer: KafkaProducer[String, String], TOPIC: String, csvPath: String) {
    //File path to read citibike trip data
    val records = readFile(csvPath)
    var key: Integer = 0;
    for (recordStr: String <- records) {
      key += 1;
      if (key > 1) {
        val record = new ProducerRecord(TOPIC, String.valueOf(key), recordStr)
        producer.send(record, new Callback() {
          def onCompletion(metadata: RecordMetadata, e: Exception) {
            if (e != null) {
              e.printStackTrace();
            }
            println("Sent:" + recordStr + ", Partition: " + metadata.partition() + ", Offset: "
              + metadata.offset());
          }
        })
      }

    }
  }

  /**
   * Produces station details to topic by reading data from the URL
   */
  def producerStationData(producer: KafkaProducer[String, String], TOPIC: String) {
    //Reads the data from source URL
    val json = Source.fromURL("https://feeds.citibikenyc.com/stations/stations.json")
    val jsonStr = json.mkString
    val mapper = new ObjectMapper() with ScalaObjectMapper
    //To define for Scala
    mapper.registerModule(DefaultScalaModule)
    val jsonMap = mapper.readValue[Map[String, Object]](jsonStr)
    val stationlistAny: Object = jsonMap.getOrElse("stationBeanList", "")
    val stationList: List[Map[String, Object]] = stationlistAny.asInstanceOf[List[Map[String, Object]]]
    var key: Integer = 0;
    for (station <- stationList) {
      key += 1;
      val message = mapper.writeValueAsString(station)
      val record = new ProducerRecord(TOPIC, String.valueOf(key), message)
      producer.send(record, new Callback() {
        def onCompletion(metadata: RecordMetadata, e: Exception) {
          if (e != null) {
            e.printStackTrace();
          }
          println("Sent:" + message + ", Partition: " + metadata.partition() + ", Offset: "
            + metadata.offset());
        }
      })
    }
  }

  /**
   * Reads the content from the given file path.
   */
  def readFile(filePath: String): List[String] = {
    //Gets the source from given file
    val source = Source.fromFile(filePath)
    //Converts the lines in file into list of string
    val fileContent = source.getLines().toList
    source.close
    return fileContent
  }
}