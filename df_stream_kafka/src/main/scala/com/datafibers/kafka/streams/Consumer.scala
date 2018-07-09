package com.datafibers.kafka.streams

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import java.util.Collections
import java.util.UUID
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import java.util.Collection

/**
 * Consumes the messages from given topic and broker
 */
object Consumer {

	def main(args: Array[String]) {

		val TOPIC = args(0)
		val broker = args(1)
		val partition = Integer.parseInt(args(0))

		val props = new Properties()
		props.put("bootstrap.servers", broker)

		props.put("zookeeper.connect", "localhost:2181");
		//Defines key type to send
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		//Defines value type to send
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("group.id", "treselle_group")
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");

		val consumer = new KafkaConsumer[String, String](props)

		consumer.subscribe(Collections.singletonList(TOPIC))
		while (true) {
			val records = consumer.poll(100)
			val iterable = records.iterator()
			while (iterable.hasNext()) {
				val data = iterable.next()
				println("Key is " + data.key() + " value is " + data.value() + " offset is " + data.offset() + "partition is "+data.partition());
			}
		}
	}
}