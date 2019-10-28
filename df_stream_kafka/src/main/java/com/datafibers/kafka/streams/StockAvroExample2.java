/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datafibers.kafka.streams;

import com.datafibers.kafka.streams.avro.Stock;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

/**
 * Computes word count for each stock
 *
 * Create Schema with namespace so that finance connect can write data into it *
 * curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
 *     --data '{"schema": "{ \"type\": \"record\",\"namespace\":\"com.datafibers.kafka.streams.avro\",\"name\": \"Stock\",\"fields\":[{\"name\":\"refresh_time\",\"type\":\"string\"},{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"company_name\",\"type\":\"string\"},{\"name\":\"exchange\",\"type\":\"string\"},{\"name\":\"open_price\",\"type\":\"double\"},{\"name\":\"ask_price\",\"type\":\"double\"},{\"name\":\"ask_size\",\"type\":\"int\"},{\"name\":\"bid_price\",\"type\":\"double\"},{\"name\":\"bid_size\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"double\"}]}"}' \
 * http://localhost:8081/subjects/stock_source/versions
 *
 * Create topic ahead to keep data stream
 * $ bin/kafka-topics --create --topic stock_out \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 *
 * Create consumer to verify
 * $ bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic stock_out --from-beginning
 * </pre>
 */
public class StockAvroExample2 {

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8002";
    final String STOCK_INPUT_TOPIC = "stock_source";
    final String STOCK_OUTPUT_TOPIC = "stock_out";

    final KafkaStreams streams = buildAvroFeed(bootstrapServers, schemaRegistryUrl,
            "/tmp/kafka-streams", STOCK_INPUT_TOPIC, STOCK_OUTPUT_TOPIC);
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  static KafkaStreams buildAvroFeed(final String bootstrapServers,
                                    final String schemaRegistryUrl,
                                    final String stateDir,
                                    final String inputTopic,
                                    final String outputTopic) throws IOException {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-avro-lambda-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "stock-avro-lambda-example-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

    final InputStream statsSchema = StockAvroExample2.class.getClassLoader()
            .getResourceAsStream("avro/com/datafibers/kafka/streams/stockcount.avsc");
    final Schema schema = new Schema.Parser().parse(statsSchema);

    final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
    // Note how we must manually call `configure()` on this serde to configure the schema registry
    // url.  This is different from the case of setting default serdes (see `streamsConfiguration`
    // above), which will be auto-configured based on the `StreamsConfiguration` instance.
    final boolean isKeySerde = false;
    genericAvroSerde.configure(
            Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
            isKeySerde);

    final StreamsBuilder builder = new StreamsBuilder();

    // read the source stream
    final KStream<String, Stock> feeds = builder.stream(inputTopic);

    // aggregate the new feed counts of by user
    final KTable<String, Long> aggregated = feeds
        // filter out old feeds
        .filter((dummy, value) -> value.getSymbol().contains("EPAM"))
        //.peek((key, value) -> System.out.println(key + value.getCompanyName()))
        // map the user id as key
        .map((key, value) -> new KeyValue<>(value.getSymbol(), value))
        //.peek((key, value) -> System.out.println(key + value.getCompanyName()))
        // no need to specify explicit serdes because the resulting key and value types match our default serde settings
        .groupByKey()
        .count();

    // write to the result topic, need to override serdes
    aggregated
            .toStream()
            .peek((key, value) -> System.out.println(key + " : " + value))
            .map(
                    (key, value) -> {
                      final GenericRecord clone = new GenericData.Record(schema);
                      clone.put("key", key);
                      clone.put("count", value);
                      return new KeyValue<>(key, clone);
                    }
            )
            .to(outputTopic, Produced.with(Serdes.String(), genericAvroSerde));

    return new KafkaStreams(builder.build(), streamsConfiguration);
  }

}
