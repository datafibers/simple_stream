/*
 * Copyright 2016 David Tucker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datafibers.spark.stream;

import org.apache.flink.streaming.connectors.kafka.Kafka010AvroTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;

import java.util.Properties;

class VersionUtil {
  public static String getVersion() {
    try {
      return VersionUtil.class.getPackage().getImplementationVersion();
    } catch(Exception ex){
      return "0.0.1";
    }
  }

  public static void main(final String[] args) {

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "9092");
    properties.setProperty("group.id", "test");
    properties.setProperty("batch.size", "0");

    KafkaTableSource source = Kafka010AvroTableSource.builder()
            // set Kafka topic
            .forTopic("sensors")
            // set Kafka consumer properties
            .withKafkaProperties(properties)
            // set Table schema
            .withSchema(TableSchema.builder()
                    .field("sensorId", Types.LONG())
                    .field("temp", Types.DOUBLE())
                    .field("time", Types.SQL_TIMESTAMP()).build())
            // set class of Avro record
            .forAvroRecordClass(SensorReading.class)
            .build();

  }
}
