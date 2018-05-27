/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.datafibers.kafka.connect;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinanceSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FinanceSourceTask.class);

    public static final String KEY_FIELD = "symbol";
    public static final String VALUE_FIELD = "refresh_time";
    public static final String HTTP_HEADER_APPLICATION_JSON_CHARSET = "application/json; charset=utf-8";
    public static final String AVRO_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private String topic;
    private String symbols;
    private int interval;
    private String spoofFlag;
    private String schemaUri;
    private Schema dataSchema;

    private String cuid;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(FinanceSourceConnector.TOPIC_CONFIG);
        symbols = props.get(FinanceSourceConnector.STOCK_SYMBOLS_CONFIG);
        interval = Integer.parseInt(props.get(FinanceSourceConnector.REFRESH_INTERVAL_CONFIG)) * 1000;
        spoofFlag = props.get(FinanceSourceConnector.SPOOF_FLAG_CONFIG);
        cuid = props.get(FinanceSourceConnector.CUID);
        schemaUri = props.get(FinanceSourceConnector.SCHEMA_URI_CONFIG);

        // TODO create a stock schema and write to the schema registry
        addSchemaIfNotAvailable(topic);
        dataSchema = getBuildSchema(schemaUri, topic, "latest");
    }

    /**
     * Poll data from Yahoo finance pai
     *
     * @return
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();

        for (String symbol : symbols.split(",")) {
            String stockMsg = spoofFlag.equalsIgnoreCase("none") ?
                    YahooFinanceStockHelper.getStockJson(symbols, true) :
                    YahooFinanceStockHelper.getFakedStockJson(symbol, spoofFlag);
            records.add(new SourceRecord(offsetKey(symbol),
                    offsetValue(new JSONObject(stockMsg).getString("refresh_time")),
                    topic,
                    dataSchema, structDecodingFromJson(stockMsg)));

        }
        Thread.sleep(interval);
        return records;
    }

    @Override
    public void stop() {
    }

    private SchemaBuilder getInnerBuilder(Field field) {
        return getInnerBuilder(field, field.schema().getType());
    }

    private SchemaBuilder getInnerBuilder(Field field, Type type) {
        boolean hasDefault = field.defaultVal() != null;
        SchemaBuilder innerBuilder = null;
        switch (type) {
            case STRING: {
                innerBuilder = SchemaBuilder.string();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asText());
                break;
            }
            case INT: {
                innerBuilder = SchemaBuilder.int32();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asInt());
                break;
            }
            case LONG: {
                innerBuilder = SchemaBuilder.int64();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asLong());
                break;
            }
            case DOUBLE: {
                innerBuilder = SchemaBuilder.float64();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asDouble());
                break;
            }
            case FLOAT:{
                innerBuilder = SchemaBuilder.float32();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asDouble());
                break;
            }
            case BOOLEAN: {
                innerBuilder = SchemaBuilder.bool();
                if (hasDefault)
                    innerBuilder.defaultValue(field.defaultValue().asBoolean());
                break;
            }
            case UNION: {
                for (org.apache.avro.Schema schema : field.schema().getTypes()) {
                    if (!schema.getType().equals(NULL))
                        return getInnerBuilder(field, schema.getType());
                }
            }
            default:
                throw new ConnectException(
                        "Unable to build schema because there is no case for type " + field.schema().getType());
        }
        return innerBuilder;
    }

    private Map<String, String> offsetKey(String keyName) {
        return Collections.singletonMap(KEY_FIELD, keyName);
    }

    private Map<String, String> offsetValue(String value) {
        return Collections.singletonMap(VALUE_FIELD, value);
    }

    /**
     * Decode Json to struct according to schema form Confluent schema registry
     * @param line
     * @return struct of decoded
     */
    public Struct structDecodingFromJson(String line) {

        if (line.length() > 0) {
            JsonNode json = null;
            try {
                json = new ObjectMapper().readValue(line, JsonNode.class);
            } catch (IOException ex) {
                throw new ConnectException(String.format("Unable to parse %s into a valid JSON"), ex);
            }

            Struct struct = new Struct(dataSchema);
            Iterator<Entry<String, JsonNode>> iterator = json.getFields();
            while (iterator.hasNext()) {
                Entry<String, JsonNode> entry = iterator.next();
                Object value = null;
                org.apache.kafka.connect.data.Field theField = dataSchema.field(entry.getKey());
                if (theField != null) {
                    switch (theField.schema().type()) {
                        case STRING:
                        case BYTES: {
                            value = entry.getValue().asText();
                            break;
                        }
                        case INT32: {
                            value = entry.getValue().asInt();
                            break;
                        }
                        case INT64: {
                            value = entry.getValue().asLong();
                            break;
                        }
                        case FLOAT32:
                        case FLOAT64: {
                            value = entry.getValue().asDouble();
                            break;
                        }
                        case BOOLEAN: {
                            value = entry.getValue().asBoolean();
                            break;
                        }
                        default:
                            value = entry.getValue().asText();
                    }
                }
                System.out.println("STRUCT PUT -" + entry.getKey() + ":" + value);
                struct.put(entry.getKey(), value);

            }
            return struct;
        }
        return null;
    }

    /**
     * Utility to get the schema from schema registry
     * @param schemaUri
     * @param schemaSubject
     * @param schemaVersion
     * @return
     */
    private Schema getBuildSchema(String schemaUri, String schemaSubject, String schemaVersion) {

        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        String schemaString = null;
        BufferedReader br = null;

        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));

            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
            schemaString = responseJson.get("schema").asText();
            log.info("Schema String is " + schemaString);
        } catch (Exception ex) {
            throw new ConnectException("Unable to retrieve schema from Schema Registry", ex);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        org.apache.avro.Schema avroSchema = null;
        try {
            avroSchema = new Parser().parse(schemaString);
        } catch (SchemaParseException ex) {
            throw new ConnectException(
                    String.format("Unable to successfully parse schema from: %s", schemaString), ex);
        }

        SchemaBuilder builder;
        // TODO: Add other avro schema types
        switch (avroSchema.getType()) {
            case RECORD: {
                builder = SchemaBuilder.struct();
                break;
            }
            case STRING: {
                builder = SchemaBuilder.string();
                break;
            }
            case INT: {
                builder = SchemaBuilder.int32();
                break;
            }
            case LONG: {
                builder = SchemaBuilder.int64();
                break;
            }
            case FLOAT: {
                builder = SchemaBuilder.float32();
                break;
            }
            case DOUBLE: {
                builder = SchemaBuilder.float64();
                break;
            }
            case BOOLEAN: {
                builder = SchemaBuilder.bool();
                break;
            }
            case BYTES: {
                builder = SchemaBuilder.bytes();
                break;
            }
            default:
                builder = SchemaBuilder.string();
        }

        if (avroSchema.getFullName() != null)
            builder.name(avroSchema.getFullName());
        if (avroSchema.getDoc() != null)
            builder.doc(avroSchema.getDoc());

        if (RECORD.equals(avroSchema.getType()) && avroSchema.getFields() != null
                && !avroSchema.getFields().isEmpty()) {
            for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                boolean hasDefault = field.defaultValue() != null;

                SchemaBuilder innerBuilder = getInnerBuilder(field);

                if (hasDefault)
                    innerBuilder.optional();

                builder.field(field.name(), innerBuilder.build());
            }
        }

        return builder.build();
    }

    private void addSchemaIfNotAvailable(String subject) {

        String schemaRegistryRestURL = schemaUri + "/subjects/" + subject + "/versions";

        try {
            HttpResponse<String> schemaRes = Unirest.get(schemaRegistryRestURL + "/latest")
                    .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(schemaRes.getStatus() == 404) { // Add the meta sink schema
                Unirest.post(schemaRegistryRestURL)
                        .header("accept", HTTP_HEADER_APPLICATION_JSON_CHARSET)
                        .header("Content-Type", AVRO_REGISTRY_CONTENT_TYPE)
                        .body(new JSONObject().put("schema",
                                "{\"type\":\"record\"," +
                                        "\"name\": \"" + subject + "\"," +
                                        "\"fields\":[" +
                                        "{\"name\": \"refresh_time\", \"type\":\"string\"}," +
                                        "{\"name\": \"symbol\", \"type\": \"string\"}," +
                                        "{\"name\": \"company_name\", \"type\": \"string\"}, " +
                                        "{\"name\": \"exchange\", \"type\": \"string\"}," +
                                        "{\"name\": \"open_price\", \"type\": \"double\"}," +
                                        "{\"name\": \"ask_price\", \"type\": \"double\"}," +
                                        "{\"name\": \"ask_size\", \"type\": \"int\"}," +
                                        "{\"name\": \"bid_price\", \"type\": \"double\"}," +
                                        "{\"name\": \"bid_size\", \"type\": \"int\"}," +
                                        "{\"name\": \"price\", \"type\": \"double\"}]}"
                                ).toString()
                        ).asString();
                log.info("Subject - " + subject + " Not Found, so create it.");
            } else {
                log.info("Subject - " + subject + "Found.");
            }
        } catch (UnirestException ue) {
            ue.printStackTrace();
        }
    }
}
