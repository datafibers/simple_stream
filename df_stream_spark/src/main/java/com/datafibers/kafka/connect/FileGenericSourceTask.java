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
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileGenericSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FileGenericSourceTask.class);

    public static final String DF_METADATA_TOPIC = "df_meta";
    public static final String DF_METADATA_SCHEMA_SUBJECT = "df_meta";
    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";
    public static final String FILENAME_EXT_PROCESSING = ".processing";
    public static final String FILENAME_EXT_PROCESSED = ".processed";

    private String topic;
    private String location;
    private String glob;
    private int interval;
    private boolean overwrite;
    private boolean schemaValidate;
    private String schemaUri;
    private String schemaSubject;
    private String schemaVersion;
    private Schema dataSchema;

    private List<Path> processedPaths = new ArrayList<Path>();
    private List<Path> inProgressPaths = new ArrayList<Path>();

    private String filename;
    private File fileInProcessing; //.processing
    private File fileProcessed;    //.processed
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private Long streamOffset;
    private String cuid;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(FileGenericSourceConnector.TOPIC_CONFIG);
        location = props.get(FileGenericSourceConnector.FILE_LOCATION_CONFIG);
        glob = props.get(FileGenericSourceConnector.FILE_LOCATION_CONFIG)
                .concat(props.get(FileGenericSourceConnector.FILE_GLOB_CONFIG));
        interval = Integer.parseInt(props.get(FileGenericSourceConnector.FILE_INTERVAL_CONFIG)) * 1000;
        overwrite = Boolean.valueOf(props.get(FileGenericSourceConnector.FILE_OVERWRITE_CONFIG));
        cuid = props.get(FileGenericSourceConnector.CUID);

        findMatch();
        schemaUri = props.get(FileGenericSourceConnector.SCHEMA_URI_CONFIG);
        String schemaIgnored = props.get(FileGenericSourceConnector.SCHEMA_IGNORED);

        if (schemaIgnored.equalsIgnoreCase("true")) {
            schemaValidate = false;

        } else {
            // Get avro schema from registry and build proper schema POJO from it
            schemaValidate = true;
            schemaSubject = props.get(FileGenericSourceConnector.SCHEMA_SUBJECT_CONFIG);
            schemaVersion = props.get(FileGenericSourceConnector.SCHEMA_VERSION_CONFIG);
            dataSchema = getBuildSchema(schemaUri, schemaSubject, schemaVersion);
        }
    }


    /**
     * file start meta_data, file meta, status, timestamp, cuid
     * file end meta_data,   row_count, status, timestamp, cuid
     *
     * file name, file size, file owner, (rowCount,) status, timestamp, (processed time,) cuid
     * file name, file size, file owner, rowCount,   status, timestamp,  processed time,  cuid
     *
     * @return
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();
        Path currentPath = null;

        if (!inProgressPaths.isEmpty()) {
            try {
                currentPath = inProgressPaths.remove(0);
                processedPaths.add(currentPath);
                filename = currentPath.getFileName().toString();
                fileInProcessing = FileUtils.getFile(currentPath.toString() + FILENAME_EXT_PROCESSING);
                fileProcessed = FileUtils.getFile(currentPath.toString() + FILENAME_EXT_PROCESSED);
                File fileToBeProcessed = FileUtils.getFile(currentPath.toString());

                // Sending file metadata to metadataTopic when file starts to be read
                sendFileMetaToDFMetaTopic(fileToBeProcessed, cuid, "STARTED", 0L, records);

                FileUtils.moveFile(FileUtils.getFile(currentPath.toString()), fileInProcessing);

                stream = new FileInputStream(fileInProcessing);

                Map<String, Object> offset = context.offsetStorageReader()
                        .offset(Collections.singletonMap(FILENAME_FIELD, filename));
                if (offset != null && !overwrite) {
                    log.info("Found previous offset, will not process {}", filename);
                    return null;
                } else
                    streamOffset = 0L;

                reader = new BufferedReader(new InputStreamReader(stream));
                log.info("Opened {} for reading", filename);
            } catch (IOException e) {
                e.printStackTrace();
                log.debug("===== error exception message1: " + e.getMessage());

                throw new ConnectException(String.format("Unable to open file %", filename), e);
            }
        } else {
            log.warn("********* Waiting for file that meets the glob criteria! *********");
            synchronized (this) {
                this.wait(interval);
                findMatch();
            }
            return null;
        }

        // ArrayList<SourceRecord> records = new ArrayList<SourceRecord>();
        //StringBuilder fileContent = new StringBuilder();

        try {
            final BufferedReader readerCopy;
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null)
                return null;

            int nread = 0;
            while (readerCopy.ready()) {
                nread = readerCopy.read(buffer, offset, buffer.length - offset);
                log.trace("Read {} bytes from {}", nread, filename);

                if (nread > 0) {
                    offset += nread;
                    if (offset == buffer.length) {
                        char[] newbuf = new char[buffer.length * 2];
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        buffer = newbuf;
                    }

                    String line;
                    do {
                        line = extractLine();

                        if (line != null && !line.trim().isEmpty()) {
                            line = line.trim();
                            log.trace("Read a line from {}", filename);

                            if (records == null)
                                records = new ArrayList<>();
                                /* records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic,
                                dataSchema, structDecodingRoute(line, filename)));*/

                            if (schemaValidate) {
                                records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic,
                                        dataSchema, structDecodingRoute(line, filename)));
                            } else {
                                log.info("STRING SCHEMA Processing");
                                records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic,
                                        Schema.STRING_SCHEMA, line));
                            }
                        }
                        
                        new ArrayList<SourceRecord>();
                    } while (line != null);
                }
            }


            // Finish processing and rename as processed.
            FileUtils.moveFile(fileInProcessing, fileProcessed);

            if (nread <= 0)
                synchronized (this) {
                    this.wait(1000);
            }

            // Sending file metadata to metadataTopic after file completes reading
            sendFileMetaToDFMetaTopic(fileProcessed, cuid, "COMPLETED", streamOffset, records);
            return records;

        } catch (IOException e) {
            log.debug("Exception message2: " + e.getMessage());
            e.printStackTrace();

            throw new ConnectException(String.format("Unable to read file %", filename), e);
        }
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    log.trace("Closed input stream");
                }
            } catch (IOException e) {
                log.error("Failed to close FileGenericSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    /**
     * Looks for files that meet the glob criteria. If any found they will be added to the list of
     * files to be processed
     */
    private void findMatch() {
        final PathMatcher globMatcher = FileSystems.getDefault().getPathMatcher("glob:".concat(glob));

        try {
            Files.walkFileTree(Paths.get(location), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attributes)
                        throws IOException {
                    if (globMatcher.matches(path)) {
                        if (!processedPaths.contains(path)) {
                            inProgressPaths.add(path);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SchemaBuilder getInnerBuilder(Field field) {
        return getInnerBuilder(field, field.schema().getType());
    }

    private SchemaBuilder getInnerBuilder(Field field, Type type) {
        boolean hasDefault = field.defaultValue() != null;
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
            case FLOAT: {
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

    private String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)
                streamOffset += newStart;
            return result;
        } else {
            return null;
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    /**
     * Decode Csv to struct according to schema form Confluent schema registry
     * @param line
     * @return struct of decoded
     */
    public Struct structDecodingRoute(String line, String file_name) {
        Struct st = null;
        switch (FilenameUtils.getExtension(file_name).toLowerCase()) {
            case "json":
                if(line.startsWith("{")) {
                    log.info("Read line @@" + line + "@@ from Json File " + file_name);
                    st = structDecodingFromJson(line);
                } else {
                    log.warn("Ignored line @@" + line + "@@ from Json File " + file_name);
                }
                break;
            case "csv":
            case "tsv":
                log.info("Read line @@" + line + "@@ from Csv File " + file_name);
                st = structDecodingFromCsv(line);
                break;
            default:
                log.info("Default file extension not processing");
        }
        return st;
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
                throw new ConnectException(String.format("Unable to parse %s into a valid JSON", filename),
                        ex);
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
     * Decode Csv to struct according to schema form Confluent schema registry
     * @param line
     * @return struct of decoded
     */
    public Struct structDecodingFromCsv(String line) {
        if (line.length() > 0) {
            Struct struct = new Struct(dataSchema);
            JsonNode json = null;
            try {
                // TODO support other type of files fro here
                CSVParser csvParser = CSVFormat.EXCEL
                        .withIgnoreEmptyLines()
                        .withIgnoreHeaderCase()
                        .withRecordSeparator('\n').withQuote('"')
                        .withEscape('\\').withDelimiter(',').withTrim()
                        .parse(new StringReader(line));

                // Since this is single line parser, we get element 0 only
                CSVRecord entry = csvParser.getRecords().get(0);
                List<org.apache.kafka.connect.data.Field> fields = dataSchema.fields();
                int schema_fields_size = fields.size();
                log.info("schema_fields_size = " + schema_fields_size);

                for (int index = 0; index <= schema_fields_size - 1; index++) {
                    Object value = null;
                    org.apache.kafka.connect.data.Field theField = fields.get(index);
                    log.info("printed indexed " + index + " fields: " + theField.name() + ":" + theField.schema().type());
                    if (theField != null) {
                        switch (theField.schema().type()) {
                            case STRING: {
                                value = entry.get(index);
                                break;
                            }
                            case INT32: {
                                value = Integer.parseInt(entry.get(index));
                                break;
                            }
                            case INT64: {
                                value = Long.parseLong(entry.get(index));
                                break;
                            }
                            case FLOAT32:
                            case FLOAT64: {
                                value = Float.parseFloat(entry.get(index));
                                break;
                            }
                            case BOOLEAN: {
                                value = Boolean.parseBoolean(entry.get(index));
                                break;
                            }
                            default:
                                value = entry.get(index);
                        }
                    }
                    struct.put(theField.name(), value);
                }
            } catch (IOException ex) {
                throw new ConnectException(String.format("Unable to parse %s into a valid CSV", filename), ex);
            }
            return struct;
        }
        return null;
    }

    /**
     * Send metadata to the df_repository
     */

    private void sendFileMetaToDFMetaTopic(File file, String cuid, String status, long streamOffset,
                                          ArrayList<SourceRecord> records) {

        Schema metaSchema = getBuildSchema(schemaUri, DF_METADATA_SCHEMA_SUBJECT, "latest");
        System.out.println(metaSchema.fields());

        try {
            String fileSize = Long.toString(file.length());
            String fileName = file.getName().replace(FILENAME_EXT_PROCESSING, "").replace(FILENAME_EXT_PROCESSED, "");
            String fileOwner = Files.getOwner(file.toPath()).getName();
            String lastModifiedTimestamp = new Timestamp(file.lastModified()).toString();
            String currentTimestamp = new Timestamp(System.currentTimeMillis()).toString();
            Long currentTimeMillis = System.currentTimeMillis();

            Struct struct = new Struct(metaSchema);
            struct.put("cuid", cuid == null ? "N/A, not submit form df" : cuid)
                    .put("file_name", fileName)
                    .put("file_size", fileSize)
                    .put("file_owner", fileOwner)
                    .put("last_modified_timestamp", lastModifiedTimestamp)
                    .put("current_timestamp", currentTimestamp)
                    .put("current_timemillis", currentTimeMillis)
                    .put("stream_offset", Long.toString(streamOffset))
                    .put("topic_sent", this.topic)
                    .put("schema_subject", schemaSubject)
                    .put("schema_version", schemaVersion)
                    .put("status", status);

            records.add(new SourceRecord(null, null, this.DF_METADATA_TOPIC, metaSchema, struct));

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

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

        SchemaBuilder builder = null;
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
}
