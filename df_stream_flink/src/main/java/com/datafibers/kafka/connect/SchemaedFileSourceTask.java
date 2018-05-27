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

package com.datafibers.kafka.connect;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SchemaedFileSourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SchemaedFileSourceTask.class);
  public static final String TASK_OFFSET_KEY = "filename";
  public static final String TASK_OFFSET_VALUE = "position";

  private SchemaedFileSourceConnectorConfig config;
  private String topic = null;
  private String filename = null;
  private String inputType = null;
  private InputStream stream = null;
  private BufferedReader reader = null;
  private char[] buffer = new char[1024];
  private int offset = 0;
  private Long streamOffset = null;
  private long lastPollTime = -1L;
  private Long recordCount = 0L;
  private Schema recordSchema = null;


  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    config = new SchemaedFileSourceConnectorConfig(props);
    filename = config.getFile();

    if (filename == null || filename.isEmpty()) {
        log.debug("No file specified; will read from stdin");
        stream = System.in;
            // Tracking offset for stdin doesn't make sense
        streamOffset = null;
        reader = new BufferedReader(new InputStreamReader(stream));
    }

    topic = config.getTopic();
    if (topic == null)
        throw new ConnectException("FileStreamSourceTask config missing topic setting");

    inputType = config.getInputType();
    if (! inputType.equalsIgnoreCase("json") && ! inputType.equalsIgnoreCase("csv"))
      throw new ConnectException("Invalid configuration for file InputType");
  }

  @Override
  public void stop() {
    log.debug("Stopping");
    synchronized (this) {
        try {
            if (stream != null && stream != System.in) {
                stream.close();
                log.trace("Closed input stream");
            }
        } 
        catch (IOException e) {
            log.error("Failed to close stream for {} : {} ", 
                logFilename(), e.toString());
        }
        finally {
            stream = null;
            reader = null;
        }
        this.notify();
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (stream != System.in) {
        return pollFromFile();
    } else {
        return pollFromStream();
    } 

  }

  private void openFileStream() throws ConnectException,InterruptedException,FileNotFoundException,IOException {
    try {
        FileInputStream fstream = new FileInputStream(filename);
        String fdes = fstream.getFD().valid() ? fstream.getFD().toString() : "unknown";

        stream = fstream;
        log.trace("FileInputStream created for {}; fd={}", filename, fdes);
        Map<String, Object> topicOffset =
            context.offsetStorageReader().offset(offsetKey(filename));
        if (topicOffset != null) {
            Object lastRecordedOffset = topicOffset.get(TASK_OFFSET_VALUE);
            if (lastRecordedOffset != null) {
                if (!(lastRecordedOffset instanceof Long)) {
                    throw new ConnectException("Offset position is the incorrect type");
                }

                if (streamOffset != null) {
                    if (streamOffset > (Long)lastRecordedOffset) {
                        log.trace("streamOffset ({}) is greater than lastRecordedOffset ({})",
                                streamOffset.toString(), lastRecordedOffset.toString());
                        lastRecordedOffset = streamOffset;
                    }
                } else {
                    if (config.getReplishAllData()) {
                        log.trace("Ignoring committed offset ({}) to allow republication of existing data", lastRecordedOffset);
                        lastRecordedOffset = 0L;
                    }
                }
                long skipLeft = (Long) lastRecordedOffset;
                while (skipLeft > 0) {
                    try {
                        long skipped = stream.skip(skipLeft);
                        skipLeft -= skipped;
                    } catch (IOException e) {
                        log.error("Error while trying to seek to previous offset in file: ", e);
                        throw new ConnectException(e);
                    }
                }
                log.debug("Skipped to offset {}", lastRecordedOffset);
            }
            if (streamOffset == null) {
                streamOffset = (lastRecordedOffset != null) ?
                        (Long) lastRecordedOffset : 0L;
            } else if (lastRecordedOffset != null) {
                streamOffset = java.lang.Math.max (streamOffset, (Long) lastRecordedOffset);
            }
        } else {
            if (streamOffset == null) {
                    // first time through
                streamOffset = 0L;
            } else {
                    // re-opening file ... make sure we skip over stuff we've read
                fstream.getChannel().position(streamOffset);
            }
        }
        log.debug("Opened {} for reading; current offset {}", logFilename(), streamOffset);
    }
    catch (FileNotFoundException e) {
        log.warn("Couldn't find file {} for SchemaedFileSourceTask, sleeping to wait for it to be created", logFilename());
        synchronized (this) {
            this.wait(1000);
        }
        throw e;
    }
    catch (IOException e) {
        log.warn("Unexpected IOException: {}", e.toString());
        synchronized (this) {
            this.wait(1000);
        }
        throw e;
    }
  }

  private List<SourceRecord> pollFromFile() throws InterruptedException {
      log.trace("pollFromFile");
      CsvSchema bootstrapCsv;
      CsvMapper csvMapper = new CsvMapper();
      ObjectMapper jsonMapper = new ObjectMapper();
      MappingIterator<Map<?, ?>> mappingIterator;
      ArrayList<SourceRecord> records = null;
      long currentTime = System.currentTimeMillis();
      long recordsPerPoll;


        // TODO: Improve ExceptionOnEof logic.
        // The code below only works when each pass through
        // poll() reads all available records (not a given).
      if (config.getExceptionOnEof() && streamOffset != null) {
          throw new ConnectException("No more deta available on FileInputStream");
      }

        // Initialize the bootstrapCsv schema if necessary
      if (recordSchema == null  ||  inputType.equalsIgnoreCase("json")) {
          log.trace("Constructing csvSchema from emptySchema");
          bootstrapCsv = config.getCsvHeaders() ?
                  CsvSchema.emptySchema().withHeader() : CsvSchema.emptySchema().withoutHeader();
      } else {
            // We've seen a schema, so we'll assume headers from the recordSchema
          log.trace("Constructing csvSchema from recordSchema");
          CsvSchema.Builder builder = new CsvSchema.Builder();
          builder.setUseHeader(false);
          builder.setColumnSeparator(',');
          for (Field f : recordSchema.fields()) {
              log.trace("adding column {}", f.name());
              builder.addColumn(f.name());
          }
          bootstrapCsv = builder.build();
      }
      try {
          if (stream == null)
              openFileStream();
          if (reader == null)
              reader = new BufferedReader(new InputStreamReader(stream));

          if (inputType.equalsIgnoreCase("json")) {
              mappingIterator = jsonMapper.readerFor(Map.class).readValues(reader);
          } else if (inputType.equalsIgnoreCase("csv")) {
              mappingIterator = csvMapper.readerWithSchemaFor(Map.class).with(bootstrapCsv).readValues(reader);
          } else {
              log.error("Unsupported file input type specified ({})", inputType);
              return null;
          }
      } catch (FileNotFoundException fnf) {
          log.warn("Couldn't find file {} for SchemaedFileSourceTask, sleeping to wait for it to be created", logFilename());
          synchronized (this) {
              this.wait(1000);
          }
          return null;
      } catch (IOException e) {
            // IOException thrown when no more records in stream
          log.warn("Processed all available data from {}; sleeping to wait additional records", logFilename());
            // Close reader and stream; swallowing exceptions ... we're about to throw a Retry
          try { reader.close(); }
          catch (Exception nested) { }
          finally { reader = null; }

          if (stream != System.in) {
              try { stream.close(); }
              catch (Exception nested) { }
              finally { stream = null; }
          }

          synchronized (this) {
              this.wait(1000);
          }
          return null;
      }
      log.debug("mappingIterator of type {} created; begin reading data file",
              mappingIterator.getClass().toString());


        // The csvMapper class is really screwy; can't figure out why it
        // won't return a rational Schema ... so we'll extract it from the
        // the first object later.
    if (recordSchema == null  &&  inputType.equalsIgnoreCase("csv")  && csvMapper.schema().size() > 0) {
        recordSchema = ConvertMappingSchema(csvMapper.schemaWithHeader());
        log.trace("recordSchema created from csvMapper; type {}", recordSchema.type().toString());
    }
    try {
        FileInputStream fstream = (FileInputStream)stream;
        Long lastElementOffset = streamOffset;
        recordsPerPoll = 3;

        while (mappingIterator.hasNext()) {
            Map<?, ?> element = mappingIterator.next();
            Long elementOffset, iteratorOffset;
            recordCount++;
            recordsPerPoll--;

            iteratorOffset = mappingIterator.getCurrentLocation().getByteOffset();  // never works !!!
            if (iteratorOffset < 0) {
                    // The stream channel will CLOSE on the last clean record
                    // seen by mapping Iterator, so we have be careful here
                    // Additionally, when parsing CSV files, there seems to be a
                    // lot of Bad File Descriptor errors; ignore them.
                try {
                    elementOffset = fstream.getChannel().position();
                }
                catch (java.nio.channels.ClosedChannelException e) {
                    log.trace("getChannel.position threw {}", e.toString());
                    elementOffset = lastElementOffset;
                }
                catch (IOException e) {
                    log.trace("getChannel.position threw {}", e.toString());
                    elementOffset = lastElementOffset;
                }
            } else {
                log.trace("mappingIterator.getCurrentLocation() returns {}", iteratorOffset.toString());
                elementOffset=iteratorOffset;
            }
            log.trace("Next input record: {} (class {}) from file position {}",
                element.toString(), element.getClass().toString(), elementOffset.toString());

            if (recordSchema == null) {
                recordSchema = ConvertMappingSchema(element.keySet());
                log.trace("recordSchema created from element; type {}", recordSchema.type().toString());
            }

            if (records == null)
                records = new ArrayList<>();
            records.add (new SourceRecord (
                    offsetKey(filename),
                    offsetValue(elementOffset),
                    topic,
                    recordSchema,
                    ConvertMappingElement(recordSchema, (HashMap<?,?>) element)
            ));
            streamOffset = lastElementOffset = elementOffset;
        }
    }
    catch (Exception e) {
        throw new ConnectException(e);
    }

    lastPollTime = currentTime;
    return records;
  }

  private List<SourceRecord> pollFromStream() throws InterruptedException {
        // Unfortunately we can't just use readLine() because it blocks 
        // in an uninterruptible way.  Instead we have to manage
        // splitting lines ourselves, using simple backoff when 
        // no new data is available.
    try {
        final BufferedReader readerCopy;
        synchronized (this) {
            readerCopy = reader;
        }
        if (readerCopy == null)
            return null;

        ArrayList<SourceRecord> records = null;

        int nread = 0;
        while (readerCopy.ready()) {
            nread = readerCopy.read(buffer, offset, buffer.length - offset);
            log.trace("Read {} bytes from {}", nread, logFilename());

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
                    if (line != null) {
                        log.trace("Read a line from {}", logFilename());
                        if (records == null)
                            records = new ArrayList<>();

                        records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, Schema.STRING_SCHEMA, line));
                    }
                } while (line != null);
            }
        }
        if (nread <= 0)
            synchronized (this) {
                this.wait(1000);
            }

        return records;
    } catch (IOException e) {
        // Underlying stream was killed, probably as a result 
        // of calling stop. Allow to return null, and driving 
        // thread will handle any shutdown if necessary.
    }
    return null;
  }

        // Return next line from buffer[] and return as a String
    private String extractLine() {
        int until = -1, newStart = -1;
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                    // Ignore lines terminated with '\r' instead of '\r\n'
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

        // TODO: Add logic to Config class to allow specification 
        // of default number format (could be integer or double)
    private Schema ConvertMappingSchema (CsvSchema srcSchema) {
        Iterator<CsvSchema.Column> colIter = srcSchema.iterator();
        SchemaBuilder builder = SchemaBuilder.struct().name(topic);
        Schema coreSchema ;

        log.trace("Converting CsvSchema {} ({} columns) to Connect Schema",
                srcSchema.toString(), srcSchema.size());
        while (colIter.hasNext()) {
            CsvSchema.Column c = colIter.next();
            log.trace("Adding column {} of type {} to MappingSchema",
                    c.getName(), c.getType().toString());
            if (c.getType().equals(CsvSchema.ColumnType.BOOLEAN)) {
                builder.field(c.getName(), Schema.BOOLEAN_SCHEMA);
            } else if (c.getType().equals(CsvSchema.ColumnType.NUMBER)) {
                builder.field(c.getName(), Schema.FLOAT64_SCHEMA);
            } else {
                    // NUMBER_OR_STRING, STRING_OR_LITERAL, or STRING
                builder.field(c.getName(), Schema.STRING_SCHEMA);
            }
        }
        coreSchema = builder.build();

        return coreSchema;

/*
        return (SchemaBuilder.map(Schema.STRING_SCHEMA, coreSchema)
            .name("SchemaedFile.Data")
            .version(1)
            .build());
*/
    }

    private Schema ConvertMappingSchema (Set<?> columns) {
        SchemaBuilder builder = SchemaBuilder.struct().name(topic);
        Schema coreSchema ;
        Iterator colIter = columns.iterator();

        log.trace("Converting columns {} to Connect Schema", columns.toString());
        while (colIter.hasNext()) {
            String column = colIter.next().toString();
            builder.field(column, Schema.STRING_SCHEMA);
        }

        coreSchema = builder.build();
        return coreSchema ;
    }

    private Struct ConvertMappingElement (Schema schema, HashMap element) {
        Struct struct = new Struct(schema);
        for (Object o : element.keySet()) {
            if (o instanceof String) {
                log.trace("Converting key {} from element", o.toString());
            } else {
                log.warn ("Element contains non-string key {}", o.toString());
                continue;
            }
            String key = (String)o;
            Object val = element.get(key);
            struct.put(key, (String) val);
            /*
            switch (schema.) {
                case Schema.BOOLEAN_SCHEMA: {
                    if (val instanceof Boolean) {
                        struct.put(key, (Boolean)val);
                    } else {
                        log.trace("Expected boolean for key {}; saw {}; storing null",
                            key, val.toString());
                        struct.put(key, null);
                    }
                }
                case Schema.FLOAT64_SCHEMA: {
                    if (val instanceof Double) {
                        struct.put(key, (Double)val);
                    } else {
                        log.trace("Expected number for key {}; saw {}; storing null",
                            key, val.toString());
                        struct.put(key, null);
                    }
                }
                case Schema.STRING_SCHEMA:
                default: {
                    if (val instanceof String) {
                        struct.put(key, (String)val);
                    } else {
                        log.trace("Expected string for key {}; saw {}; storing null",
                                key, val.toString());
                        struct.put(key, null);
                    }
                }
            }
            */
        }
        return struct;
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(TASK_OFFSET_KEY, topic + "/" + filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(TASK_OFFSET_VALUE, pos);
    }

    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }
}
