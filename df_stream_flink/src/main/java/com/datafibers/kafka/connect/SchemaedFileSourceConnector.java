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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.*;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SchemaedFileSourceConnector extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(SchemaedFileSourceConnector.class);
  private SchemaedFileSourceConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new SchemaedFileSourceConnectorConfig(map);

    String topic = config.getTopic();
    if ( topic == null || topic.isEmpty() ) {
        throw new ConnectException("SchemaedFileSourceConnector configuration must include 'topic' setting");
    }
    if ( topic.contains(",") ) { 
        throw new ConnectException("SchemaedFileSourceConnector supports only a single topic, not a topic list");
    }

    String filename = config.getFile();
    String fileExt = filename.substring(filename.lastIndexOf('.') + 1);
    String cfgExt = config.getInputType();

    if (fileExt.equalsIgnoreCase("json")) {
      if (! fileExt.equalsIgnoreCase(cfgExt)) {
        log.warn ("File extension {} does not match configuration setting ({}); defaulting to {}",
                fileExt, cfgExt, fileExt);
        config.setInputType("json");
      }
    } else if (fileExt.equalsIgnoreCase("csv")) {
      if (!fileExt.equalsIgnoreCase(cfgExt)) {
        log.warn("File extension {} does not match configuration setting ({}); defaulting to {}",
                fileExt, cfgExt, fileExt);
        config.setInputType("csv");
      }
    }

    if (config.getInputType() == null) {
      config.setInputType("json");
      log.warn("No input type specified; defaulting to {}", "json");
    }

  }

  @Override
  public Class<? extends Task> taskClass() {
    return SchemaedFileSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.debug("taskConfigs called for {} tasks; overriding to 1 task", maxTasks);
    return Collections.nCopies(1, config.originalsStrings());
  }

  @Override
  public void stop() {
    // Nothing to do 
  }

  @Override
  public ConfigDef config() {
    return config.conf() ;
  }
}
