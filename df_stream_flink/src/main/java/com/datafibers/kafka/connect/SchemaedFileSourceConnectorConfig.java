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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;



public class SchemaedFileSourceConnectorConfig extends AbstractConfig {

  public enum FileTypes {
    ;
    static final Integer JSON = 0;
    static final Integer CSV = 1;
  }

  private enum CfgKeys {
    ;
    static final String TOPIC = "topic" ;
    static final String FILE = "file" ;
    static final String INPUT_TYPE= "input.type" ;
    static final String CSV_HEADERS = "csv.headers" ;
    static final String EXCEPTION_ON_EOF= "end.on.eof" ;
    static final String REPUBLISH_ALL_DATA = "republish.all.data" ;
    static final String PUBLISH_RATE = "publish.rate" ;
  }

  private enum CfgTips {
    ;
    static final String TOPIC = "Topic onto which data will be published" ;
    static final String FILE = "Source filename" ;
    static final String INPUT_TYPE = "CSV or JSON (default)" ;
    static final String CSV_HEADERS = "Include header row in CSV output";
    static final String EXCEPTION_ON_EOF= "Throw ConnectException from SourceTask when end-of-file is reached (ending the task)" ;
    static final String REPUBLISH_ALL_DATA = "Publish records to topic even if Connector has published the records in a previous invocation (useful for testing)." ;
    static final String PUBLISH_RATE = "approximate # of file lines per second to publish to the topic" ;
  }

  private static final ConfigDef myConfigDef = new ConfigDef()
      .define(CfgKeys.TOPIC, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CfgTips.TOPIC)
      .define(CfgKeys.FILE, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, CfgTips.FILE)
      .define(CfgKeys.INPUT_TYPE, ConfigDef.Type.STRING, "json", ConfigDef.Importance.HIGH, CfgTips.INPUT_TYPE)
      .define(CfgKeys.CSV_HEADERS, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, CfgTips.CSV_HEADERS)
      .define(CfgKeys.EXCEPTION_ON_EOF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CfgTips.EXCEPTION_ON_EOF)
      .define(CfgKeys.REPUBLISH_ALL_DATA, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CfgTips.REPUBLISH_ALL_DATA)
      .define(CfgKeys.PUBLISH_RATE, ConfigDef.Type.INT, 1000, ConfigDef.Importance.HIGH, CfgTips.PUBLISH_RATE) ;

    private String localInputType = null;

    public SchemaedFileSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
        localInputType = this.getString(CfgKeys.INPUT_TYPE);
    }

    public SchemaedFileSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return myConfigDef;
    }

    public String getTopic(){
        return this.getString(CfgKeys.TOPIC);
    }

    public String getFile(){
        return this.getString(CfgKeys.FILE);
    }

    public String getInputType() {
        return this.localInputType;
    }

    public void setInputType(String newInputType) {
        if (newInputType != null) {
            if (newInputType.equalsIgnoreCase("json")) {
                this.localInputType = "json";
            } else if (newInputType.equalsIgnoreCase("csv")) {
                this.localInputType = "csv";
            }
        }
    }

  public Boolean getCsvHeaders() {
      if (localInputType.equalsIgnoreCase("json")) {
          return false;
      } else {
          return this.getBoolean(CfgKeys.CSV_HEADERS);
      }
  }

  public Boolean getExceptionOnEof() {
      return this.getBoolean(CfgKeys.EXCEPTION_ON_EOF);
  }

  public Boolean getReplishAllData() {
      return this.getBoolean(CfgKeys.REPUBLISH_ALL_DATA);
  }

  public Integer getPublishRate(){
    return this.getInt(CfgKeys.PUBLISH_RATE);
  }

    // We need to override this method to ensure that the proper
    // configurations are propogated down to the SourceTasks (since
    // that propogation is done strictly through strings).
  @Override
    public Map<String, String> originalsStrings() {
      Map<String,String> copy = super.originalsStrings();
      copy.put(CfgKeys.INPUT_TYPE, localInputType);
      copy.put(CfgKeys.CSV_HEADERS, getCsvHeaders().toString());

      return copy;
    }
}
