/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package com.datafibers.kafka.connect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileGenericSourceConnector extends SourceConnector {
	private static final Logger log = LoggerFactory.getLogger(FileGenericSourceConnector.class);

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_CONFIG_DOC = "The topic to publish data to";
	public static final String TOPIC_CONFIG_DEFAULT = "n/a";
	public static final String FILE_LOCATION_CONFIG = "file.location";
	public static final String FILE_LOCATION_CONFIG_DOC = "The location of the file(s) to process.";
	public static final String FILE_LOCATION_CONFIG_DEFAULT = "n/a";
	public static final String FILE_GLOB_CONFIG = "file.glob";
	public static final String FILE_GLOB_CONFIG_DOC = "The glob criteria.";
	public static final String FILE_GLOB_CONFIG_DEFAULT = "*";
	public static final String FILE_INTERVAL_CONFIG = "file.glob.interval";
	public static final String FILE_INTERVAL_CONFIG_DOC = "How often to check for new file(s) to be processed.";
	public static final String FILE_INTERVAL_CONFIG_DEFAULT = "10";
	public static final String FILE_OVERWRITE_CONFIG = "file.overwrite";
	public static final String FILE_OVERWRITE_CONFIG_DOC = "If a file is modified should it be republished to kafka?";
	public static final String FILE_OVERWRITE_CONFIG_DEFAULT = "FALSE";
    public static final String SCHEMA_IGNORED = "schema.ignored";
	public static final String SCHEMA_IGNORED_DOC = "If the file schema is ignored?";
	public static final String SCHEMA_IGNORED_DEFAULT = "FALSE";
	public static final String SCHEMA_URI_CONFIG = "schema.registry.uri";
	public static final String SCHEMA_URI_CONFIG_DOC = "The URI to the Schema Registry.";
	public static final String SCHEMA_URI_CONFIG_DEFAULT = "http://localhost:8081";
	public static final String SCHEMA_SUBJECT_CONFIG = "schema.subject";
	public static final String SCHEMA_SUBJECT_CONFIG_DOC = "The subject used to validate avro schema.";
	public static final String SCHEMA_SUBJECT_CONFIG_DEFAULT = "n/a";
	public static final String SCHEMA_VERSION_CONFIG = "schema.version";
	public static final String SCHEMA_VERSION_CONFIG_DOC = "The version of the subject to be used for schema validation.";
	public static final String SCHEMA_VERSION_CONFIG_DEFAULT = "n/a";
	public static final String CUID = "cuid";
	public static final String CUID_DOC = "cuid";
	public static final String CUID_DEFAULT = "n/a";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(TOPIC_CONFIG, Type.STRING, TOPIC_CONFIG_DEFAULT, Importance.HIGH, TOPIC_CONFIG_DOC)
			.define(FILE_LOCATION_CONFIG, Type.STRING, FILE_LOCATION_CONFIG_DEFAULT, Importance.HIGH, FILE_LOCATION_CONFIG_DOC)
			.define(FILE_GLOB_CONFIG, Type.STRING, FILE_GLOB_CONFIG_DEFAULT, Importance.HIGH, FILE_GLOB_CONFIG_DOC)
			.define(FILE_INTERVAL_CONFIG, Type.STRING, FILE_INTERVAL_CONFIG_DEFAULT, Importance.MEDIUM, FILE_INTERVAL_CONFIG_DOC)
			.define(FILE_OVERWRITE_CONFIG, Type.STRING, FILE_OVERWRITE_CONFIG_DEFAULT, Importance.MEDIUM, FILE_OVERWRITE_CONFIG_DOC)
            .define(SCHEMA_IGNORED, Type.STRING, SCHEMA_IGNORED_DEFAULT, Importance.HIGH, SCHEMA_IGNORED_DOC)
			.define(SCHEMA_URI_CONFIG, Type.STRING, SCHEMA_URI_CONFIG_DEFAULT, Importance.HIGH, SCHEMA_URI_CONFIG_DOC)
			.define(SCHEMA_SUBJECT_CONFIG, Type.STRING, SCHEMA_SUBJECT_CONFIG_DEFAULT, Importance.MEDIUM, SCHEMA_SUBJECT_CONFIG_DOC)
			.define(SCHEMA_VERSION_CONFIG, Type.STRING, SCHEMA_VERSION_CONFIG_DEFAULT,Importance.MEDIUM, SCHEMA_VERSION_CONFIG_DOC)
			.define(CUID, Type.STRING, CUID_DEFAULT, Importance.MEDIUM, CUID_DOC);

	private String topic;
	private String fileLocation;
	private String fileGlob;
	private String fileInterval;
	private String fileOverwrite;
    private String schemaIgnored;
	private String schemaUri;
	private String schemaSubject;
	private String schemaVersion;
	private String cuid;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		topic = props.get(TOPIC_CONFIG);
		fileLocation = props.get(FILE_LOCATION_CONFIG);
		fileGlob = props.get(FILE_GLOB_CONFIG);
		fileInterval = props.get(FILE_INTERVAL_CONFIG);
		fileOverwrite = props.get(FILE_OVERWRITE_CONFIG);
        schemaIgnored = props.get(SCHEMA_IGNORED);
		schemaUri = props.get(SCHEMA_URI_CONFIG);
		schemaSubject = props.get(SCHEMA_SUBJECT_CONFIG);
		schemaVersion = props.get(SCHEMA_VERSION_CONFIG);
		cuid = props.get(CUID);

		if (topic == null || topic.equalsIgnoreCase("n/a"))
			throw new ConnectException("FileGenericSourceConnector configuration must include 'topic' setting");
		if (topic.contains(","))
			throw new ConnectException("FileGenericSourceConnector should only have a single topic when used as a source.");
		if (fileLocation == null || fileLocation.equalsIgnoreCase("n/a"))
			throw new ConnectException("FileGenericSourceConnector configuration must include 'file.location' setting");
		if (fileInterval != null && !fileInterval.isEmpty()) {
			try {
				Integer.parseInt(fileInterval);
			} catch (NumberFormatException nfe) {
				throw new ConnectException("'file.glob.interval' must be a valid integer");
			}
		} else {
			fileInterval = "10";
		}
		if (schemaIgnored == null || schemaIgnored.equalsIgnoreCase("FALSE")) { // Do not ignore all schema info
            if (schemaUri.endsWith("/"))
                schemaUri = schemaUri.substring(0, schemaUri.length() - 1);
            if (schemaSubject == null || schemaSubject.equalsIgnoreCase("n/a"))
                schemaSubject = topic;
            if (schemaVersion == null || schemaVersion.equalsIgnoreCase("n/a"))
                schemaVersion = getLatestVersion(schemaUri, schemaSubject);
			schemaIgnored = "FALSE";
        } else { // Do ignore all schema info
            log.warn("The Schema is undefined. Avro Convert will create STRING schema at subject: " + topic + "_value");
            schemaUri = "NULL";
            schemaVersion = "NULL";
            schemaSubject = "NULL";
            schemaIgnored = "TRUE";
        }
	}

	@Override
	public Class<? extends Task> taskClass() {
		return FileGenericSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		Map<String, String> config = new HashMap<String, String>();
		config.put(TOPIC_CONFIG, topic);
		config.put(FILE_LOCATION_CONFIG, fileLocation);
		config.put(FILE_GLOB_CONFIG, fileGlob);
		config.put(FILE_INTERVAL_CONFIG, fileInterval);
		config.put(FILE_OVERWRITE_CONFIG, fileOverwrite);
        config.put(SCHEMA_IGNORED, schemaIgnored);
		config.put(SCHEMA_URI_CONFIG, schemaUri);
		config.put(SCHEMA_SUBJECT_CONFIG, schemaSubject);
		config.put(SCHEMA_VERSION_CONFIG, schemaVersion);
		config.put(CUID, cuid);
		log.info("FileGenericSourceConnector value: {}", getValues(config));
		return Arrays.asList(config);
	}

	private String getValues(Map<String, String> config) {
		StringBuilder builder = new StringBuilder();
		for (String key : config.keySet()) {
			builder.append("\n\t").append(key).append(" = ").append(config.get(key));
		}
		return builder.append("\n").toString();
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	/**
	 * 
	 * @param schemaUri
	 *            The base Schema Registry URI
	 * @param schemaSubject
	 *            The Schema Registry subject
	 * @return The latest version of the Schema Registry subject
	 */
	private String getLatestVersion(String schemaUri, String schemaSubject) {
		BufferedReader br = null;
		String version = null;
		try {
			String fullUrl = String.format("%s/subjects/%s/versions", schemaUri, schemaSubject);

			StringBuilder response = new StringBuilder();
			String line;
			br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
			while ((line = br.readLine()) != null) {
				response.append(line);
			}

			ArrayNode responseJson = new ObjectMapper().readValue(response.toString(), ArrayNode.class);
			version = responseJson.get(responseJson.size() - 1).toString();
		} catch (IOException e) {
			throw new ConnectException("Unable to retrieve schema from Schema Registry", e);
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return version;
	}

}
