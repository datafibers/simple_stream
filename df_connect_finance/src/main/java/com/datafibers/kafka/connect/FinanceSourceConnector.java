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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinanceSourceConnector extends SourceConnector {
	private static final Logger log = LoggerFactory.getLogger(FinanceSourceConnector.class);

	public static final String TOPIC_CONFIG = "topic";
	public static final String TOPIC_CONFIG_DOC = "The topic to publish data to";
	public static final String TOPIC_CONFIG_DEFAULT = "topic_stock";
	public static final String STOCK_SYMBOLS_CONFIG = "symbols";
	public static final String STOCK_SYMBOLS_CONFIG_DOC = "List of stock symbols to process.";
	public static final String STOCK_SYMBOLS_CONFIG_DEFAULT = "INTC,TSLA";
	public static final String STOCK_PORTFOLIO_CONFIG = "portfolio";
	public static final String STOCK_PORTFOLIO_CONFIG_DOC = "list of predefined symbols";
	public static final String STOCK_PORTFOLIO_CONFIG_DEFAULT = "none";
	public static final String REFRESH_INTERVAL_CONFIG = "interval";
	public static final String REFRESH_INTERVAL_CONFIG_DOC = "How often to check stock update in seconds.";
	public static final String REFRESH_INTERVAL_CONFIG_DEFAULT = "20";
	public static final String SCHEMA_URI_CONFIG = "schema.registry.uri";
	public static final String SCHEMA_URI_CONFIG_DOC = "The URI to the Schema Registry.";
	public static final String SCHEMA_URI_CONFIG_DEFAULT = "http://localhost:8081";
	public static final String SCHEMA_SUBJECT_CONFIG = "schema.subject";
	public static final String SCHEMA_SUBJECT_CONFIG_DOC = "The subject used to validate avro schema.";
	public static final String SCHEMA_SUBJECT_CONFIG_DEFAULT = "n/a";
	public static final String SPOOF_FLAG_CONFIG = "spoof";
	public static final String SPOOF_FLAG_CONFIG_DOC = "If data comes from fake past data (PAST) or random (RAND) or NONE";
	public static final String SPOOF_FLAG_CONFIG_DEFAULT = "NONE";
	public static final String CUID = "cuid";
	public static final String CUID_DOC = "cuid";
	public static final String CUID_DEFAULT = "n/a";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(TOPIC_CONFIG, Type.STRING, TOPIC_CONFIG_DEFAULT, Importance.HIGH, TOPIC_CONFIG_DOC)
			.define(STOCK_SYMBOLS_CONFIG, Type.STRING, STOCK_SYMBOLS_CONFIG_DEFAULT, Importance.HIGH, STOCK_SYMBOLS_CONFIG_DOC)
			.define(SPOOF_FLAG_CONFIG, Type.STRING, SPOOF_FLAG_CONFIG_DEFAULT, Importance.HIGH, SPOOF_FLAG_CONFIG_DOC)
			.define(STOCK_PORTFOLIO_CONFIG, Type.STRING, STOCK_PORTFOLIO_CONFIG_DEFAULT, Importance.MEDIUM, STOCK_PORTFOLIO_CONFIG_DOC)
			.define(REFRESH_INTERVAL_CONFIG, Type.STRING, REFRESH_INTERVAL_CONFIG_DEFAULT, Importance.MEDIUM, REFRESH_INTERVAL_CONFIG_DOC)
			.define(SCHEMA_URI_CONFIG, Type.STRING, SCHEMA_URI_CONFIG_DEFAULT, Importance.HIGH, SCHEMA_URI_CONFIG_DOC)
			.define(SCHEMA_SUBJECT_CONFIG, Type.STRING, SCHEMA_SUBJECT_CONFIG_DEFAULT, Importance.HIGH, SCHEMA_SUBJECT_CONFIG_DOC)
			.define(CUID, Type.STRING, CUID_DEFAULT, Importance.MEDIUM, CUID_DOC);

	private String topic;
	private String stockSymbols;
	private String stockPortfolio;
	private String refreshInterval;
	private String schemaUri;
	private String schemaSubject;
	private String spoofFlag;
	private String cuid;

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		topic = props.get(TOPIC_CONFIG);
		stockPortfolio = props.get(STOCK_PORTFOLIO_CONFIG);
		refreshInterval = props.get(REFRESH_INTERVAL_CONFIG);
		schemaUri = props.get(SCHEMA_URI_CONFIG);
		schemaSubject = props.get(SCHEMA_SUBJECT_CONFIG);
		spoofFlag = props.get(SPOOF_FLAG_CONFIG);
		cuid = props.get(CUID);

		if (topic == null)
			topic = "topic_stock";
		if (topic.contains(","))
			throw new ConnectException("FinanceSourceConnector should only have a single topic when used as a source.");

		if(!stockPortfolio.equalsIgnoreCase("none")) {
			stockSymbols = YahooFinanceStockHelper.portfolio.get(stockPortfolio);
		} else {
			stockSymbols = props.get(STOCK_SYMBOLS_CONFIG);
			if(stockSymbols == null) stockSymbols = "INTC,TSLA";
		}

		if (refreshInterval != null && !refreshInterval.isEmpty()) {
			try {
				Integer.parseInt(refreshInterval);
			} catch (NumberFormatException nfe) {
				throw new ConnectException("'interval' must be a valid integer");
			}
		} else {
			refreshInterval = "20";
		}

		if (schemaUri.endsWith("/"))
                schemaUri = schemaUri.substring(0, schemaUri.length() - 1);

		if (schemaSubject == null || schemaSubject.equalsIgnoreCase("n/a"))
                schemaSubject = topic;

	}

	@Override
	public Class<? extends Task> taskClass() {
		return FinanceSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		Map<String, String> config = new HashMap<String, String>();
		config.put(TOPIC_CONFIG, topic);
		config.put(STOCK_SYMBOLS_CONFIG, stockSymbols);
		config.put(STOCK_PORTFOLIO_CONFIG, stockPortfolio);
		config.put(REFRESH_INTERVAL_CONFIG, refreshInterval);
		config.put(SPOOF_FLAG_CONFIG, spoofFlag);
		config.put(SCHEMA_URI_CONFIG, schemaUri);
		config.put(SCHEMA_SUBJECT_CONFIG, schemaSubject);
		config.put(CUID, cuid);
		log.info("FinanceSourceConnector value: {}", getValues(config));
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

}
