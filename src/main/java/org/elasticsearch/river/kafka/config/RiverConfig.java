/*
 * Copyright 2013 Mariam Hakobyan
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
package org.elasticsearch.river.kafka.config;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;

/**
 * The configuration properties that the client will provide while creating a river in elastic search.
 *
 * @author Mariam Hakobyan
 */
public class RiverConfig {

    private static final String ACTION_TYPE = "action.type";


    /* Kakfa config */
    public KafkaConfig kafkaConfig;

    /* ES config */
    public ESConfig esConfig;

    private ActionType actionType;

    public RiverConfig(RiverName riverName, RiverSettings riverSettings) {

        // Extract kafka related configuration
        if (riverSettings.settings().containsKey("kafka")) {
            kafkaConfig = new KafkaConfig((Map<String, Object>) riverSettings.settings().get("kafka"));
        } else {
            kafkaConfig = new KafkaConfig();
        }

        // Extract ElasticSearch related configuration
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            esConfig = new ESConfig(indexSettings, riverName.getName(), kafkaConfig.getMessageType());
            actionType = ActionType.fromValue(XContentMapValues.nodeStringValue(indexSettings.get(ACTION_TYPE),
                    ActionType.INDEX.toValue()));
        } else {
            esConfig = new ESConfig(riverName.name(), "status", 1, 100, 5, kafkaConfig.getMessageType());
            actionType = ActionType.INDEX;
        }
    }

    public enum ActionType {

        INDEX("index"),
        DELETE("delete"),
        RAW_EXECUTE("raw.execute");

        private String actionType;

        private ActionType(String actionType) {
            this.actionType = actionType;
        }

        public String toValue() {
            return actionType;
        }

        public static ActionType fromValue(String value) {
            if(value == null) throw new IllegalArgumentException();

            for(ActionType values : values()) {
                if(value.equalsIgnoreCase(values.toValue()))
                    return values;
            }

            throw new IllegalArgumentException("ActionType with value " + value + " does not exist.");
        }
    }

    public enum MessageType {
        STRING("string"),
        JSON("json");

        private String messageType;

        private MessageType(String messageType) {
            this.messageType = messageType;
        }

        public String toValue() {
            return messageType;
        }

        public static MessageType fromValue(String value) {
            if(value == null) throw new IllegalArgumentException();

            for(MessageType values : values()) {
                if(value.equalsIgnoreCase(values.toValue()))
                    return values;
            }

            throw new IllegalArgumentException("MessageType with value " + value + " does not exist.");
        }
    }

    public ActionType getActionType() {
        return actionType;
    }

}
