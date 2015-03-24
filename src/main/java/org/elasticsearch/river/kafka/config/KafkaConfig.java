package org.elasticsearch.river.kafka.config;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.Map;

/**
* Created by rsl on 24-03-15.
*/
public class KafkaConfig {
    private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
    private static final String TOPIC = "topic";
    private static final String MESSAGE_TYPE = "message.type";

    private String zookeeperConnect;
    private int zookeeperConnectionTimeout;
    private String topic;
    private RiverConfig.MessageType messageType;

    public KafkaConfig(Map<String, Object> kafkaSettings) {
        topic = (String) kafkaSettings.get(TOPIC);
        zookeeperConnect = XContentMapValues.nodeStringValue(kafkaSettings.get(ZOOKEEPER_CONNECT), "localhost");
        zookeeperConnectionTimeout = XContentMapValues.nodeIntegerValue(kafkaSettings.get(ZOOKEEPER_CONNECTION_TIMEOUT), 10000);
        messageType = RiverConfig.MessageType.fromValue(XContentMapValues.nodeStringValue(kafkaSettings.get(MESSAGE_TYPE),
                RiverConfig.MessageType.JSON.toValue()));
    }

    public KafkaConfig() {
        this.zookeeperConnect = "localhost";
        this.zookeeperConnectionTimeout = 10000;
        this.topic = "elasticsearch-river-kafka";
        this.messageType = RiverConfig.MessageType.JSON;
    }

    public String getZookeeperConnect() {
        return zookeeperConnect;
    }

    public int getZookeeperConnectionTimeout() {
        return zookeeperConnectionTimeout;
    }

    public RiverConfig.MessageType getMessageType() {
        return messageType;
    }

    public String getTopic() {
        return topic;
    }
}
