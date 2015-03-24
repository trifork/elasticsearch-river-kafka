package org.elasticsearch.river.kafka.config;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by rsl on 24-03-15.
 */
public class ESConfig {

    /* Elasticsearch config */
    private static final String INDEX_NAME = "index";
    private static final String MAPPING_TYPE = "type";
    private static final String BULK_SIZE = "bulk.size";
    private static final String CONCURRENT_REQUESTS = "concurrent.requests";
    private static final String FLUSH_INTERVAL_SECS = "flush.interval.secs";

    private String indexName;
    private String typeName;
    private int concurrentRequests;
    private int bulkSize;
    private long flushIntervalSecs;
    private RiverConfig.MessageType messageType;

    public ESConfig(String indexName, String typeName, int concurrentRequests, int bulkSize, int flushIntervalSecs, RiverConfig.MessageType messageType) {
        this.indexName = indexName;
        this.typeName = typeName;
        this.concurrentRequests = concurrentRequests;
        this.bulkSize = bulkSize;
        this.flushIntervalSecs = flushIntervalSecs;
        this.messageType = messageType;
    }

    public ESConfig(Map<String, Object> indexSettings, String riverName, RiverConfig.MessageType messageType) {
        indexName = XContentMapValues.nodeStringValue(indexSettings.get(INDEX_NAME), riverName);
        typeName = XContentMapValues.nodeStringValue(indexSettings.get(MAPPING_TYPE), "status");
        concurrentRequests = XContentMapValues.nodeIntegerValue(indexSettings.get(CONCURRENT_REQUESTS), 1);
        bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE), 100);
        flushIntervalSecs = XContentMapValues.nodeIntegerValue(indexSettings.get(FLUSH_INTERVAL_SECS), 5);
        this.messageType = messageType;
    }

    public String generateIndexName() {
        // support logstash style index names -%{+YYYY.MM.dd}
        if (indexName.contains("{")) {
            return generateIndexNameFromTimestamp(indexName, new Date());
        }
        return indexName;
    }

    private static String generateIndexNameFromTimestamp(String indexName, Date date) {
        String datePattern = indexName.substring(2 + indexName.indexOf("%{"), indexName.indexOf("}"));
        SimpleDateFormat sdf = new SimpleDateFormat(datePattern);
        return indexName.substring(0, indexName.indexOf("%{")) + sdf.format(date) + indexName.substring(1 + indexName.indexOf("}"));
    }

    public String getTypeName() {
        return typeName;
    }

    public int getConcurrentRequests() {
        return concurrentRequests;
    }

    public String getIndexName() {
        return indexName;
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public long getFlushIntervalSecs() {
        return flushIntervalSecs;
    }

    public RiverConfig.MessageType getMessageType() {
        return messageType;
    }
}
