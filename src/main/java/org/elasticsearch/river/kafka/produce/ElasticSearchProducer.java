 /*
 * Copyright 2014 Mariam Hakobyan
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
package org.elasticsearch.river.kafka.produce;

import kafka.message.MessageAndMetadata;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.kafka.consume.Consumer;
import org.elasticsearch.river.kafka.consume.KafkaConsumer;
import org.elasticsearch.river.kafka.config.ESConfig;

import java.util.Map;
import java.util.Set;

/**
 * An ElasticSearch base producer, which creates an index, mapping in the EL.
 * Also, creates index/delete document requests against ElasticSearch, and executes them with Bulk API.
 *
 * @author Mariam Hakobyan
 */
public abstract class ElasticSearchProducer {

    private static final ESLogger logger = ESLoggerFactory.getLogger(ElasticSearchProducer.class.getName());

    protected final ObjectReader reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});

    private Client client;
    protected BulkProcessor bulkProcessor;

    protected ESConfig config;

    public ElasticSearchProducer(final Client client, final ESConfig config, final Consumer consumer) {
        this.client = client;
        this.config = config;
        createBulkProcessor(consumer);
    }

    private void createBulkProcessor(final Consumer consumer) {
        bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.info("Index: {}: Going to execute bulk request composed of {} actions.", config.getIndexName(), request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("Index: {}: Executed bulk composed of {} actions.", config.getIndexName(), request.numberOfActions());

                        // Commit the kafka messages offset, only when messages have been successfully
                        // inserted into ElasticSearch
                        consumer.commitReads();
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.warn("Index: {}: Error executing bulk.", failure, config.getIndexName());
                    }
                })
                .setBulkActions(config.getBulkSize())
                .setFlushInterval(TimeValue.timeValueSeconds(config.getFlushIntervalSecs()))
                .setConcurrentRequests(config.getConcurrentRequests())
                .build();
    }

    /**
     * For the given messages executes the specified operation type and adds the results bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public abstract void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet);

    public void closeBulkProcessor() {
        bulkProcessor.close();
    }
}
