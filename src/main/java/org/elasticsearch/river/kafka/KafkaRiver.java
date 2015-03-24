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
package org.elasticsearch.river.kafka;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.kafka.config.RiverConfig;
import org.elasticsearch.river.kafka.produce.DeleteDocumentProducer;
import org.elasticsearch.river.kafka.produce.ElasticSearchProducer;
import org.elasticsearch.river.kafka.produce.IndexDocumentProducer;
import org.elasticsearch.river.kafka.consume.KafkaConsumer;
import org.elasticsearch.river.kafka.produce.RawMessageProducer;

/**
 * This is the actual river implementation, which starts a thread to read messages from kafka and put them into elasticsearch.
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

    private KafkaConsumer kafkaConsumer;
    private ElasticSearchProducer elasticsearchProducer;
    private RiverConfig riverConfig;

    private Thread thread;

    @Inject
    protected KafkaRiver(final RiverName riverName, final RiverSettings riverSettings, final Client client) {
        super(riverName, riverSettings);

        riverConfig = new RiverConfig(riverName, riverSettings);
        kafkaConsumer = new KafkaConsumer(riverConfig);

        switch (riverConfig.getActionType()) {
            case INDEX:
                elasticsearchProducer = new IndexDocumentProducer(client, riverConfig.esConfig, kafkaConsumer);
                break;
            case DELETE:
                elasticsearchProducer = new DeleteDocumentProducer(client, riverConfig.esConfig, kafkaConsumer);
                break;
            case RAW_EXECUTE:
                elasticsearchProducer = new RawMessageProducer(client, riverConfig.esConfig, kafkaConsumer);
                break;
        }
    }

    @Override
    public void start() {

        try {
            logger.debug("Index: {}: Starting Kafka River...", riverConfig.esConfig.getIndexName());
            final KafkaRiverWorker kafkaRiverWorker = new KafkaRiverWorker(kafkaConsumer, elasticsearchProducer, riverConfig);

            thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "Kafka River Worker").newThread(kafkaRiverWorker);
            thread.start();
        } catch (Exception ex) {
            logger.error("Index: {}: Unexpected Error occurred", ex, riverConfig.esConfig.getIndexName());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        logger.debug("Index: {}: Closing kafka river...", riverConfig.esConfig.getIndexName());

        elasticsearchProducer.closeBulkProcessor();
        kafkaConsumer.shutdown();

        thread.interrupt();
    }
}
