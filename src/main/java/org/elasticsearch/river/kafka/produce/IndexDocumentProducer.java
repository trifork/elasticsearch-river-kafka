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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.river.kafka.config.ESConfig;
import org.elasticsearch.river.kafka.consume.KafkaConsumer;

import java.util.Map;
import java.util.Set;

/**
 * Producer to index documents. Creates index document requests, which are executed with Bulk API.
 *
 * @author Mariam Hakobyan
 */
public class IndexDocumentProducer extends ElasticSearchProducer {

    public IndexDocumentProducer(Client client, ESConfig config, KafkaConsumer kafkaConsumer) {
        super(client, config, kafkaConsumer);
    }

    /**
     * For the given messages creates index document requests and adds them to the bulk processor queue, for
     * processing later when the size of bulk actions is reached.
     *
     * @param messageSet given set of messages
     */
    public void addMessagesToBulkProcessor(final Set<MessageAndMetadata> messageSet) {

        for (MessageAndMetadata messageAndMetadata : messageSet) {
            final byte[] messageBytes = (byte[]) messageAndMetadata.message();

            if (messageBytes == null || messageBytes.length == 0) return;

            try {
                // TODO - future improvement - support for protobuf messages

                String message = null;
                IndexRequest request = null;

                switch (config.getMessageType()) {
                    case STRING:
                        message = XContentFactory.jsonBuilder()
                                .startObject()
                                .field("value", new String(messageBytes, "UTF-8"))
                                .endObject()
                                .string();
                        request = Requests.indexRequest(config.generateIndexName()).
                                type(config.getTypeName()).
                                source(message);
                        break;
                    case JSON:
                        final Map<String, Object> messageMap = reader.readValue(messageBytes);
                        request = Requests.
                                        indexRequest(config.generateIndexName())
                                        .type(config.getTypeName())
                                        .source(messageMap);
                }

                bulkProcessor.add(request);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
