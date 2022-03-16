/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.kafka;

import java.nio.ByteBuffer;

import com.baidu.hugegraph.kafka.consumer.StandardConsumer;
import com.baidu.hugegraph.kafka.consumer.StandardConsumerBuilder;
import com.baidu.hugegraph.kafka.producer.ProducerClient;
import com.baidu.hugegraph.kafka.producer.StandardProducerBuilder;
import com.baidu.hugegraph.kafka.producer.SyncConfProducer;
import com.baidu.hugegraph.kafka.producer.SyncConfProducerBuilder;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.syncgateway.SyncMutationClient;

/**
 * @author Scorpiour
 * @since 2022-03-08
 */
public class ClientFactory {

    private static class ClientInstanceHolder {
        public static final ClientFactory factory = new ClientFactory();
        /**
         * Producers
         */
        public static final ProducerClient<String, ByteBuffer> standardProducer = 
            new StandardProducerBuilder().build();
        public static final SyncConfProducer syncConfProducer = new SyncConfProducerBuilder().build();
        /**
         * Consumers
         */
        public static final StandardConsumer standardConsumer = new StandardConsumerBuilder().build();
        public static final SyncMutationClient syncMutationClient = new SyncMutationClient(
                            MetaManager.instance().getKafkaSlaveServerHost(),
                            MetaManager.instance().getKafkaSlaveServerPort());

    }

    private ClientFactory() {

    }

    public static ClientFactory getInstance() {
        return ClientInstanceHolder.factory;
    }

    public ProducerClient<String, ByteBuffer> getStandardProducer() {
        return ClientInstanceHolder.standardProducer;
    }

    public StandardConsumer getStandardConsumer() {
        return ClientInstanceHolder.standardConsumer;
    }

    public SyncMutationClient getSyncMutationClient() {
        return ClientInstanceHolder.syncMutationClient;
    }

    public SyncConfProducer getSyncConfProducer() {
        return ClientInstanceHolder.syncConfProducer;
    }
}
