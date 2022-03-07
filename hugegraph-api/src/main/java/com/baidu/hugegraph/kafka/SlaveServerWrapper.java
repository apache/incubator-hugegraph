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
import java.util.function.Consumer;

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.kafka.producer.ProducerClient;
import com.baidu.hugegraph.kafka.producer.StandardProducerBuilder;
import com.baidu.hugegraph.kafka.topic.HugeGraphMutateTopic;
import com.baidu.hugegraph.kafka.topic.HugeGraphMutateTopicBuilder;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.syncgateway.MutationDTO;
import com.baidu.hugegraph.syncgateway.SyncMutationServer;
import com.baidu.hugegraph.util.Log;

/**
 * Used to wrap the mutation sync related modules of slave
 * @author Scorpiour
 * @since 2022-03-03
 */
public class SlaveServerWrapper {

    private static final HugeGraphLogger LOGGER = 
                    Log.getLogger(SlaveServerWrapper.class);

    private static class InstanceHolder {
        private static SlaveServerWrapper instance = new SlaveServerWrapper();
    }

    public static SlaveServerWrapper getInstance() {
        return InstanceHolder.instance;
    }

    private volatile boolean closing = false;

    private SyncMutationServer server = null;
    private ProducerClient<String, ByteBuffer> producer = null;
    private SyncMutateConsumer consumer = null;

    private void initConsumer(GraphManager manager) {
        SyncMutateConsumerBuilder.setGraphManager(manager);
        consumer = new SyncMutateConsumerBuilder().build();
    }

    private void initProducer() {
        producer = new StandardProducerBuilder().build();
    }


    private void initServer() {
        server = new SyncMutationServer(CoreOptions.SLAVE_CLUSTER_GRPC_PORT.defaultValue());
        
        Consumer<MutationDTO> callback = new Consumer<MutationDTO>() {

            public void accept(MutationDTO t) {
                LOGGER.logCustomDebug("Recv in callback, {} ",
                        "Scorpiour",
                        t.getGraphSpace() + " - "
                        + t.getGraphName() + " size "
                        + t.getMutation().length);

                
                HugeGraphMutateTopic topic = new HugeGraphMutateTopicBuilder()
                    .setBuffer(ByteBuffer.wrap(t.getMutation()))
                    .setGraphSpace(t.getGraphSpace())
                    .setGraphName(t.getGraphName())
                    .build();
                producer.produce(topic);
            }
        };
        server.registerListener("", callback);
    }

    public void init(GraphManager manager) {

        // Init consumer first
        this.initConsumer(manager);

        // Then init producer to handle mutation
        this.initProducer();
        
        // At last, init server wait for grpc 
        initServer();

    }

    /**
     * Close resource, be aware of the order
     */
    public synchronized void close() {
        if (closing) {
            return;
        }
        closing = true;
        // close server
        try {
            if (null != server) {
                server.stop();
            }
        } catch (Exception e) {
            
        }
        // close producer
        try {
            if (null != producer) {
                producer.close();
            }
        } catch (Exception e) {

        }
        // close consumer
        try {
            if (null != consumer) {
                consumer.close();
            }
        } catch (Exception e) {

        }

    }
    
}
