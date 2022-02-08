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

package com.baidu.hugegraph.example;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.kafka.SyncMutateConsumer;
import com.baidu.hugegraph.kafka.SyncMutateConsumerBuilder;
import com.baidu.hugegraph.kafka.consumer.StandardConsumer;
import com.baidu.hugegraph.kafka.consumer.StandardConsumerBuilder;
import com.baidu.hugegraph.kafka.producer.ProducerClient;
import com.baidu.hugegraph.kafka.producer.StandardProducerBuilder;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.syncgateway.MutationDTO;
import com.baidu.hugegraph.syncgateway.SyncMutationServer;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;


/**
 * Example of using kafka client
 */
public class KafkaExample extends PerfExampleBase {
    private final ProducerClient<String, ByteBuffer> producer
            = new StandardProducerBuilder().build();
    private StandardConsumer standardConsumer = new StandardConsumerBuilder().build();
    private SyncMutateConsumer mutateConsumer = new SyncMutateConsumerBuilder().build();
    
    public static void main(String[] args) throws Exception {

        SyncMutationServer server = new SyncMutationServer(51777);
        server.registerListener("kafka re-send", (MutationDTO dto) -> {

        });
        server.start();

       KafkaExample tester = new KafkaExample();

        String[] arg = new String[]{ "1", "1", "1", "false"};

        tester.test(arg);

    }

    @Override protected void initSchema(SchemaManager schema) {
        /*
        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age")
              .primaryKeys("name")
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .primaryKeys("name")
              .ifNotExist()
              .create();

        schema.edgeLabel("knows")
              .sourceLabel("person").targetLabel("person")
              .properties("date")
              .nullableKeys("date")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .sourceLabel("person").targetLabel("software")
              .properties("date")
              .nullableKeys("date")
              .ifNotExist()
              .create();
*/

    }

    @Override
    protected void testInsert(GraphManager graph, int times, int multiple) {

        produceExample(graph, 0, 0);

        try {
            Thread.sleep(10000);
        } catch (Exception e) {

        }

        consumeExample(graph.graph());

        try {
            Thread.sleep(1000 * 120);
        } catch (Exception e) {

        }

        producer.close();
        standardConsumer.close();
        mutateConsumer.close();
    }


    private void produceExample(GraphManager graph, int times, int multiple) {
        List<Object> personIds = new ArrayList<>(PERSON_NUM * multiple);
        List<Object> softwareIds = new ArrayList<>(SOFTWARE_NUM * multiple);

        for (int time = 0; time < times; time++) {
            LOG.debug("============== random person vertex ===============");
            for (int i = 0; i < PERSON_NUM * multiple; i++) {
                Random random = new Random();
                int age = random.nextInt(70);
                String name = "P" + random.nextInt();
                Vertex vertex = graph.addVertex(T.label, "person",
                                               "name", name, "age", age);
                personIds.add(vertex.id());
                LOG.debug("Add person: {}", vertex);
            }

            LOG.debug("============== random software vertex ============");
            for (int i = 0; i < SOFTWARE_NUM * multiple; i++) {
                Random random = new Random();
                int price = random.nextInt(10000) + 1;
                String name = "S" + random.nextInt();
                Vertex vertex = graph.addVertex(T.label, "software",
                                               "name", name, "lang", "java",
                                               "price", price);
                softwareIds.add(vertex.id());
                LOG.debug("Add software: {}", vertex);
            }

            LOG.debug("========== random knows & created edges ==========");
            for (int i = 0; i < EDGE_NUM / 2 * multiple; i++) {
                Random random = new Random();

                // Add edge: person --knows-> person
                Object p1 = personIds.get(random.nextInt(PERSON_NUM));
                Object p2 = personIds.get(random.nextInt(PERSON_NUM));
                graph.getVertex(p1).addEdge("knows", graph.getVertex(p2));

                // Add edge: person --created-> software
                Object p3 = personIds.get(random.nextInt(PERSON_NUM));
                Object s1 = softwareIds.get(random.nextInt(SOFTWARE_NUM));
                graph.getVertex(p3).addEdge("created", graph.getVertex(s1));
            }
        }

        graph.tx().commit();
/*
        
        BackendMutation mutation = new BackendMutation();

        String val = "{ \"key\": \"hello\", \"value\": \"world, this is raw binary test with lz4 compress with non-topic\"}";
        byte[] raw = val.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        return producer.produce("hugegraph-nospace-default", 0 ,  "hello", buffer);
        */
 
    }

    private String consumeExample(HugeGraph graph) {
        
        standardConsumer.consume();

        try {
            Thread.sleep(1000 * 30);
        } catch (Exception e) {

        }
        
        mutateConsumer.consume(graph);

        return "";
    }

}
