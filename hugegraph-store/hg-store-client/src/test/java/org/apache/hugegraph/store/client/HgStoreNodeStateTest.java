/*
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

package org.apache.hugegraph.store.client;

// import org.junit.Test;

import static org.apache.hugegraph.store.util.HgStoreTestUtil.GRAPH_NAME;
import static org.apache.hugegraph.store.util.HgStoreTestUtil.println;

import java.util.concurrent.atomic.AtomicInteger;


public class HgStoreNodeStateTest {
    private final static HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
    static int nodeNumber = 0;

    static {
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9180");
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9280");
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9380");
    }

    private static void registerNode(String graphName, Long nodeId, String address) {
        nodeManager.addNode(graphName,
                            nodeManager.getNodeBuilder().setNodeId(nodeId).setAddress(address)
                                       .build());
    }


    // @Test
    public void isNodeHealthy() {
        AtomicInteger count = new AtomicInteger(0);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            nodeManager.getStoreNodes(GRAPH_NAME)
                       .stream().map(
                               node -> {
                                   println(node.getNodeId() + " " + node.getAddress() + "is " +
                                           "healthy: " +
                                           node.isHealthy());
                                   return node.isHealthy();
                               }
                       ).count();

            Thread.yield();

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}