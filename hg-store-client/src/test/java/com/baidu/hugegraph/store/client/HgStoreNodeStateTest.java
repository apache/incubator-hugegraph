package com.baidu.hugegraph.store.client;

// import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.baidu.hugegraph.store.util.HgStoreTestUtil.GRAPH_NAME;
import static com.baidu.hugegraph.store.util.HgStoreTestUtil.println;


public class HgStoreNodeStateTest {
    private final static HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
    static int nodeNumber = 0;

    static {
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9180");
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9280");
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9380");
    }

    private static void registerNode(String graphName, Long nodeId, String address) {
        nodeManager.addNode(graphName, nodeManager.getNodeBuilder().setNodeId(nodeId).setAddress(address).build());
    }


    // @Test
    public void isNodeHealthy() {
        AtomicInteger count = new AtomicInteger(0);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            nodeManager.getStoreNodes(GRAPH_NAME)
                    .stream().map(
                            node -> {
                                println(node.getNodeId() + " " + node.getAddress() + "is healthy: " + node.isHealthy());
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