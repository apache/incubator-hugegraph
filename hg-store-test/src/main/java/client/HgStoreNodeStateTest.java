package client;

import com.baidu.hugegraph.store.client.HgStoreNodeManager;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import static util.HgStoreTestUtil.GRAPH_NAME;

public class HgStoreNodeStateTest {
    private static final  HgStoreNodeManager NODE_MANAGER = HgStoreNodeManager.getInstance();
    static int nodeNumber = 0;

    static {
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9180");
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9280");
        registerNode(GRAPH_NAME, Long.valueOf(nodeNumber++), "localhost:9380");
    }

    private static void registerNode(String graphName, Long nodeId, String address) {
        NODE_MANAGER.addNode(graphName, NODE_MANAGER.getNodeBuilder().setNodeId(nodeId).setAddress(address).build());
    }


    @Test
    public void isNodeHealthy() {
        AtomicInteger count = new AtomicInteger(0);

        for (int i = 0; i < 100; i++) {
            NODE_MANAGER.getStoreNodes(GRAPH_NAME)
                    .stream().map(
                            node -> {
                                System.out.println(node.getNodeId() + " " + node.getAddress()
                                        + "is healthy: " + node.isHealthy());
                                return node.isHealthy();
                            }
                    ).count();

            Thread.yield();
        }
    }
}
