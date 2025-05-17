package org.apache.hugegraph.pd.client;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.BaseTest;
import org.junit.Before;
import org.junit.Test;

import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.Query;

public class DiscoveryClientTest extends BaseTest {

    private DiscoveryClientImpl client;

    @Before
    public void setUp() {
        client = getClient("appName", "localhost:8654", new HashMap());
    }

    @Test
    public void testGetRegisterNode() {
        // Setup
        try {
            Consumer result = client.getRegisterConsumer();
            final NodeInfo expectedResult = NodeInfo.newBuilder()
                                                    .setAppName("appName")
                                                    .build();

            Thread.sleep(3000);
            Query query = Query.newBuilder().setAppName("appName")
                               .setVersion("0.13.0").build();

            // Run the test
            client.getNodeInfos(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }

    }

    private DiscoveryClientImpl getClient(String appName, String address,
                                          Map labels) {
        DiscoveryClientImpl discoveryClient = null;
        try {
            discoveryClient =
                    DiscoveryClientImpl.newBuilder().setCenterAddress(pdGrpcAddr).setAddress(address)
                                       .setAppName(appName)
                                       .setDelay(2000)
                                       .setVersion("0.13.0")
                                       .setId("0").setLabels(labels)
                                       .setPdConfig(getPdConfig())
                                       .build();
            discoveryClient.scheduleTask();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return discoveryClient;
    }
}
