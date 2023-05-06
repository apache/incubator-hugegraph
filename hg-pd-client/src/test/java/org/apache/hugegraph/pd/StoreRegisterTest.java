package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.PDPulse;
import com.baidu.hugegraph.pd.common.KVPair;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import org.junit.Assert;
import org.junit.BeforeClass;
// import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class StoreRegisterTest {
    private static PDClient pdClient;

    private long storeId = 0;
    private final String storeAddr = "localhost";
    private final String graphName = "default/hugegraph/g";

    @BeforeClass
    public static void beforeClass() throws Exception {
        PDConfig config = PDConfig.of("localhost:8686");
        config.setEnableCache(true);
        pdClient = PDClient.create(config);
    }

    // @Test
    public void testRegisterStore() throws PDException {
        Metapb.Store store = Metapb.Store.newBuilder().setAddress(storeAddr).build();
        try {
            storeId = pdClient.registerStore(store);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertTrue("RegisterStore store_id = " + storeId, storeId != 0);
    }

    // @Test
    public void testGetStore() throws PDException {
        testRegisterStore();
        Metapb.Store store = pdClient.getStore(storeId);
        Assert.assertTrue(store.getAddress().equals(storeAddr));
        System.out.println(store);
    }
    // @Test
    public void testGetActiveStores() throws PDException {
        testRegisterStore();
        List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
        stores.forEach((e) -> {
            System.out.println("-------------------------------------");
            System.out.println(e);
        });
    }


    // @Test
    public void testStoreHeartbeat() throws PDException {
        testRegisterStore();
        Metapb.StoreStats stats = Metapb.StoreStats.newBuilder()
                .setStoreId(storeId)
                .build();
        pdClient.storeHeartbeat(stats);
        List<Metapb.Store> stores = pdClient.getActiveStores(graphName);
        boolean exist = false;
        for (Metapb.Store store : stores) {
            if (store.getId() == storeId) {
                exist = true;
                break;
            }
        }
        Assert.assertTrue(exist);
    }



    // @Test
    public void testPartitionHeartbeat() throws InterruptedException, PDException {
        testRegisterStore();
        PDPulse pdPulse = pdClient.getPulseClient();
        PDPulse.Notifier<PartitionHeartbeatRequest.Builder> notifier = pdPulse.connectPartition(
                new PDPulse.Listener<PartitionHeartbeatResponse>() {

                    @Override
                    public void onNext(PartitionHeartbeatResponse response) {

                    }

                    @Override
                    public void onNotice(PulseServerNotice<PartitionHeartbeatResponse> notice) {

                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {

                    }
                });
        KVPair<Metapb.Partition, Metapb.Shard> partShard = pdClient.getPartition("test", "1".getBytes(StandardCharsets.UTF_8));
        notifier.notifyServer(PartitionHeartbeatRequest.newBuilder()
                .setStates(
                        Metapb.PartitionStats.newBuilder()
                                .addGraphName("test")
                                .setId(partShard.getKey().getId())
                                .setLeader(Metapb.Shard.newBuilder()
                                        .setStoreId(1).build())));


        Thread.sleep(10000);
    }

}
