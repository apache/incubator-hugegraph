package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.StoreMonitorDataService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StoreMonitorDataServiceTest extends PdTestBase{

    StoreMonitorDataService service;

    @Before
    public void init(){
        service = getStoreMonitorDataService();
        var store = getPdConfig().getStore();
        store.setMonitorDataEnabled(true);
        store.setMonitorDataInterval("1s");
        getPdConfig().setStore(store);
    }

    @Test
    public void test() throws InterruptedException, PDException {
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 5; i++) {
            service.saveMonitorData(genStats());
            now =  System.currentTimeMillis() / 1000;
            Thread.sleep(1100);
        }
        assertTrue(service.getLatestStoreMonitorDataTimeStamp(1) == 0 ||
                service.getLatestStoreMonitorDataTimeStamp(1) == now);

        var data = service.getStoreMonitorData(1);
        assertEquals(5, data.size());

        assertNotNull(service.debugMonitorInfo(List.of(Metapb.RecordPair.newBuilder()
                .setKey("key1")
                .setValue(1)
                .build())));

        assertNotNull(service.getStoreMonitorDataText(1));


        service.removeExpiredMonitorData(1, now + 1);
        assertEquals(0, service.getStoreMonitorData(1).size());
    }


    private Metapb.StoreStats genStats(){
        return Metapb.StoreStats.newBuilder()
                .setStoreId(1)
                .addSystemMetrics(Metapb.RecordPair.newBuilder().setKey("key1").setValue(1).build())
                .build();
    }


}
