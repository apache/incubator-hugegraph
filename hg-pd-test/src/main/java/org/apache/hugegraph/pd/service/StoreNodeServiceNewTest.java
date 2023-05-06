package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.StoreNodeService;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StoreNodeServiceNewTest extends PdTestBase{
    private StoreNodeService service;

    @Before
    public void init(){
        service = getStoreNodeService();
    }

    @Test
    public void testGetTaskInfoMeta(){
        assertNotNull(service.getTaskInfoMeta());
    }

    public void testGetStoreInfoMeta(){
        assertNotNull(service.getStoreInfoMeta());
    }

    @Test
    public void testRemoveShardGroup() throws PDException {
        for (int i = 0; i < 12; i++) {
            Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder()
                    .setId(i)
                    .setState(Metapb.PartitionState.PState_Offline)
                    .build();
            service.getStoreInfoMeta().updateShardGroup(group);
        }

        service.deleteShardGroup(11);
        service.deleteShardGroup(10);

        assertEquals(10, getPdConfig().getConfigService().getPDConfig().getPartitionCount());
        // restore
        getPdConfig().getConfigService().setPartitionCount(12);
    }
}
