package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;
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

    @Test
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

        assertEquals(10, getConfigService().getPartitionCount(DEFAULT_STORE_GROUP_ID));
        // restore
        getPdConfig().getConfigService().setPartitionCount(DEFAULT_STORE_GROUP_ID, 12);
    }
}
