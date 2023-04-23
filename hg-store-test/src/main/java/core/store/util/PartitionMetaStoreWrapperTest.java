package core.store.util;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.MetadataKeyHelper;
import com.baidu.hugegraph.store.util.PartitionMetaStoreWrapper;

import core.StoreEngineTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PartitionMetaStoreWrapperTest extends StoreEngineTestBase {

    private PartitionMetaStoreWrapper wrapper;

    private Metapb.Partition partition;

    @Before
    public void setup(){
        wrapper = new PartitionMetaStoreWrapper();
        partition = Metapb.Partition.newBuilder()
                .setId(1)
                .setGraphName("graph0")
                .setStartKey(0L)
                .setEndKey(65535L)
                .build();
    }

    public static void putToDb(Metapb.Partition partition, PartitionMetaStoreWrapper wrapper){
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        wrapper.put(partition.getId(), key, partition.toByteArray());
    }

    @Test
    public void testGet(){
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        assertEquals(partition, wrapper.get(1, key, Metapb.Partition.parser()));
        byte[] key2 = MetadataKeyHelper.getPartitionKey("not_exists", partition.getId());
        assertNull(wrapper.get(1, key2, Metapb.Partition.parser()));
    }

    @Test
    public void testPut(){
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        var list = wrapper.scan(partition.getId(), Metapb.Partition.parser(), key);
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getGraphName(), partition.getGraphName());
    }

    @Test
    public void testDelete(){
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        wrapper.delete(partition.getId(), key);
        var list = wrapper.scan(partition.getId(), Metapb.Partition.parser(), key);
        assertEquals(list.size(), 0);
    }

    @Test
    public void testScan(){
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        var list = wrapper.scan(partition.getId(), Metapb.Partition.parser(), key);
        assertEquals(list.size(), 1);
    }

}
