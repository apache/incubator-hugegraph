package org.apache.hugegraph.pd.common;

import com.baidu.hugegraph.pd.common.KVPair;
import com.baidu.hugegraph.pd.common.PartitionCache;
import com.baidu.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartitionCacheTest {

    private PartitionCache cache ;

    @Before
    public void setup(){
        cache = new PartitionCache();
    }

    @Test
    public void testGetPartitionById(){
        var partition = createPartition(0, "graph0", 0, 65535);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition);
        var ret = cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
    }

    @Test
    public void testGetPartitionByKey() throws UnsupportedEncodingException {
        var partition = createPartition(0, "graph0", 0, 65535);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition);
        var ret = cache.getPartitionByKey("graph0", "0".getBytes("utf-8"));
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
    }

    @Test
    public void getPartitionByCode(){
        var partition = createPartition(0, "graph0", 0, 1024);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition);
        var ret = cache.getPartitionByCode("graph0", 10);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
        assertNull(cache.getPartitionByCode("graph0", 2000));
    }

    @Test
    public void testGetPartitions(){
        var partition1 = createPartition(0, "graph0", 0, 1024);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition1);
        assertEquals(cache.getPartitions("graph0").size(), 1);
        var partition2 = createPartition(1, "graph0", 1024, 2048);
        cache.updateShardGroup(creteShardGroup(1));
        cache.updatePartition(partition2);
        assertEquals(cache.getPartitions("graph0").size(), 2);
        System.out.print(cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testAddPartition(){
        var partition = createPartition(0, "graph0", 0, 65535);
        cache.addPartition("graph0", 0, partition);
        var ret = cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
        assertNotNull(cache.getPartitionByCode("graph0", 2000));
        System.out.print(cache.debugCacheByGraphName("graph0"));
        var partition2 = createPartition(0, "graph0", 0, 1024);
        cache.addPartition("graph0", 0, partition2);
        ret = cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition2);
        assertNull(cache.getPartitionByCode("graph0", 2000));
        System.out.print(cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testUpdatePartition(){
        var partition = createPartition(0, "graph0", 0, 65535);
        cache.updateShardGroup(creteShardGroup(0));
        cache.addPartition("graph0", 0, partition);
        var partition2 = createPartition(0, "graph0", 0, 1024);
        cache.updatePartition("graph0", 0, partition2);
        var ret = cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition2);
        assertNull(cache.getPartitionByCode("graph0", 2000));
    }

    @Test
    public void testUpdatePartition2(){
        var partition = createPartition(0, "graph0", 0, 1024);
        cache.updateShardGroup(creteShardGroup(0));
        assertTrue(cache.updatePartition(partition));
        assertFalse(cache.updatePartition(partition));
        var ret = cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
        assertNull(cache.getPartitionByCode("graph0", 2000));
    }

    @Test
    public void testRemovePartition(){
        var partition = createPartition(0, "graph0", 0, 1024);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition);
        assertNotNull(cache.getPartitionById("graph0", 0));
        cache.removePartition("graph0", 0);
        assertNull(cache.getPartitionById("graph0", 0));
        System.out.print(cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testRange(){
        var partition1 = createPartition(1, "graph0", 0, 3);
        var partition2 = createPartition(2, "graph0", 3, 6);
        cache.updatePartition(partition1);
        cache.updatePartition(partition2);

        var partition3 = createPartition(3, "graph0", 1, 2);
        var partition4 = createPartition(4, "graph0", 2, 3);

        cache.updatePartition(partition3);
        cache.updatePartition(partition4);
        System.out.println(cache.debugCacheByGraphName("graph0"));
        var partition6 = createPartition(1, "graph0", 0, 1);
        cache.updatePartition(partition6);


        System.out.println(cache.debugCacheByGraphName("graph0"));

        var partition5 = createPartition(1, "graph0", 0, 3);
        cache.updatePartition(partition5);
        System.out.println(cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testRange2(){
        var partition1 = createPartition(1, "graph0", 0, 3);
        var partition2 = createPartition(2, "graph0", 3, 6);
        cache.updatePartition(partition1);
        cache.updatePartition(partition2);

        System.out.println(cache.debugCacheByGraphName("graph0"));

        // 中间有缺失
        var partition3 = createPartition(1, "graph0", 2, 3);
        cache.updatePartition(partition3);

        System.out.println(cache.debugCacheByGraphName("graph0"));

        var partition5 = createPartition(1, "graph0", 0, 3);
        cache.updatePartition(partition5);
        System.out.println(cache.debugCacheByGraphName("graph0"));
    }


    @Test
    public void testRemovePartitions(){
        var partition1 = createPartition(0, "graph0", 0, 1024);
        var partition2 = createPartition(1, "graph0", 1024, 2048);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition1);
        cache.updateShardGroup(creteShardGroup(1));
        cache.updatePartition(partition2);
        assertEquals(cache.getPartitions("graph0").size(), 2);
        cache.removePartitions();
        assertEquals(cache.getPartitions("graph0").size(), 0);
    }



    @Test
    public void testRemoveAll(){
        var partition1 = createPartition(0, "graph0", 0, 1024);
        var partition2 = createPartition(1, "graph0", 1024, 2048);
        var partition3 = createPartition(0, "graph1", 0, 2048);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updateShardGroup(creteShardGroup(1));
        cache.updatePartition(partition1);
        cache.updatePartition(partition2);
        cache.updatePartition(partition3);

        assertEquals(cache.getPartitions("graph0").size(), 2);
        assertEquals(cache.getPartitions("graph1").size(), 1);
        cache.removeAll("graph0");
        assertEquals(cache.getPartitions("graph0").size(), 0);
        assertEquals(cache.getPartitions("graph1").size(), 1);
    }

    @Test
    public void testUpdateShardGroup(){
        var shardGroup = createShardGroup();
        cache.updateShardGroup(shardGroup);
        assertNotNull(cache.getShardGroup(shardGroup.getId()));
    }

    @Test
    public void testGetShardGroup(){
        var shardGroup = createShardGroup();
        cache.updateShardGroup(shardGroup);
        assertTrue(Objects.equals(cache.getShardGroup(shardGroup.getId()), shardGroup));
    }

    @Test
    public void testAddStore(){
        var store = createStore(1);
        cache.addStore(1L, store);
        assertEquals(cache.getStoreById(1L), store);
    }

    @Test
    public void testGetStoreById(){
        var store = createStore(1);
        cache.addStore(1L, store);
        assertEquals(cache.getStoreById(1L), store);
    }

    @Test
    public void testRemoveStore(){
        var store = createStore(1);
        cache.addStore(1L, store);
        assertEquals(cache.getStoreById(1L), store);

        cache.removeStore(1L);
        assertNull(cache.getStoreById(1L));
    }

    @Test
    public void testHasGraph(){
        var partition = createPartition(0, "graph0", 0, 65535);
        cache.updateShardGroup(creteShardGroup(0));
        cache.updatePartition(partition);
        assertTrue(cache.hasGraph("graph0"));
        assertFalse(cache.hasGraph("graph1"));
    }

    @Test
    public void testUpdateGraph(){
        var graph = createGraph("graph0", 10);
        cache.updateGraph(graph);
        assertEquals(cache.getGraph("graph0"), graph);
        graph = createGraph("graph0", 12);
        cache.updateGraph(graph);
        assertEquals(cache.getGraph("graph0"), graph);
    }

    @Test
    public void testGetGraph(){
        var graph = createGraph("graph0", 12);
        cache.updateGraph(graph);
        assertEquals(cache.getGraph("graph0"), graph);
    }

    @Test
    public void testGetGraphs(){
        var graph1 = createGraph("graph0", 12);
        var graph2 = createGraph("graph1", 12);
        var graph3 = createGraph("graph2", 12);
        cache.updateGraph(graph1);
        cache.updateGraph(graph2);
        cache.updateGraph(graph3);
        assertEquals(cache.getGraphs().size(), 3);
    }

    @Test
    public void testReset(){
        var graph1 = createGraph("graph0", 12);
        var graph2 = createGraph("graph1", 12);
        var graph3 = createGraph("graph2", 12);
        cache.updateGraph(graph1);
        cache.updateGraph(graph2);
        cache.updateGraph(graph3);
        assertEquals(cache.getGraphs().size(), 3);
        cache.reset();
        assertEquals(cache.getGraphs().size(), 0);
    }

    @Test
    public void testUpdateShardGroupLeader(){
        var shardGroup = createShardGroup();
        cache.updateShardGroup(shardGroup);

        var leader = Metapb.Shard.newBuilder().setStoreId(2).setRole(Metapb.ShardRole.Leader).build();
        cache.updateShardGroupLeader(shardGroup.getId(), leader);

        assertEquals(cache.getLeaderShard(shardGroup.getId()), leader);
    }

    private static Metapb.Partition createPartition(int pid, String graphName, long start, long end){
        return Metapb.Partition.newBuilder()
                .setId(pid)
                .setGraphName(graphName)
                .setStartKey(start)
                .setEndKey(end)
                .setState(Metapb.PartitionState.PState_Normal)
                .setVersion(1)
                .build();
    }

    private static Metapb.ShardGroup creteShardGroup(int pid) {
        return Metapb.ShardGroup.newBuilder()
                .addShards(
                    Metapb.Shard.newBuilder().setStoreId(0).setRole(Metapb.ShardRole.Leader).build()
                )
                .setId(pid)
                .setVersion(0)
                .setConfVer(0)
                .setState(Metapb.PartitionState.PState_Normal)
                .build();
    }

    private static Metapb.Shard createShard(){
        return Metapb.Shard.newBuilder()
                .setStoreId(0)
                .setRole(Metapb.ShardRole.Leader)
                .build();
    }

    private static Metapb.Store createStore(long storeId){
        return Metapb.Store.newBuilder()
                .setId(storeId)
                .setAddress("127.0.0.1")
                .setCores(4)
                .setVersion("1")
                .setDataPath("/tmp/junit")
                .setDataVersion(1)
                .setLastHeartbeat(System.currentTimeMillis())
                .setStartTimestamp(System.currentTimeMillis())
                .setState(Metapb.StoreState.Up)
                .setDeployPath("/tmp/junit")
                .build();
    }

    private static Metapb.Graph createGraph(String graphName, int partitionCount){
        return Metapb.Graph.newBuilder()
                .setGraphName(graphName)
                .setPartitionCount(partitionCount)
                .setState(Metapb.PartitionState.PState_Normal)
                .build();
    }

    private static Metapb.ShardGroup createShardGroup(){
        List<Metapb.Shard> shards = new ArrayList<>() ;
        for (int i = 0 ; i < 3 ; i ++ ) {
            shards.add(Metapb.Shard.newBuilder()
                    .setStoreId(i)
                    .setRole( i == 0 ? Metapb.ShardRole.Leader : Metapb.ShardRole.Follower)
                    .build()
            );
        }

        return Metapb.ShardGroup.newBuilder()
                .setId(1)
                .setVersion(1)
                .setConfVer(1)
                .setState(Metapb.PartitionState.PState_Normal)
                .addAllShards(shards)
                .build();
    }

}
