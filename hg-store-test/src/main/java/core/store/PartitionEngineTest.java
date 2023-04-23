package core.store;

import com.baidu.hugegraph.store.PartitionEngine;
import core.StoreEngineTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
public class PartitionEngineTest extends StoreEngineTestBase {

    PartitionEngine engine;

    @Before
    public void setup(){
        engine = createPartitionEngine(0);
    }

    @Test
    public void testHasPartition(){
        assertTrue(engine.hasPartition("graph0"));
    }

    @Test
    public void testGetGroupId(){
        assertEquals(engine.getGroupId().intValue(), 0);
    }

    @Test
    public void testGetShardGroup(){
        assertEquals(engine.getShardGroup().getShards().size(), 1);
    }

    @Test
    public void testIsLeader(){
        System.out.println(engine.isLeader());
    }

    @Test
    public void testGetLeader(){
        assertEquals(engine.getLeader().toString(), engine.getOptions().getRaftAddress());
    }

    @Test
    public void testGetAlivePeers(){
        try {
            System.out.println(engine.getAlivePeers().size());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetRaftNode(){
        assertNotNull(engine.getRaftNode());
    }

    @Test
    public void testGetPartitions(){
        assertEquals(engine.getPartitions().get("graph0").getId(), 0);
    }

    @Test
    public void testGetPartition(){
        assertEquals(engine.getPartition("graph0").getId(), 0);
    }

    @Test
    public void testGetCommittedIndex() throws InterruptedException {
        Thread.sleep(1000);
        System.out.println(engine.getCommittedIndex() );
    }

}
