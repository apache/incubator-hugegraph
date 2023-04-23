package core.store;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.pd.FakePdServiceProvider;
import com.baidu.hugegraph.testutil.Assert;
import core.StoreEngineTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class HgStoreEngineTest extends StoreEngineTestBase {

    private HgStoreEngine engine;

    @Before
    public void setup(){
        engine = HgStoreEngine.getInstance();
    }

    @Test
    public void testGetInstance(){
        assertNotNull(HgStoreEngine.getInstance());
    }

    @Test
    public void testStateChanged() {
        createPartitionEngine(0);
        var storeId = FakePdServiceProvider.makeStoreId("127.0.0.1:6511");
        var store = engine.getPartitionManager().getStore(storeId);
        engine.stateChanged(store, Metapb.StoreState.Offline, Metapb.StoreState.Up);
        assertEquals(engine.getPartitionEngines().size(), 1);
    }

    @Test
    public void testCreatePartitionEngine(){
        var partition = getPartition(0);
        assertNotNull(engine.createPartitionEngine(partition));
    }

    @Test
    public void testCreatePartitionGroups(){
        var partition = getPartition(0);
        engine.createPartitionGroups(partition);
    }

    @Test
    public void testDestroyPartitionEngine(){
        createPartitionEngine(16);
        engine.destroyPartitionEngine(16, Arrays.asList("graph0"));
        // assertEquals(engine.getPartitionEngines().size(), 0);
    }

    @Test
    public void testDeletePartition(){
        createPartitionEngine(0);
        engine.deletePartition(0, "graph0");
        // TODO: check logic
        assertEquals(engine.getPartitionEngines().size(), 1);
    }

    @Test
    public void testGetLeaderPartition() throws InterruptedException {
        createPartitionEngine(0);
        assertEquals(engine.getLeaderPartition().size(), 1);
    }

    @Test
    public void testGetAlivePeers() throws InterruptedException {
        createPartitionEngine(0);
        assertEquals(engine.getAlivePeers(0).size(), 1);
    }

    @Test
    public void testGetLeaderTerm(){
        createPartitionEngine(0);
        // no vote
        assertEquals(engine.getLeaderTerm(0), -1);
    }

    @Test
    public void testGetCommittedIndex() throws InterruptedException {
        createPartitionEngine(0);
        // write something background
        Assert.assertTrue(engine.getCommittedIndex(0) > 0);
    }

    @Test
    public void testGetRaftRpcServer(){
        assertNotNull(engine.getRaftRpcServer());
    }

    @Test
    public void testGetPartitionManager(){
        assertNotNull(engine.getPartitionManager());
    }

    @Test
    public void testGetDataMover(){
        assertNotNull(engine.getDataMover());
    }

    @Test
    public void testGetPdProvider(){
        assertNotNull(engine.getPdProvider());
    }

    @Test
    public void testGetCmdClient(){
        assertNotNull(engine.getHgCmdClient());
    }

    @Test
    public void testGetHeartbeatService(){
        assertNotNull(engine.getHeartbeatService());
    }

    @Test
    public void testIsClusterReady() throws InterruptedException {
        // wait heart beat
        Thread.sleep(2000);
        assertNotNull(engine.isClusterReady());
    }

    @Test
    public void testGetDataLocations(){
        assertEquals(engine.getDataLocations().size(), 1);
    }

    @Test
    public void testGetPartitionEngine(){
        createPartitionEngine(0);
        assertNotNull(engine.getPartitionEngine(0));
    }

    @Test
    public void testGetPartitionEngines(){
        createPartitionEngine(0);
        assertEquals(engine.getPartitionEngines().size(), 1);
    }

    @Test
    public void testGetNodeMetrics(){
        assertNotNull(engine.getNodeMetrics());
    }

    @Test
    public void testGetRaftGroupCount(){
        createPartitionEngine(0);
        assertEquals(engine.getRaftGroupCount(), 1);
    }

}
