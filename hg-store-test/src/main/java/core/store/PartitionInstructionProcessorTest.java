package core.store;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.pulse.DbCompaction;
import com.baidu.hugegraph.pd.grpc.pulse.SplitPartition;
import com.baidu.hugegraph.pd.grpc.pulse.TransferLeader;
import com.baidu.hugegraph.store.PartitionInstructionProcessor;
import com.baidu.hugegraph.store.pd.FakePdServiceProvider;
import core.StoreEngineTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PartitionInstructionProcessorTest extends StoreEngineTestBase {

    PartitionInstructionProcessor processor;

    @Before
    public void init(){
        processor = new PartitionInstructionProcessor(getStoreEngine());
    }

    @Test
    public void testTransferLeader(){
        var engine = createPartitionEngine(0);
        engine.waitForLeader(1000);
        var shard = Metapb.Shard.newBuilder()
                .setStoreId(FakePdServiceProvider.makeStoreId("127.0.0.1:6511"))
                .setRole(Metapb.ShardRole.Leader)
                .build();

        TransferLeader trans = TransferLeader.newBuilder()
                .setShard(shard)
                .build();
        processor.onTransferLeader(1, getPartition(0), trans, integer -> {
            assertEquals(0, integer.intValue());
        });
    }

    @Test
    public void testDbCompaction() throws InterruptedException {
        var engine = createPartitionEngine(0);
        engine.waitForLeader(1000);
        DbCompaction dbCompaction = DbCompaction.newBuilder()
                .setTableName("test")
                .build();
        processor.onDbCompaction(2, getPartition(0), dbCompaction, integer -> {
            assertEquals(0, integer.intValue());
        });

        Thread.sleep(2000);
    }

    @Test
    public void testSplitPartition() throws InterruptedException {
        var engine = createPartitionEngine(0);
        engine.waitForLeader(1000);

        var partition = getPartition(0);
        System.out.println(partition);

        List<Metapb.Partition> list = new ArrayList<>();
        list.add(Metapb.Partition.newBuilder(partition.getProtoObj())
                .setStartKey(0)
                .setEndKey(20000)
                .build());
        list.add(Metapb.Partition.newBuilder(partition.getProtoObj())
                .setStartKey(20000)
                .setEndKey(65535)
                .setId(1).build());

        SplitPartition splitPartition = SplitPartition.newBuilder()
                        .addAllNewPartition(list)
                        .build();

        processor.onSplitPartition(3, partition, splitPartition, integer -> {
            assertEquals(0, integer.intValue());
        });

        Thread.sleep(2000);
        System.out.println(getStoreEngine().getPartitionEngines());
    }

    @Test
    public void testChangeShard(){

    }
}
