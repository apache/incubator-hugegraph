package core;

import core.raft.RaftOperationTest;
import core.raft.RaftUtilsTest;
import core.snapshot.HgSnapshotHandlerTest;
import core.store.HgStoreEngineTest;
import core.store.PartitionEngineTest;
import core.store.PartitionInstructionProcessorTest;
import core.store.meta.MetadataKeyHelperTest;
import core.store.meta.PartitionManagerTest;
import core.store.meta.TaskManagerTest;
import core.store.meta.asynctask.CleanTaskTest;
import core.store.util.MiscUtilClassTest;
import core.store.util.PartitionMetaStoreWrapperTest;
import core.store.util.ZipUtilsTest;
import lombok.extern.slf4j.Slf4j;
import util.CopyOnWriteCacheTest;
import util.UnsafeUtilTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        HgCmdClientTest.class,
        HgSnapshotHandlerTest.class,
        RaftUtilsTest.class,
        RaftOperationTest.class,
        UnsafeUtilTest.class,
        CopyOnWriteCacheTest.class,
        MetricServiceTest.class,
        TaskManagerTest.class,
        CleanTaskTest.class,
        MetadataKeyHelperTest.class,
        HgStoreEngineTest.class,
        PartitionEngineTest.class,
        PartitionManagerTest.class,
        PartitionMetaStoreWrapperTest.class,
        ZipUtilsTest.class,
        MiscUtilClassTest.class,
        PartitionInstructionProcessorTest.class,
        // 尽量放到最后
        HgBusinessImplTest.class
})

@Slf4j
public class CoreSuiteTest {


}