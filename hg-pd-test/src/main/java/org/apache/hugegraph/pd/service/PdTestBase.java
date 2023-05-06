package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.ConfigService;
import com.baidu.hugegraph.pd.IdService;
import com.baidu.hugegraph.pd.PartitionInstructionListener;
import com.baidu.hugegraph.pd.PartitionService;
import com.baidu.hugegraph.pd.PartitionStatusListener;
import com.baidu.hugegraph.pd.StoreMonitorDataService;
import com.baidu.hugegraph.pd.StoreNodeService;
import com.baidu.hugegraph.pd.StoreStatusListener;
import com.baidu.hugegraph.pd.TaskScheduleService;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.pulse.ChangeShard;
import com.baidu.hugegraph.pd.grpc.pulse.CleanPartition;
import com.baidu.hugegraph.pd.grpc.pulse.DbCompaction;
import com.baidu.hugegraph.pd.grpc.pulse.MovePartition;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import com.baidu.hugegraph.pd.grpc.pulse.SplitPartition;
import com.baidu.hugegraph.pd.grpc.pulse.TransferLeader;
import com.baidu.hugegraph.pd.raft.RaftEngine;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

public class PdTestBase {
    private static PDConfig pdConfig;

    private static StoreNodeService storeNodeService;
    private static PartitionService partitionService;
    private static TaskScheduleService taskService;
    private static StoreMonitorDataService storeMonitorDataService;

    private static final String DATA_PATH = "/tmp/pd_data";

    @BeforeClass
    public static void initService() throws PDException {
        deleteDir(new File(DATA_PATH));

        PDConfig config = new PDConfig();
        config.setDataPath(DATA_PATH);
        config.setMinStoreCount(3);
        config.setInitialStoreList("127.0.0.1:8501");
        config.setHost("127.0.0.1");
        config.setVerifyPath("");
        config.setLicensePath("");
        PDConfig.Raft raft = new PDConfig().new Raft();
        raft.setAddress("127.0.0.1:8601");
        raft.setPeersList("127.0.0.1:8601");
        raft.setDataPath(DATA_PATH);
        raft.setHost("127.0.0.1");
        raft.setGrpcPort(8688);
        raft.setPort(8621);

        config.setRaft(raft);

        config.setStore(new PDConfig().new Store());
        config.setPartition( new PDConfig().new Partition(){{
            setShardCount(1);
            setTotalCount(12);
            setMaxShardsPerStore(12);
        }});
        config.setDiscovery(new PDConfig().new Discovery());

        pdConfig = config;

        var configService = new ConfigService(pdConfig);
        configService.loadConfig();

        var engine = RaftEngine.getInstance();
        engine.addStateListener(configService);
        engine.init(pdConfig.getRaft());
        engine.waitingForLeader(5000);

        storeNodeService = new StoreNodeService(pdConfig);
        partitionService = new PartitionService(pdConfig, storeNodeService);
        taskService = new TaskScheduleService(pdConfig, storeNodeService, partitionService);
        var idService = new IdService(pdConfig);
        storeMonitorDataService = new StoreMonitorDataService(pdConfig);
        RaftEngine.getInstance().addStateListener(partitionService);
        pdConfig.setIdService(idService);


        storeNodeService.init(partitionService);
        partitionService.init();
        partitionService.addInstructionListener(new PartitionInstructionListener() {
            @Override
            public void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws PDException {

            }

            @Override
            public void transferLeader(Metapb.Partition partition, TransferLeader transferLeader) throws PDException {

            }

            @Override
            public void splitPartition(Metapb.Partition partition, SplitPartition splitPartition) throws PDException {

            }

            @Override
            public void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws PDException {

            }

            @Override
            public void movePartition(Metapb.Partition partition, MovePartition movePartition) throws PDException {

            }

            @Override
            public void cleanPartition(Metapb.Partition partition, CleanPartition cleanPartition) throws PDException {

            }

            @Override
            public void changePartitionKeyRange(Metapb.Partition partition, PartitionKeyRange partitionKeyRange)
                    throws PDException {

            }
        });

        partitionService.addStatusListener(new PartitionStatusListener() {
            @Override
            public void onPartitionChanged(Metapb.Partition partition, Metapb.Partition newPartition) {

            }

            @Override
            public void onPartitionRemoved(Metapb.Partition partition) {

            }
        });

        storeNodeService.addStatusListener(new StoreStatusListener() {
            @Override
            public void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old, Metapb.StoreState status) {

            }

            @Override
            public void onGraphChange(Metapb.Graph graph, Metapb.GraphState stateOld, Metapb.GraphState stateNew) {

            }

            @Override
            public void onStoreRaftChanged(Metapb.Store store) {

            }
        });

        taskService.init();
    }

    @AfterClass
    public static void shutdownService(){
        var instance = RaftEngine.getInstance();
        if (instance != null) {
            instance.shutDown();
        }
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }

    public static StoreNodeService getStoreNodeService() {
        return storeNodeService;
    }

    public static PartitionService getPartitionService() {
        return partitionService;
    }

    public static PDConfig getPdConfig(){
        return pdConfig;
    }

    public static TaskScheduleService getTaskService() {
        return taskService;
    }

    public static StoreMonitorDataService getStoreMonitorDataService() {
        return storeMonitorDataService;
    }
}
