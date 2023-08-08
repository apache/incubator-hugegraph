/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package core;

import java.io.File;
import java.util.HashMap;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.DefaultDataMover;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.RaftRocksdbOptions;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.alipay.sofa.jraft.util.StorageOptionsFactory;

import lombok.extern.slf4j.Slf4j;
import util.UnitTestBase;

/**
 * 使用FakePd 和 FakePdOptions，初始化HgStoreEngine，该类的各项get函数可用
 */
@Slf4j
public class StoreEngineTestBase {
    private static final String DB_PATH = "/tmp/junit";

    @BeforeClass
    public static void initEngine() {
        UnitTestBase.deleteDir(new File(DB_PATH));

        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(DB_PATH);
        options.setRaftPath(DB_PATH);
        options.setFakePD(true);
        options.setRocksdbConfig(new HashMap<>() {{
            put("rocksdb.write_buffer_size", "1048576");
        }});
        options.setGrpcAddress("127.0.0.1:6511");
        options.setRaftAddress("127.0.0.1:6510");
        options.setDataTransfer(new DefaultDataMover());

        options.setFakePdOptions(new HgStoreEngineOptions.FakePdOptions() {{
            setStoreList("127.0.0.1");
            setPeersList("127.0.0.1");
            setPartitionCount(1);
            setShardCount(1);
        }});

        // TODO: uncomment later (jraft)
//        StorageOptionsFactory.clear();
        RaftRocksdbOptions.initRocksdbGlobalConfig(options.getRocksdbConfig());

        HgStoreEngine.getInstance().init(options);
    }

    public static Partition getPartition(int partitionId) {
        return getPartition(partitionId, "graph0");
    }

    public static Partition getPartition(int partitionId, String graphName) {
        Partition partition = new Partition();
        partition.setId(partitionId);
        partition.setGraphName(graphName);
        partition.setStartKey(0);
        partition.setEndKey(65535);
        partition.setWorkState(Metapb.PartitionState.PState_Normal);
        partition.setVersion(1);
        return partition;
    }

    /**
     * 创建 分区为0的partition engine. 该分区1个shard，为leader, graph name: graph0
     *
     * @return
     */
    public static PartitionEngine createPartitionEngine(int partitionId) {
        return createPartitionEngine(partitionId, "graph0");
    }


    public static PartitionEngine createPartitionEngine(int partitionId, String graphName) {
        Metapb.Shard shard = Metapb.Shard.newBuilder()
                                         .setStoreId(FakePdServiceProvider.makeStoreId(
                                                 "127.0.0.1:6511"))
                                         .setRole(Metapb.ShardRole.Leader)
                                         .build();

        Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                        .setId(partitionId)
                                                        .setConfVer(1)
                                                        .setVersion(1)
                                                        .setState(
                                                                Metapb.PartitionState.PState_Normal)
                                                        .addShards(shard)
                                                        .build();

        getStoreEngine().getPartitionManager().updateShardGroup(ShardGroup.from(shardGroup));

        var engine = getStoreEngine().createPartitionEngine(getPartition(partitionId, graphName));
        engine.waitForLeader(2000);
        return engine;
    }

    public static HgStoreEngine getStoreEngine() {
        return HgStoreEngine.getInstance();
    }

    @AfterClass
    public static void shutDownEngine() {
        try {
            HgStoreEngine.getInstance().shutdown();
        } catch (Exception e) {
            log.error("shut down engine error: {}", e.getMessage());
        }
    }
}
