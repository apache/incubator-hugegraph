package com.baidu.hugegraph.pd.upgrade.scripts;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.meta.MetadataKeyHelper;
import com.baidu.hugegraph.pd.meta.MetadataRocksDBStore;
import com.baidu.hugegraph.pd.upgrade.VersionUpgradeScript;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;

@Slf4j
public class PartitionMetaUpgrade implements VersionUpgradeScript {

    @Override
    public String getHighVersion() {
        return "3.6.2";
    }

    @Override
    public String getLowVersion() {
        return UNLIMITED_VERSION;
    }

    @Override
    public void runInstruction(PDConfig config) {

        log.info("run PartitionMetaUpgrade script");
        var dbStore = new MetadataRocksDBStore(config);

        try {
            var partSet = new HashSet<Integer>();
            for (var graph : dbStore.scanPrefix(Metapb.Graph.parser(), MetadataKeyHelper.getGraphPrefix())) {
                var graphPrefix = MetadataKeyHelper.getPartitionPrefix(graph.getGraphName());
                for (var partition : dbStore.scanPrefix(Metapb.PartitionV36.parser(), graphPrefix)) {
                    var newPartition = trans(partition);
                    var partId = partition.getId();
                    log.info("trans partition structure: from {} to {}", partition, newPartition);
                    // backup
                    var key36 = MetadataKeyHelper.getPartitionV36Key(graph.getGraphName(), partId);
                    dbStore.put(key36, partition.toByteArray());
                    // write new structure
                    var key = MetadataKeyHelper.getPartitionKey(graph.getGraphName(), partId);
                    dbStore.put(key, newPartition.toByteArray());

                    // construct shard group
                    if (! partSet.contains(partId)) {
                        var shardGroupKey = MetadataKeyHelper.getShardGroupKey(partId);
                        var shardGroup = dbStore.getOne(Metapb.ShardGroup.parser(), shardGroupKey);
                        if (shardGroup == null) {
                            var shardList = partition.getShardsList();
                            if (shardList.size() > 0) {
                                shardGroup = Metapb.ShardGroup.newBuilder()
                                        .setId(partId)
                                        .setVersion(partition.getVersion())
                                        .setConfVer(0)
                                        .setState(partition.getState())
                                        .addAllShards(shardList)
                                        .build();
                                dbStore.put(shardGroupKey, shardGroup.toByteArray());
                                log.info("extract shard group from partition, {}", shardGroup);
                            } else {
                                throw new PDException(1000, "trans partition failed, no shard list");
                            }
                        }
                        partSet.add(partId);
                    }

                }
            }
        } catch (Exception e) {
            log.error("script: {}, run error : {}", getClass().getName(), e.getMessage());
        }
    }

    @Override
    public boolean isRunOnce() {
        return true;
    }

    @Override
    public boolean isRunWithoutDataVersion() {
        return true;
    }

    private Metapb.Partition trans(Metapb.PartitionV36 partition) {

        return Metapb.Partition.newBuilder()
                .setId(partition.getId())
                .setGraphName(partition.getGraphName())
                .setStartKey(partition.getStartKey())
                .setEndKey(partition.getEndKey())
                .setVersion(partition.getVersion())
                .setState(partition.getState())
                .setMessage(partition.getMessage())
                .build();
    }
}
