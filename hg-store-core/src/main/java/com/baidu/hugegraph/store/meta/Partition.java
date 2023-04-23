package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;

import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.PartitionEngine;
import lombok.Data;

/**
 * @projectName: hugegraph-store
 * @package: com.baidu.hugegraph.store.partitions
 * @className: Partition
 * @author: tyzer
 * @description: TODO
 * @date: 2021/10/19 18:16
 * @version: 1.0
 */
@Data
public class Partition implements Cloneable{
    private int id;                                             // region id
    private String graphName;
    // Region key range [startKey, endKey)
    private long startKey;
    private long endKey;
    private long version;
    // shardlist版本，shardlist每次改变加1
    // private long confVer;

    private Metapb.PartitionState workState;
   // private PartitionRole role;
    // private List<Metapb.Shard> shardsList;
    // exclusive

    public
    Partition(){
        workState = Metapb.PartitionState.PState_Normal;
    }

    public Partition(Metapb.Partition protoObj){
        id = protoObj.getId();
        graphName = protoObj.getGraphName();
        startKey = protoObj.getStartKey();
        endKey = protoObj.getEndKey();
        // shardsList = protoObj.getShardsList();
        workState = protoObj.getState();
        version = protoObj.getVersion();
        // confVer = protoObj.getConfVer();
        if (workState == Metapb.PartitionState.UNRECOGNIZED || workState == Metapb.PartitionState.PState_None) {
            workState = Metapb.PartitionState.PState_Normal;
        }
    }

    public boolean isLeader() {
        PartitionEngine engine = HgStoreEngine.getInstance().getPartitionEngine(id);
        return engine == null ? false : engine.isLeader();
    }

    public Metapb.Partition getProtoObj(){
        return Metapb.Partition.newBuilder()
                .setId(id)
                .setVersion(version)
                // .setConfVer(confVer)
                .setGraphName(graphName)
                .setStartKey(startKey)
                .setEndKey(endKey)
                .setState(workState)
                // .addAllShards(shardsList)
                .build();
    }

    @Override
    public Partition clone() {
        Partition obj = null;
        try {
            obj = (Partition) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return obj;
    }

    @Override
    public String toString() {
        return getProtoObj().toString();
    }
}
