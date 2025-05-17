package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PulseGraphResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNodeResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulsePartitionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseShardGroupResponse;
import org.apache.hugegraph.pd.grpc.pulse.StoreNodeEventType;


import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;
import static org.apache.hugegraph.pd.common.HgAssert.isArgumentValid;

/**
 * @author lynn.bond@hotmail.com on 2023/11/8
 */
public class PulseNotices {
    private PulseNotices() {
    }

    public static PulsePartitionResponse createPartitionChange(ChangeType changeType,
                                                               String graph, int partitionId) {
        isArgumentNotNull(changeType, "changeType");
        isArgumentValid(graph, "graph");

        return PulsePartitionResponse.newBuilder()
                .setChangeType(changeType.getGrpcType())
                .setGraph(graph)
                .setPartitionId(partitionId)
                .build();
    }

    public static PulseShardGroupResponse createShardGroupChange(ChangeType changeType,
                                                                 int groupId, Metapb.ShardGroup shardGroup) {
        isArgumentNotNull(changeType, "changeType");
        isArgumentNotNull(shardGroup, "shardGroup");

        return PulseShardGroupResponse.newBuilder()
                .setShardGroupId(groupId)
                .setType(changeType.getGrpcType())
                .setShardGroup(shardGroup)
                .build();

    }

    public static PulseNodeResponse createNodeChange(StoreNodeEventType nodeEventType, String graph, long nodeId){
        isArgumentNotNull(nodeEventType, "nodeEventType");
        isArgumentNotNull(graph, "graph");

        return PulseNodeResponse.newBuilder()
                .setGraph(graph)
                .setNodeId(nodeId)
                .setNodeEventType(nodeEventType)
                .build();
    }

    public static PulseGraphResponse createGraphChange(Metapb.Graph graph){
        isArgumentNotNull(graph, "graph");

        return PulseGraphResponse.newBuilder()
                .setGraph(graph)
                .build();
    }

}
