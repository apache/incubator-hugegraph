package org.apache.hugegraph.pd.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyingjie
 * @date 2023/7/25
 **/
@Data
public class GraphStatistics {

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private transient PDRestService pdRestService;
    // 图统计信息
    String graphName;
    long partitionCount;
    String state;
    int storeGroupId;
    List<Partition> partitions;
    long dataSize;
    int nodeCount;
    int edgeCount;
    long keyCount;

    public GraphStatistics(Metapb.Graph graph, PDRestService restService, PDService pdService) throws PDException {
        this.pdRestService = restService;
        if (graph == null) {
            return;
        }
        Map<Integer, Long> partition2DataSize = new HashMap<>();
        graphName = graph.getGraphName();
        partitionCount = graph.getPartitionCount();
        state = String.valueOf(graph.getState());
        storeGroupId = graph.getStoreGroupId();
        // 数据量及key的数量
        List<Metapb.Store> stores = pdRestService.getStores(graphName);
        for (Metapb.Store store : stores) {
            List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
            for (Metapb.GraphStats graphStats : graphStatsList) {
                if ((graphName.equals(graphStats.getGraphName()))
                    && (Metapb.ShardRole.Leader.equals(graphStats.getRole()))) {
                    keyCount += graphStats.getApproximateKeys();
                    dataSize += graphStats.getApproximateSize();
                    partition2DataSize.put(graphStats.getPartitionId(), graphStats.getApproximateSize());
                }
            }
        }
        List<Partition> resultPartitionList = new ArrayList<>();
        List<Metapb.Partition> tmpPartitions = pdRestService.getPartitions(graphName);
        if ((tmpPartitions != null) && (!tmpPartitions.isEmpty())) {
            // 需要返回的分区信息
            for (Metapb.Partition partition : tmpPartitions) {
                Metapb.PartitionStats partitionStats = pdRestService.getPartitionStats(graphName, partition.getId());
                Partition pt = new Partition(partition, partitionStats, pdService);
                pt.dataSize = partition2DataSize.getOrDefault(partition.getId(), 0L);
                resultPartitionList.add(pt);
            }
        }
        partitions = resultPartitionList;
        // 隐去图名后面的 /g /m /s
        final int postfixLength = 2;
        graphName = graphName.substring(0, graphName.length() - postfixLength);
    }
}
