package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 * @version 1.0.0
 */
public interface HgStoreNodePartitioner {

    /**
     * The partition algorithm implementation, that specialized by user.
     *
     * @param builder   The builder of HgNodePartitionerBuilder. It's supposed to be invoked directly by user.
     *                  <b>e.g. builder.add(nodeId,address,partitionId);</b>
     * @param graphName
     * @param startKey
     * @param endKey
     * @return status:
     * <ul>
     *     <li>0: The partitioner is OK.</li>
     *     <li>10: The partitioner is not work.</li>
     * </ul>
     */
    int partition(HgNodePartitionerBuilder builder, String graphName, byte[] startKey, byte[] endKey);

    /**
     * @param builder
     * @param graphName
     * @param startCode hash code
     * @param endCode   hash code
     * @return
     */
    default int partition(HgNodePartitionerBuilder builder, String graphName, int startCode, int endCode) {
        return this.partition(builder, graphName
                , HgStoreClientConst.ALL_PARTITION_OWNER
                , HgStoreClientConst.ALL_PARTITION_OWNER);
    }

    default int partition(HgNodePartitionerBuilder builder, String graphName, int partitionId) {
        return this.partition(builder, graphName
                , HgStoreClientConst.ALL_PARTITION_OWNER
                , HgStoreClientConst.ALL_PARTITION_OWNER);
    }
}
