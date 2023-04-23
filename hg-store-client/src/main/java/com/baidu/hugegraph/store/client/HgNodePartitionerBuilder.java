package com.baidu.hugegraph.store.client;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.CookieHandler;
import java.util.*;

import static com.baidu.hugegraph.store.client.util.HgAssert.isFalse;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/26
 * @version 1.0.0
 */
@NotThreadSafe
public final class HgNodePartitionerBuilder {

    private Set<HgNodePartition> partitions = null;
    /**
     *
     * @param nodeId
     * @param keyCode
     * @return
     * @see HgNodePartitionerBuilder:setPartitions(Set<HgNodePartition> partitions)
     */
    @Deprecated
    public HgNodePartitionerBuilder add(Long nodeId, Integer keyCode) {
        isFalse(nodeId == null, "The argument is invalid: nodeId");
        isFalse(keyCode == null, "The argument is invalid: keyCode");

        if(this.partitions==null){
            this.partitions=new HashSet<>(16,1);
        }

        this.partitions.add(HgNodePartition.of(nodeId, keyCode));
        return this;
    }

    public void setPartitions(Set<HgNodePartition> partitions) {
        isFalse(partitions == null, "The argument is invalid: partitions");
        this.partitions = partitions;
    }

    Collection<HgNodePartition> getPartitions() {
        return this.partitions;
    }

    static HgNodePartitionerBuilder resetAndGet() {
        return new HgNodePartitionerBuilder();
    }

}
