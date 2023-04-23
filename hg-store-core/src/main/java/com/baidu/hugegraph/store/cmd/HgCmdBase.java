package com.baidu.hugegraph.store.cmd;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HgCmdBase {

    public static final byte GET_STORE_INFO     = 0x01;
    public static final byte BATCH_PUT          = 0x02;
    public static final byte CLEAN_DATA         = 0x03;
    public static final byte RAFT_UPDATE_PARTITION         = 0x04;
    public static final byte ROCKSDB_COMPACTION         = 0x05;
    public static final byte CREATE_RAFT         = 0x06;
    public static final byte DESTROY_RAFT         = 0x07;
    @Data
    public abstract static class BaseRequest implements Serializable {
        private String graphName;
        private int partitionId;

        public abstract byte magic();
    }

    @Data
    public abstract static class BaseResponse implements Serializable {
        private HgCmdProcessor.Status status;
        List<PartitionLeader> partitionLeaders;

        public static class PartitionLeader implements Serializable {
            private Integer partId;
            private Long storeId;

            public PartitionLeader(Integer partId, Long storeId) {
                this.partId = partId;
                this.storeId = storeId;
            }

            public Long getStoreId() {
                return storeId;
            }

            public Integer getPartId() {
                return partId;
            }
        }

        public synchronized BaseResponse addPartitionLeader(PartitionLeader ptLeader) {
            if (partitionLeaders == null) {
                partitionLeaders = new ArrayList<>();
            }
            partitionLeaders.add(ptLeader);
            return this;
        }
    }
}
