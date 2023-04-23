package com.baidu.hugegraph.store.pd;


import com.baidu.hugegraph.pd.grpc.pulse.ChangeShard;
import com.baidu.hugegraph.pd.grpc.pulse.CleanPartition;
import com.baidu.hugegraph.pd.grpc.pulse.DbCompaction;
import com.baidu.hugegraph.pd.grpc.pulse.MovePartition;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import com.baidu.hugegraph.pd.grpc.pulse.SplitPartition;
import com.baidu.hugegraph.pd.grpc.pulse.TransferLeader;
import com.baidu.hugegraph.store.meta.Partition;

import java.util.function.Consumer;

public interface PartitionInstructionListener {
    void onChangeShard(long taskId, Partition partition, ChangeShard changeShard, Consumer<Integer> consumer);

    void onTransferLeader(long taskId, Partition partition, TransferLeader transferLeader, Consumer<Integer> consumer);

    void onSplitPartition(long taskId, Partition partition, SplitPartition splitPartition, Consumer<Integer> consumer);

    void onDbCompaction(long taskId, Partition partition, DbCompaction rocksdbCompaction,
                             Consumer<Integer> consumer);

    void onMovePartition(long taskId, Partition partition, MovePartition movePartition, Consumer<Integer> consumer);

    void onCleanPartition(long taskId, Partition partition, CleanPartition cleanPartition, Consumer<Integer> consumer);

    void onPartitionKeyRangeChanged(long taskId, Partition partition, PartitionKeyRange partitionKeyRange,
                                    Consumer<Integer> consumer);
}
