package org.apache.hugegraph.pd.listener;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;

/**
 * 分区命令监听
 */
public interface PartitionInstructionListener {
    void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws PDException;

    void transferLeader(Metapb.Partition partition, TransferLeader transferLeader) throws PDException;

    void splitPartition(Metapb.Partition partition, SplitPartition splitPartition) throws PDException;

    void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws PDException;

    void movePartition(Metapb.Partition partition, MovePartition movePartition) throws PDException;

    void cleanPartition(Metapb.Partition partition, CleanPartition cleanPartition) throws PDException;

    void changePartitionKeyRange(Metapb.Partition partition, PartitionKeyRange partitionKeyRange) throws PDException;

}
