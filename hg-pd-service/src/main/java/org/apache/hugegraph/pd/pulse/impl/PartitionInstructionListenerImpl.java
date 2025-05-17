package org.apache.hugegraph.pd.pulse.impl;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;
import org.apache.hugegraph.pd.listener.PartitionInstructionListener;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;
import org.apache.hugegraph.pd.service.PDService;

import java.util.List;

public class PartitionInstructionListenerImpl implements PartitionInstructionListener {

    PDService pdService;
    public PartitionInstructionListenerImpl(PDService pdService) {
        this.pdService = pdService;
    }

    private PartitionHeartbeatResponse.Builder getBuilder(Metapb.Partition partition) throws PDException {
        return PartitionHeartbeatResponse.newBuilder().setPartition(partition)
                                         .setId(pdService.getIdService().getId(PDService.TASK_ID_KEY, 1));
    }

    @Override
    public void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setChangeShard(changeShard), getStoreIds(partition.getId()));
    }

    @Override
    public void transferLeader(Metapb.Partition partition, TransferLeader transferLeader) throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setTransferLeader(transferLeader),
                getStoreIds(partition.getId()));
    }

    @Override
    public void splitPartition(Metapb.Partition partition, SplitPartition splitPartition) throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setSplitPartition(splitPartition),
                getStoreIds(partition.getId()));

    }

    @Override
    public void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setDbCompaction(dbCompaction),
                getStoreIds(partition.getId()));

    }

    @Override
    public void movePartition(Metapb.Partition partition, MovePartition movePartition) throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setMovePartition(movePartition),
                getStoreIds(partition.getId()));
    }

    @Override
    public void cleanPartition(Metapb.Partition partition, CleanPartition cleanPartition) throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setCleanPartition(cleanPartition),
                getStoreIds(partition.getId()));
    }

    @Override
    public void changePartitionKeyRange(Metapb.Partition partition, PartitionKeyRange partitionKeyRange)
            throws PDException {
        PDPulseSubjects.notifyClient(getBuilder(partition).setKeyRange(partitionKeyRange),
                getStoreIds(partition.getId()));
    }

    private List<Long> getStoreIds(int partId) throws PDException {
        return pdService.getStoreNodeService().getActiveStoresByPartition(partId);
    }
}
