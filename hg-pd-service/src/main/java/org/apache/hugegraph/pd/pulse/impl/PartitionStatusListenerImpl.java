package org.apache.hugegraph.pd.pulse.impl;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.listener.PartitionStatusListener;
import org.apache.hugegraph.pd.pulse.ChangeType;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;


public class PartitionStatusListenerImpl implements PartitionStatusListener {
    @Override
    public void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
        PDPulseSubjects.notifyPartitionChange(ChangeType.ALTER,
                                              partition.getGraphName(), partition.getId());
    }

    @Override
    public void onPartitionRemoved(Metapb.Partition partition) {
        PDPulseSubjects.notifyPartitionChange(ChangeType.DEL, partition.getGraphName(),
                                              partition.getId());

    }
}
