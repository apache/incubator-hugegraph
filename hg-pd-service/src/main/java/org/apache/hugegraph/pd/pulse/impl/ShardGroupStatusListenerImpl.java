package org.apache.hugegraph.pd.pulse.impl;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.listener.ShardGroupStatusListener;
import org.apache.hugegraph.pd.pulse.ChangeType;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;

/**
 * @author zhangyingjie
 * @date 2024/2/27
 **/
public class ShardGroupStatusListenerImpl implements ShardGroupStatusListener {
    @Override
    public void onShardListChanged(Metapb.ShardGroup shardGroup, Metapb.ShardGroup newShardGroup) {
        if (shardGroup == null && newShardGroup == null) {
            return;
        }

        // invoked before change, saved to db and update cache.
        if (newShardGroup == null) {
            PDPulseSubjects.notifyShardGroupChange(ChangeType.DEL, shardGroup.getId(), shardGroup);
        } else if (shardGroup == null) {
            PDPulseSubjects.notifyShardGroupChange(ChangeType.ADD, newShardGroup.getId(), newShardGroup);
        } else {
            PDPulseSubjects.notifyShardGroupChange(ChangeType.ALTER, shardGroup.getId(), newShardGroup);
        }
    }

    @Override
    public void onShardListOp(Metapb.ShardGroup shardGroup) {
        PDPulseSubjects.notifyShardGroupChange(ChangeType.USER_DEFINED, shardGroup.getId(), shardGroup);
    }
}
