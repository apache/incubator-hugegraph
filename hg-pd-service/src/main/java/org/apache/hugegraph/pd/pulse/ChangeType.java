package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.PulseChangeType;
import org.apache.hugegraph.pd.grpc.watch.WatchChangeType;

/**
 * Copy from {@link WatchChangeType}
 *
 * @author original by zhangyingjie
 * @date 2023/11/08
 **/
public enum ChangeType {
    ADD(PulseChangeType.PULSE_CHANGE_TYPE_ADD),
    ALTER(PulseChangeType.PULSE_CHANGE_TYPE_ALTER),
    DEL(PulseChangeType.PULSE_CHANGE_TYPE_DEL),
    USER_DEFINED(PulseChangeType.PULSE_CHANGE_TYPE_SPECIAL1);

    private final PulseChangeType grpcType;

    ChangeType(PulseChangeType grpcType) {
        this.grpcType = grpcType;
    }

    public PulseChangeType getGrpcType() {
        return this.grpcType;
    }
}
