package org.apache.hugegraph.pd.watch;

import org.apache.hugegraph.pd.grpc.pulse.PulseChangeType;
import org.apache.hugegraph.pd.grpc.pulse.PulseGraphResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseShardGroupResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchChangeType;
import org.apache.hugegraph.pd.grpc.watch.WatchGraphResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchShardGroupResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;


/**
 * @author lynn.bond@hotmail.com on 2023/11/8
 */
abstract class PDWatchPulseConverter {

    static WatchResponse toWatchShardGroupResponse(PulseResponse pulse) {
        WatchResponse.Builder builder = WatchResponse.newBuilder()
                .setWatchType(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE);

        if (pulse == null) {
            return builder.build();
        }

        builder.setWatcherId(pulse.getObserverId())
                .setNoticeId(pulse.getNoticeId());

        PulseShardGroupResponse origin = pulse.getShardGroupResponse();
        if (origin == null) {
            return builder.build();
        }

        builder.setShardGroupResponse(WatchShardGroupResponse.newBuilder()
                .setShardGroupId(origin.getShardGroupId())
                .setType(toWatchChangeType(origin.getType()))
                .setShardGroup(origin.getShardGroup())
                .build());

        return builder.build();
    }

    static WatchResponse toWatchGraphResponse(PulseResponse pulse) {
        WatchResponse.Builder builder = WatchResponse.newBuilder()
                .setWatchType(WatchType.WATCH_TYPE_GRAPH_CHANGE);

        if (pulse == null) {
            return builder.build();
        }

        builder.setWatcherId(pulse.getObserverId())
                .setNoticeId(pulse.getNoticeId());

        PulseGraphResponse origin = pulse.getGraphResponse();
        if (origin == null){
            return builder.build();
        }

        builder.setGraphResponse(WatchGraphResponse.newBuilder()
                .setGraph(origin.getGraph())
                .build());

        return builder.build();
    }

    private static WatchChangeType toWatchChangeType(PulseChangeType type) {
        switch (type) {
            case PULSE_CHANGE_TYPE_UNKNOWN:
                return WatchChangeType.WATCH_CHANGE_TYPE_UNKNOWN;
            case PULSE_CHANGE_TYPE_ADD:
                return WatchChangeType.WATCH_CHANGE_TYPE_ADD;
            case PULSE_CHANGE_TYPE_ALTER:
                return WatchChangeType.WATCH_CHANGE_TYPE_ALTER;
            case PULSE_CHANGE_TYPE_DEL:
                return WatchChangeType.WATCH_CHANGE_TYPE_DEL;
            case PULSE_CHANGE_TYPE_SPECIAL1:
                return WatchChangeType.WATCH_CHANGE_TYPE_SPECIAL1;
        }

        return WatchChangeType.WATCH_CHANGE_TYPE_UNKNOWN;
    }
}
