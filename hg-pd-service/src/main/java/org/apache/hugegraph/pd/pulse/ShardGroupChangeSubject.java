package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseShardGroupRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseShardGroupResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author lynn.bond@hotmail.com created on 2023/11/08
 */
class ShardGroupChangeSubject extends AbstractObserverSubject {

    ShardGroupChangeSubject() {
        super(PulseType.PULSE_TYPE_SHARD_GROUP_CHANGE);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getShardGroupResponse().toString();
    }

    @Override
    Function<PulseNoticeRequest, PulseShardGroupRequest> getNoticeHandler() {
        return r -> r.getShardGroupRequest();
    }

//    void notifyClient(PulseShardGroupResponse.Builder responseBuilder) {
//        super.notifyClient(b -> b.setShardGroupResponse(responseBuilder));
//    }

    @Override
    long notifyClient(GeneratedMessageV3 response, String originId) {
        return super.send2Clients(b -> b.setShardGroupResponse((PulseShardGroupResponse) response),originId);
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds) {
        return super.send2Clients(b -> b.setShardGroupResponse((PulseShardGroupResponse) response),noticeId,observerIds);
    }
}
