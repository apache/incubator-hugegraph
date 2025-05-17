package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.*;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
public class PartitionHeartbeatSubject extends AbstractObserverSubject {

    PartitionHeartbeatSubject() {
        super(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getPartitionHeartbeatResponse().toString();
    }

    @Override
    Function<PulseNoticeRequest, PartitionHeartbeatRequest> getNoticeHandler() {
        return r -> r.getPartitionHeartbeatRequest();
    }
//
//    void notifyClient(PartitionHeartbeatResponse.Builder responseBuilder) {
//
//        super.send2Clients(b -> {
//            b.setPartitionHeartbeatResponse(responseBuilder);;
//        });
//
//    }

    @Override
    long notifyClient(GeneratedMessageV3 response, String originId) {
        return super.send2Clients(b -> {
            b.setPartitionHeartbeatResponse((PartitionHeartbeatResponse) response);
        }, originId);
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds) {
        return super.send2Clients(b -> {
            b.setPartitionHeartbeatResponse((PartitionHeartbeatResponse) response);
        }, noticeId, observerIds);
    }
}
