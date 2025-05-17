package org.apache.hugegraph.pd.pulse;


import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulsePartitionRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulsePartitionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author lynn.bond@hotmail.com created on 2023/11/08
 */
class PartitionChangeSubject extends AbstractObserverSubject {

    PartitionChangeSubject() {
        super(PulseType.PULSE_TYPE_PARTITION_CHANGE);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getPartitionResponse().toString();
    }

    @Override
    Function<PulseNoticeRequest, PulsePartitionRequest> getNoticeHandler() {
        return r -> r.getPartitionRequest();
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, String originId) {
        return super.send2Clients(b -> b.setPartitionResponse((PulsePartitionResponse) response),originId);
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds) {
        return super.send2Clients(b -> b.setPartitionResponse((PulsePartitionResponse) response),noticeId,observerIds);
    }
}
