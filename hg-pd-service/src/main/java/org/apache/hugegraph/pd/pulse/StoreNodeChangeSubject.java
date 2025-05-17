package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.PulseNodeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseNodeResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author lynn.bond@hotmail.com created on 2023/11/07
 */
class StoreNodeChangeSubject extends AbstractObserverSubject {

    StoreNodeChangeSubject() {
        super(PulseType.PULSE_TYPE_STORE_NODE_CHANGE);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getNodeResponse().toString();
    }

    @Override
    Function<PulseNoticeRequest, PulseNodeRequest> getNoticeHandler() {
        return r -> r.getNodeRequest();
    }

//    void notifyClient(PulseNodeResponse.Builder responseBuilder) {
//        super.notifyClient(b -> b.setNodeResponse(responseBuilder));
//    }

    @Override
    long notifyClient(GeneratedMessageV3 response, String originId) {
        return super.send2Clients(b -> b.setNodeResponse((PulseNodeResponse) response),originId);
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds) {
        return super.send2Clients(b -> b.setNodeResponse((PulseNodeResponse) response),noticeId,observerIds);
    }
}
