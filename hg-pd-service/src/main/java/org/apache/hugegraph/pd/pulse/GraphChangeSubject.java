package org.apache.hugegraph.pd.pulse;


import org.apache.hugegraph.pd.grpc.pulse.PulseGraphRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseGraphResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collection;
import java.util.function.Function;

/**
 * @author lynn.bond@hotmail.com created on 2023/11/08
 */
class GraphChangeSubject extends AbstractObserverSubject {

    GraphChangeSubject() {
        super(PulseType.PULSE_TYPE_GRAPH_CHANGE);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getGraphResponse().toString();
    }

    @Override
    Function<PulseNoticeRequest, PulseGraphRequest> getNoticeHandler() {
        return r -> r.getGraphRequest();
    }


    @Override
    long notifyClient(GeneratedMessageV3 response, String originId) {
        return super.send2Clients(b -> b.setGraphResponse((PulseGraphResponse) response), originId);
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds) {
        return super.send2Clients(b -> b.setGraphResponse((PulseGraphResponse) response), noticeId, observerIds);
    }
}
