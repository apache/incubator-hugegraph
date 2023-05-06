package org.apache.hugegraph.pd.pulse;

import com.baidu.hugegraph.pd.grpc.pulse.*;

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
        return r->r.getPartitionHeartbeatRequest();
    }

    void notifyClient(PartitionHeartbeatResponse.Builder responseBuilder) {

        super.notifyClient(b -> {
            b.setPartitionHeartbeatResponse(responseBuilder);;
        });

    }

    long notifyClient(PartitionHeartbeatResponse response) {
        return super.notifyClient(b -> {
            b.setPartitionHeartbeatResponse(response);;
        });
    }
}
