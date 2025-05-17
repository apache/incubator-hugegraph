package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.PdInstructionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collection;
import java.util.function.Function;

class PdInstructionSubject extends AbstractObserverSubject{

    public PdInstructionSubject() {
        super(PulseType.PULSE_TYPE_PD_INSTRUCTION);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getInstructionResponse().toString();
    }

    /**
     * pd单纯的向pulse发送的指令，不接收对应的notice
     * @return null
     */
    @Override
    Function<PulseNoticeRequest, PdInstructionSubject> getNoticeHandler() {
        return pulseNoticeRequest -> null;
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, String originId) {
        return super.send2Clients(b -> {
            b.setInstructionResponse((PdInstructionResponse) response);
        },originId);
    }

    @Override
    long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds) {
        return super.send2Clients(b -> {
            b.setInstructionResponse((PdInstructionResponse) response);
        },noticeId,observerIds);
    }
}
