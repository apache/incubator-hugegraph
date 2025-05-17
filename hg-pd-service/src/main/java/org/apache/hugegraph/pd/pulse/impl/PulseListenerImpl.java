package org.apache.hugegraph.pd.pulse.impl;

import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.pulse.PulseListener;
import org.apache.hugegraph.pd.service.PDService;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2024/2/27
 **/
@Slf4j
public class PulseListenerImpl implements PulseListener<PartitionHeartbeatRequest> {

    PDService pdService;

    public PulseListenerImpl(PDService pdService) {
        this.pdService = pdService;
    }

    @Override
    public void onNext(PartitionHeartbeatRequest request) throws Exception {
        this.pdService.getPartitionService().partitionHeartbeat(request.getStates());
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Received an error notice from pd-client", throwable);
    }

    @Override
    public void onCompleted() {
        log.info("Received an completed notice from pd-client");
    }
}
