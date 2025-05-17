package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;
import org.apache.hugegraph.pd.pulse.PulseDurableProvider;
import org.apache.hugegraph.pd.service.interceptor.GrpcAuthentication;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */

@Slf4j
@GRpcService(interceptors = {GrpcAuthentication.class})
@Component("pdPulseService")
public class PDPulseService extends HgPdPulseGrpc.HgPdPulseImplBase {

    @Autowired
    private PulseDurableProvider durableQueueProvider;

    @PostConstruct
    public void init() {
        PDPulseSubjects.setDurableQueueProvider(this.durableQueueProvider);
    }

    @Override
    public StreamObserver<PulseRequest> pulse(StreamObserver<PulseResponse> responseObserver) {
        return PDPulseSubjects.addObserver(responseObserver);
    }

}
