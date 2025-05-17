package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.grpc.pulse.PulseAckRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseCreateRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.util.IdUtil;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author lynn.bond@hotmail.com on 2023/11/6
 */
@Slf4j
class SubjectIndividualObserver implements StreamObserver<PulseRequest> {
    private final StreamObserver<PulseResponse> responseObserver;
    private AbstractObserverSubject subject;
    private Long observerId;

    private BiConsumer<AbstractObserverSubject, PulseAckRequest> ackConsumer = defaultAckConsumer();
    private Function<PulseType, AbstractObserverSubject> subjectProvider;

    static SubjectIndividualObserver of(StreamObserver<PulseResponse> responseObserver
            , Function<PulseType, AbstractObserverSubject> subjectProvider) {

        HgAssert.isArgumentNotNull(responseObserver, "responseObserver");
        HgAssert.isArgumentNotNull(subjectProvider, "subjectProvider");

        return new SubjectIndividualObserver(responseObserver, subjectProvider);
    }

    private SubjectIndividualObserver(StreamObserver<PulseResponse> responseObserver
            , Function<PulseType, AbstractObserverSubject> subjectProvider) {

        this.responseObserver = responseObserver;
        this.subjectProvider = subjectProvider;
    }

    public SubjectIndividualObserver setAckConsumer(BiConsumer<AbstractObserverSubject, PulseAckRequest> ackConsumer) {
        HgAssert.isArgumentNotNull(ackConsumer, "ackConsumer");
        this.ackConsumer = ackConsumer;
        return this;
    }

    private BiConsumer<AbstractObserverSubject, PulseAckRequest> defaultAckConsumer() {
        return (subject, ack) -> {
            log.info("[defaultAckConsumer] Receiving an ack of subject [ {} ], ack: {noticeId={},observerId={}}",
                    subject.getPulseType(), ack.getNoticeId(), ack.getObserverId());
        };
    }

    private void cancelObserver() {
        if (this.observerId == null) {
            this.responseObserver.onError(new Exception("Invoke cancel-observer before create-observer."));
            return;
        }

        this.subject.removeObserver(this.observerId, this.responseObserver);
    }

    private void addObserver(PulseCreateRequest request) {
        if (this.subject != null) {
            log.warn("Aborted a PulseCreateRequest because the subject already exists.");
            return;
        }

        PulseType pulseType = request.getPulseType();

        if (pulseType.equals(PulseType.PULSE_TYPE_UNKNOWN)) {
            log.warn("Aborted a PulseCreateRequest because of the unknown pulse type.");
            this.responseObserver.onError(new Exception("Unknown pulse type."));
            return;
        }

        try {
            this.subject = this.subjectProvider.apply(pulseType);
        } catch (Throwable t) {
            log.error("Failed to apply a subject with pulse type [" + pulseType + "], caused by: ", t);
            responseObserver.onError(new Exception("Failed to apply a subject with pulse type ["
                    + pulseType + "], caused by: ", t));
            return;
        }

        if (subject == null) {
            log.warn("Aborted a PulseCreateRequest because of an unsupported pulse type: [{}]", pulseType);
            responseObserver.onError(new Exception("Unsupported pulse type: " + pulseType.name()));
            return;
        }

        if (request.getObserverId() > 0L) {
            /* Accepted the observerId from the PulseCreateRequest, which is greater than 0. */
            this.observerId = request.getObserverId();
            log.info("Accepted observerId: [ {} ].", this.observerId);
        } else {
            /* Created a new observerId if the id passed in PulseCreateRequest is less than or equal to 0. */
            this.observerId = createObserverId();
            log.info("Created observerId: [ {} ].", this.observerId);
        }

        this.subject.addObserver(this.observerId, this.responseObserver);
    }

    private void ackNotice(PulseAckRequest ackRequest) {
        this.ackConsumer.accept(this.subject, ackRequest);
    }

    private void handleNotice(PulseNoticeRequest noticeRequest) {
        subject.handleClientNotice(noticeRequest);
    }

    private static Long createObserverId() {
        return IdUtil.createMillisId();
    }

    @Override
    public void onNext(PulseRequest pulseRequest) {
        if (pulseRequest.hasCreateRequest()) {
            this.addObserver(pulseRequest.getCreateRequest());
            return;
        }

        if (pulseRequest.hasCancelRequest()) {
            this.cancelObserver();
            return;
        }

        if (pulseRequest.hasNoticeRequest()) {
            this.handleNotice(pulseRequest.getNoticeRequest());
        }

        if (pulseRequest.hasAckRequest()) {
            this.ackNotice(pulseRequest.getAckRequest());
            return;
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Received a client's onError, subject [ {} ], error: {}"
                , subject == null ? "no subject" : subject.getPulseType(), throwable);
        this.cancelObserver();
    }

    @Override
    public void onCompleted() {
        this.cancelObserver();
    }

}