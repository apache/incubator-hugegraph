package org.apache.hugegraph.pd.pulse;

import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;

/**
 * Bidirectional communication interface of pd-client and pd-server
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
public interface Pulse {

    /**
     *
     * @param listener
     * @return
     */
    PulseNotifier<PartitionHeartbeatRequest.Builder> connect(PulseListener<PulseResponse> listener);

    /**
     * 切换成新的host。做 channel/host的检查，如果需要关闭，notifier调用close方法。
     *
     * @param host new host
     * @param notifier notifier
     * @return true if create new stub, otherwise false
     */
    @Deprecated
    boolean resetStub(String host, PulseNotifier notifier);

    /*** inner static methods ***/
    static <T> PulseListener<T> listener(Consumer<T> onNext) {
        return listener(onNext, t -> {}, () -> {});
    }

    static <T> PulseListener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError) {
        return listener(onNext, onError, () -> {});
    }

    static <T> PulseListener<T> listener(Consumer<T> onNext, Runnable onCompleted) {
        return listener(onNext, t -> {}, onCompleted);
    }

    static <T> PulseListener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError, Runnable onCompleted) {
        return new PulseListener<T>() {
            @Override
            public void onNext(T response) {
                onNext.accept(response);
            }

            @Override
            public void onNotice(PulseServerNotice<T> notice) {

            }

            @Override
            public void onError(Throwable throwable) {
                onError.accept(throwable);
            }

            @Override
            public void onCompleted() {
                onCompleted.run();
            }
        };
    }
}