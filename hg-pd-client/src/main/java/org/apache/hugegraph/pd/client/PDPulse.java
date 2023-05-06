package org.apache.hugegraph.pd.client;

import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * Bidirectional communication interface of pd-client and pd-server
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
public interface PDPulse {

    /**
     *
     * @param listener
     * @return
     */
    Notifier<PartitionHeartbeatRequest.Builder> connectPartition(Listener<PartitionHeartbeatResponse> listener);

    /*** inner static methods ***/
    static <T> Listener<T> listener(Consumer<T> onNext) {
        return listener(onNext, t -> {}, () -> {});
    }

    static <T> Listener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError) {
        return listener(onNext, onError, () -> {});
    }

    static <T> Listener<T> listener(Consumer<T> onNext, Runnable onCompleted) {
        return listener(onNext, t -> {}, onCompleted);
    }

    static <T> Listener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError, Runnable onCompleted) {
        return new Listener<T>() {
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

    /**
     * Interface of pulse.
     */
    interface Listener<T> {
        /**
         * Invoked on new events.
         *
         * @param response the response.
         */
        @Deprecated
        default void onNext(T response){};

        /**
         * Invoked on new events.
         * @param notice a wrapper of response
         */
        default void onNotice(PulseServerNotice<T> notice){
            notice.ack();
        }

        /**
         * Invoked on errors.
         *
         * @param throwable the error.
         */
        void onError(Throwable throwable);

        /**
         * Invoked on completion.
         */
        void onCompleted();

    }

    /**
     * Interface of notifier that can send notice to server.
     * @param <T>
     */
    interface Notifier<T> extends Closeable {
        /**
         * closes this watcher and all its resources.
         */
        @Override
        void close();

        /**
         * Send notice to pd-server.
         * @return
         */
        void notifyServer(T t);

        /**
         * Send an error report to pd-server.
         * @param error
         */
        void crash(String error);

    }
}