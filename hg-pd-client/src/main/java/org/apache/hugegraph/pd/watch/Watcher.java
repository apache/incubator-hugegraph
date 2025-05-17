package org.apache.hugegraph.pd.watch;

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
public interface Watcher {

    static <T> WatchListener<T> listen(Consumer<T> onNext) {
        return listen(onNext, t -> {

        }, () -> {

        });
    }

    static <T> WatchListener<T> listen(Consumer<T> onNext, Consumer<Throwable> onError) {
        return listen(onNext, onError, () -> {

        });
    }

    static <T> WatchListener<T> listen(Consumer<T> onNext, Runnable onCompleted) {
        return listen(onNext, t -> {

        }, onCompleted);
    }

    static <T> WatchListener<T> listen(final Consumer<T> onNext, final Consumer<Throwable> onError,
                                       final Runnable onCompleted) {
        return new WatchListener<T>() {
            public void onNext(T response) {
                onNext.accept(response);
            }

            public void onError(Throwable throwable) {
                onError.accept(throwable);
            }

            public void onCompleted() {
                onCompleted.run();
            }
        };
    }

    default Closeable watchPartition(Consumer<PartitionEvent> consumer) {
        return watchPartition(listen(consumer));
    }

    Closeable watchPartition(WatchListener<PartitionEvent> paramWatchListener);

    default Closeable watchNode(Consumer<NodeEvent> consumer) {
        return watchNode(listen(consumer));
    }

    Closeable watchNode(WatchListener<NodeEvent> paramWatchListener);

    default Closeable watchGraph(Consumer<WatchResponse> consumer) {
        return watchGraph(listen(consumer));
    }

    Closeable watchGraph(WatchListener<WatchResponse> paramWatchListener);

    default Closeable watchShardGroup(Consumer<WatchResponse> consumer) {
        return watchShardGroup(listen(consumer));
    }

    Closeable watchShardGroup(WatchListener<WatchResponse> paramWatchListener);

    default Closeable watchPdPeers(Consumer<PulseResponse> consumer) {
        return watchPdPeers(listen(consumer));
    }
    Closeable watchPdPeers(WatchListener<PulseResponse> paramWatchListener);

    @Deprecated
    String getCurrentHost();

    @Deprecated
    boolean checkChannel();
}
