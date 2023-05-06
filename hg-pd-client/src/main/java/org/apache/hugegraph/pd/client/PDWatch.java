package org.apache.hugegraph.pd.client;

import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
public interface PDWatch {

    /**
     * Watch the events of all store-nodes registered in the remote PD-Server.
     *
     * @param listener
     * @return
     */
    //PDWatcher watchNode(Listener<NodeEvent> listener);

    /**
     * Watch the events of the store-nodes assigned to a specified graph.
     *
     * @param graph the graph name which you want to watch
     * @param listener
     * @return
     */
    //PDWatcher watchNode(String graph, Listener<NodeEvent> listener);


    /**
     *
     * @param listener
     * @return
     */
    Watcher watchPartition(Listener<PartitionEvent> listener);

    Watcher watchNode(Listener<NodeEvent> listener);

    Watcher watchGraph(Listener<WatchResponse> listener);

    Watcher watchShardGroup(Listener<WatchResponse> listener);

    /*** inner static methods ***/
    static <T> Listener<T> listener(Consumer<T> onNext) {
        return listener(onNext, t -> {
        }, () -> {
        });
    }

    static <T> Listener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError) {
        return listener(onNext, onError, () -> {
        });
    }

    static <T> Listener<T> listener(Consumer<T> onNext, Runnable onCompleted) {
        return listener(onNext, t -> {
        }, onCompleted);
    }

    static <T> Listener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError, Runnable onCompleted) {
        return new Listener<T>() {
            @Override
            public void onNext(T response) {
                onNext.accept(response);
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
     * Interface of Watcher.
     */
    interface Listener<T> {
        /**
         * Invoked on new events.
         *
         * @param response the response.
         */
        void onNext(T response);

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

    interface Watcher extends Closeable {
        /**
         * closes this watcher and all its resources.
         */
        @Override
        void close();

        /**
         * Requests the latest revision processed and propagates it to listeners
         */
        // TODO: what's it for?
        //void requestProgress();
    }
}
