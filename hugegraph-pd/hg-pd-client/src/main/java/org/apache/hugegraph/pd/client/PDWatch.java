/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;

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

    String getCurrentHost();

    boolean checkChannel();

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

    static <T> Listener<T> listener(Consumer<T> onNext, Consumer<Throwable> onError,
                                    Runnable onCompleted) {
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
     * @param listener
     * @return
     */
    Watcher watchPartition(Listener<PartitionEvent> listener);

    Watcher watchNode(Listener<NodeEvent> listener);

    Watcher watchGraph(Listener<WatchResponse> listener);

    Watcher watchShardGroup(Listener<WatchResponse> listener);


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
        default void onCompleted() {};
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
