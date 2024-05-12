/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;

/**
 * Bidirectional communication interface of pd-client and pd-server
 */
public interface PDPulse {

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
        return new Listener<>() {
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
     * @param listener
     * @return
     */
    Notifier<PartitionHeartbeatRequest.Builder> connectPartition(Listener<PulseResponse> listener);

    /**
     * Switch to the new host. Do a channel/host check, and if you need to close, notifier calls
     * the close method.
     *
     * @param host     new host
     * @param notifier notifier
     * @return true if create new stub, otherwise false
     */
    boolean resetStub(String host, Notifier notifier);

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
        default void onNext(T response) {
        }

        /**
         * Invoked on new events.
         *
         * @param notice a wrapper of response
         */
        default void onNotice(PulseServerNotice<T> notice) {
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
     *
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
         *
         * @return
         */
        void notifyServer(T t);

        /**
         * Send an error report to pd-server.
         *
         * @param error
         */
        void crash(String error);

    }
}
