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

package org.apache.hugegraph.store.client.query;

import io.grpc.stub.StreamObserver;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hugegraph.store.client.type.HgStoreClientException;

@Slf4j
public class CommonKvStreamObserver<R, T> implements StreamObserver<R> {

    /**
     * 结果保存的队列
     */
    private final BlockingQueue<Iterator<T>> queue;

    /**
     * 向 server 发送请求的函数
     */
    @Setter
    private Consumer<Boolean> requestSender;

    /**
     * 当 server 没有结果时候，所做的处理，关闭双向流
     */
    @Setter
    private Consumer<Boolean> transferComplete;

    /**
     * 解析 server 返回数据的函数
     */
    private final Function<R, Iterator<T>> valueExtractor;
    /**
     * 解析 server 返回数据的状态
     */
    private final Function<R, ResultState> stateWatcher;

    /**
     * 是否结束，可以由 client 发送，即不接收多余的数据了
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Setter
    private long timeout = 1800 * 1000;

    /**
     * 监控内部的状态
     */
    private final ResultStateWatcher watcher = new ResultStateWatcher();

    public CommonKvStreamObserver(Function<R, Iterator<T>> valueExtractor,
                                  Function<R, ResultState> stateWatcher) {
        this.queue = new LinkedBlockingQueue<>();
        this.valueExtractor = valueExtractor;
        this.stateWatcher = stateWatcher;
    }

    /**
     * 发送请求
     */
    public void sendRequest() {
        if (!isServerFinished() && !closed.get()) {
            this.requestSender.accept(true);
            this.watcher.setState(ResultState.WAITING);
        }
    }

    public boolean isServerFinished() {
        return this.watcher.getState() == ResultState.FINISHED
               || this.watcher.getState() == ResultState.ERROR;
    }

    @Override
    public void onNext(R value) {
        watcher.setState(ResultState.INNER_BUSY);
        try {
            var state = stateWatcher.apply(value);
            log.debug("observer state: {}", state);

            switch (state) {
                case IDLE:
                case FINISHED:
                    if (! this.closed.get()) {
                        queue.offer(this.valueExtractor.apply(value));
                    }
                    // this.stop();
                    break;
                default:
                    queue.offer(new ErrorMessageIterator<>(state.getMessage()));
                    break;
            }
            watcher.setState(state);
            // sendRequest();
        } catch (Exception e) {
            log.error("handling server data, got error: ", e);
            queue.offer(new ErrorMessageIterator<>(e.getMessage()));
        }
    }

    public Iterator<T> consume() {
        try {
            while (!Thread.currentThread().isInterrupted() && (!this.queue.isEmpty() ||
                   !isServerFinished())) {
                var iterator = this.queue.poll(200, TimeUnit.MILLISECONDS);
                if (iterator != null) {
                    sendRequest();
                    return iterator;
                }

                if (System.currentTimeMillis() - watcher.current > this.timeout) {
                    throw new HgStoreClientException("iterator timeout");
                }

            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return null;
    }

    /**
     * 发送 onComplete，停止接收数据
     */
    public void clear() {
        if (! this.closed.get()) {
            this.closed.set(true);
            this.transferComplete.accept(true);
        }
        this.queue.clear();
    }

    @Override
    public void onError(Throwable t) {
        log.error("StreamObserver got error:", t);
        this.queue.offer(new ErrorMessageIterator<>(t.getMessage()));
        this.watcher.setState(ResultState.ERROR);
    }

    @Override
    public void onCompleted() {
        if (watcher.getState() != ResultState.ERROR) {
            watcher.setState(ResultState.FINISHED);
        }
    }

    public void setWatcherQueryId(String queryId) {
        this.watcher.setQueryId(queryId);
    }

    @Data
    private static class ResultStateWatcher{
        private long current = System.nanoTime();
        private volatile ResultState state = ResultState.IDLE;

        private String queryId;

        public void setState(ResultState state)  {
            log.debug("query Id: {}, COST_STAT: {} -> {}, cost {} ms",  this.queryId, this.state, state,
                    + (System.nanoTime() - current) * 1.0 / 1000000);
            this.state = state;
            this.current = System.nanoTime();
        }
    }
}
