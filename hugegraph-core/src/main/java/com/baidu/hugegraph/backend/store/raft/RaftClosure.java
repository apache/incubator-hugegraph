/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.backend.store.raft;

import static com.baidu.hugegraph.backend.store.raft.RaftSharedContext.WAIT_RAFT_LOG_TIMEOUT;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.Log;

public class RaftClosure<T> implements Closure {

    private static final Logger LOG = Log.logger(StoreClosure.class);

    private final CompletableFuture<RaftResult<T>> future;

    public RaftClosure() {
        this.future = new CompletableFuture<>();
    }

    public T waitFinished() throws Throwable {
        RaftResult<T> result = this.get();
        if (result.status().isOk()) {
            return result.callback().get();
        } else {
            throw result.exception();
        }
    }

    public Status status() {
        return this.get().status();
    }

    private RaftResult<T> get() {
        try {
            return this.future.get(WAIT_RAFT_LOG_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new BackendException("ExecutionException", e);
        } catch (InterruptedException e) {
            throw new BackendException("InterruptedException", e);
        } catch (TimeoutException e) {
            throw new BackendException("Wait closure timeout");
        }
    }

    public void complete(Status status, Supplier<T> callback) {
        this.future.complete(new RaftResult<>(status, callback));
    }

    public void failure(Status status, Throwable exception) {
        this.future.complete(new RaftResult<>(status, exception));
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            this.complete(status, () -> null);
        } else {
            LOG.error("Failed to apply command: {}", status);
            String msg = "Failed to apply command in raft node with error : " +
                         status.getErrorMsg();
            this.failure(status, new BackendException(msg));
        }
    }
}
