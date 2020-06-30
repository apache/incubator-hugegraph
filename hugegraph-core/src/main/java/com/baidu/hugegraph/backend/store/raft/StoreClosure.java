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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class StoreClosure implements Closure {

    private static final Logger LOG = Log.logger(StoreClosure.class);

    // unit seconds
    private static final int FUTURE_TIMEOUT = 30;

    // the command can be null
    private final StoreCommand command;
    private final CompletableFuture<RaftResult> future;

    public StoreClosure(StoreCommand command) {
        E.checkNotNull(command, "store command");
        this.command = command;
        this.future = new CompletableFuture<>();
    }

    public StoreCommand command() {
        return this.command;
    }

    public CompletableFuture<RaftResult> future() {
        return this.future;
    }

    public Status status() {
        return this.get().status();
    }

    public Object data() {
        return this.get().data();
    }

    public Throwable throwable() {
        return this.get().throwable();
    }

    private RaftResult get() {
        try {
            return this.future.get(FUTURE_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new BackendException("ExecutionException", e);
        } catch (TimeoutException e) {
            throw new BackendException("Wait store command '%s' timeout",
                                       this.command.action());
        }
    }

    public void complete(Status status, Object data) {
        this.future.complete(new RaftResult(status, data, null));
    }

    public void failure(Status status, Throwable ex) {
        this.future.complete(new RaftResult(status, null, ex));
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            this.complete(status, null);
        } else {
            LOG.error("Failed to apply command: {}", status);
            String msg = "Failed to apply command in raft node with error : " +
                         status.getErrorMsg();
            this.failure(status, new BackendException(msg));
        }
    }
}
