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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.baidu.hugegraph.backend.BackendException;

public class StoreClosure implements Closure {

    private final StoreCommand command;
    private final CompletableFuture<RaftResult> future;

    public StoreClosure(StoreCommand command) {
        this.command = command;
        this.future = new CompletableFuture<>();
    }

    public StoreCommand command() {
        return this.command;
    }

    public CompletableFuture<RaftResult> future() {
        return this.future;
    }

    public Object data() {
        return this.get().data();
    }

    public Throwable throwable() {
        return this.get().throwable();
    }

    private RaftResult get() {
        try {
            return this.future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new BackendException("ExecutionException", e);
        }
    }

    public void complete(Object data) {
        this.future.complete(new RaftResult(data, null));
    }

    public void failure(Throwable ex) {
        this.future.complete(new RaftResult(null, ex));
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            this.complete(null);
        } else {
            this.failure(new BackendException("Failed to apply command in " +
                                              "raft node with error : %s",
                                              status.getErrorMsg()));
        }
    }

    public static class RaftResult {

        private Object data;
        private Throwable e;

        public RaftResult(Object data, Throwable e) {
            this.data = data;
            this.e = e;
        }

        public Object data() {
            return this.data;
        }

        public Throwable throwable() {
            return this.e;
        }
    }
}
