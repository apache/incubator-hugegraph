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

import java.util.function.Supplier;

import com.alipay.sofa.jraft.Status;

public class RaftResult {

    private final Status status;
    private final Supplier<Object> callback;
    private final Throwable exception;

    public RaftResult(Status status, Supplier<Object> callback) {
        this(status, callback, null);
    }

    public RaftResult(Status status, Throwable exception) {
        this(status, () -> null, exception);
    }

    public RaftResult(Status status, Supplier<Object> callback,
                      Throwable exception) {
        this.status = status;
        this.callback = callback;
        this.exception = exception;
    }

    public Status status() {
        return this.status;
    }

    public Supplier<Object> callback() {
        return this.callback;
    }

    public Throwable exception() {
        return this.exception;
    }
}
