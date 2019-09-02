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

package com.baidu.hugegraph.define;

import java.util.concurrent.atomic.AtomicInteger;

public final class WorkLoad {

    private final AtomicInteger load;

    public WorkLoad() {
        this(0);
    }

    public WorkLoad(int load) {
        this.load = new AtomicInteger(load);
    }

    public WorkLoad(AtomicInteger load) {
        this.load = load;
    }

    public AtomicInteger get() {
        return this.load;
    }

    public int incrementAndGet() {
        return this.load.incrementAndGet();
    }

    public int decrementAndGet() {
        return this.load.decrementAndGet();
    }
}
