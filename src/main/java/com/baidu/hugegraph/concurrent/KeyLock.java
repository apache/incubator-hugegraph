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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.concurrent;

import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;

public class KeyLock {

    private Striped<Lock> locks;

    public KeyLock() {
        this(Runtime.getRuntime().availableProcessors() << 2);
    }

    public KeyLock(int size) {
        this.locks = Striped.lock(size);
    }

    public final void lock(Object key) {
        this.locks.get(key).lock();
    }

    public final void unlock(Object key) {
        this.locks.get(key).unlock();
    }
}
