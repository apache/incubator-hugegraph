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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by zhangyi51 on 17/7/13.
 */
public class LockManager {

    private static final LockManager INSTANCE = new LockManager();

    public static LockManager instance() {
        return INSTANCE;
    }

    private Map<String, LockGroup> lockGroupMap;

    private LockManager() {
        this.lockGroupMap = new ConcurrentHashMap<>();
    }

    public boolean exists(String lockGroup) {
        return this.lockGroupMap.containsKey(lockGroup);
    }

    public LockGroup create(String lockGroup) {
        if (this.lockGroupMap.containsKey(lockGroup)) {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' already exists!", lockGroup));
        }
        LockGroup lockgroup = new LockGroup(lockGroup);

        this.lockGroupMap.put(lockGroup, lockgroup);
        return lockgroup;
    }

    public LockGroup get(String lockGroup) {
        if (!exists(lockGroup)) {
            throw new RuntimeException(String.format(
                      "Not exist LockGroup '%s'", lockGroup));
        }
        return this.lockGroupMap.get(lockGroup);
    }

    public void destroy(String lockGroup) {
        if (this.exists(lockGroup)) {
            this.lockGroupMap.remove(lockGroup);
        } else {
            throw new RuntimeException(String.format(
                      "Not exist LockGroup '%s'", lockGroup));
        }
    }
}
