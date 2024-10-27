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

package org.apache.hugegraph.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private static final LockManager INSTANCE = new LockManager();

    public static LockManager instance() {
        return INSTANCE;
    }

    private Map<String, LockGroup> lockGroupMap;

    private LockManager() {
        this.lockGroupMap = new ConcurrentHashMap<>();
    }

    public boolean exists(String group) {
        return this.lockGroupMap.containsKey(group);
    }

    public LockGroup create(String group) {
        if (exists(group)) {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' already exists", group));
        }
        LockGroup lockGroup = new LockGroup(group);
        LockGroup previous = this.lockGroupMap.putIfAbsent(group, lockGroup);
        if (previous != null) {
            return previous;
        }
        return lockGroup;
    }

    public LockGroup get(String group) {
        LockGroup lockGroup = this.lockGroupMap.get(group);
        if (lockGroup == null) {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' does not exists", group));
        }
        return lockGroup;
    }

    public void destroy(String group) {
        if (this.exists(group)) {
            this.lockGroupMap.remove(group);
        } else {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' does not exists", group));
        }
    }
}
