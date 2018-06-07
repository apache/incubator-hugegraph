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

package com.baidu.hugegraph.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.concurrent.LockManager;
import com.baidu.hugegraph.type.HugeType;

public final class LockUtil {

    private static final Logger LOG = Log.logger(LockUtil.class);

    public static final String WRITE = "write";
    public static final String READ = "read";

    public static final String INDEX_LABEL_DELETE = "il_delete";
    public static final String EDGE_LABEL_DELETE = "el_delete";
    public static final String VERTEX_LABEL_DELETE = "vl_delete";
    public static final String INDEX_LABEL_REBUILD = "il_rebuild";
    public static final String INDEX_LABEL_ADD_UPDATE = "il_add_update";
    public static final String EDGE_LABEL_ADD_UPDATE = "el_add_update";
    public static final String VERTEX_LABEL_ADD_UPDATE = "vl_add_update";
    public static final String PROPERTY_KEY_ADD_UPDATE = "pk_add_update";

    public static final long WRITE_WAIT_TIME = 30L;

    public static void init() {
        LockManager.instance().create(INDEX_LABEL_DELETE);
        LockManager.instance().create(EDGE_LABEL_DELETE);
        LockManager.instance().create(VERTEX_LABEL_DELETE);
        LockManager.instance().create(INDEX_LABEL_REBUILD);
        LockManager.instance().create(INDEX_LABEL_ADD_UPDATE);
        LockManager.instance().create(EDGE_LABEL_ADD_UPDATE);
        LockManager.instance().create(VERTEX_LABEL_ADD_UPDATE);
        LockManager.instance().create(PROPERTY_KEY_ADD_UPDATE);
    }

    private static Lock lockRead(String group, String lock) {
        Lock readLock = LockManager.instance().get(group)
                                   .readWriteLock(lock).readLock();
        LOG.debug("Trying to get the read lock '%s' of LockGroup '%s'",
                  lock, group);
        if (!readLock.tryLock()) {
            throw new HugeException(
                      "Lock [%s:%s] is locked by other operation",
                      group, lock);
        }
        LOG.debug("Got the read lock '%s' of LockGroup '%s'", lock, group);
        return readLock;
    }

    private static Lock lockWrite(String group, String lock, long time) {
        Lock writeLock = LockManager.instance().get(group)
                                    .readWriteLock(lock).writeLock();
        LOG.debug("Trying to get the write lock '%s' of LockGroup '%s'",
                  lock, group);
        while (true) {
            try {
                if (!writeLock.tryLock(time, TimeUnit.SECONDS)) {
                    throw new HugeException(
                              "Lock [%s:%s] is locked by other operation",
                              group, lock);
                }
                break;
            } catch (InterruptedException ignore) {
                LOG.info("Trying to lock write of is interrupted!");
            }
        }
        LOG.debug("Got the write lock '%s' of LockGroup '%s'", lock, group);
        return writeLock;
    }

    public static List<Lock> lock(String... locks) {
        List<Lock> lockList = new ArrayList<>();
        E.checkArgument(locks.length % 3 == 0,
                        "Invalid arguments number, expect multiple of 3.");
        for (int i = 0; i < locks.length; i += 3) {
            switch (locks[i]) {
                case WRITE:
                    lockList.add(lockWrite(locks[i + 1],
                                           locks[i + 2],
                                           WRITE_WAIT_TIME));
                    break;
                case READ:
                    lockList.add(lockRead(locks[i + 1], locks[i + 2]));
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                              "Invalid args '%s' at position '%s', " +
                              "expect 'write' or 'read'", locks[i], i));
            }
        }
        return lockList;
    }

    public static String hugeType2Group(HugeType type) {
        switch (type) {
            case PROPERTY_KEY:
                return PROPERTY_KEY_ADD_UPDATE;
            case VERTEX_LABEL:
                return VERTEX_LABEL_ADD_UPDATE;
            case EDGE_LABEL:
                return EDGE_LABEL_ADD_UPDATE;
            case INDEX_LABEL:
                return INDEX_LABEL_ADD_UPDATE;
            default:
                throw new AssertionError(String.format(
                          "Invalid HugeType '%s'", type));
        }
    }

    public static class Locks {

        public List<Lock> lockList;

        public Locks() {
            this.lockList = new ArrayList<>();
        }

        public void lockReads(String group, Id... locks) {
            for (Id lock : locks) {
                this.lockList.add(lockRead(group, lock.asString()));
            }
        }

        public void lockReads(String group, Collection<Id> locks) {
            for (Id lock : locks) {
                this.lockList.add(lockRead(group, lock.asString()));
            }
        }

        public void lockWrites(String group, Id... locks) {
            for (Id lock : locks) {
                this.lockList.add(lockWrite(group, lock.asString(), WRITE_WAIT_TIME));
            }
        }

        public void lockWrites(String group, Collection<Id> locks) {
            for (Id lock : locks) {
                this.lockList.add(lockWrite(group, lock.asString(), WRITE_WAIT_TIME));
            }
        }

        public void unlock() {
            for (Lock lock : this.lockList) {
                lock.unlock();
            }
        }
    }
}
