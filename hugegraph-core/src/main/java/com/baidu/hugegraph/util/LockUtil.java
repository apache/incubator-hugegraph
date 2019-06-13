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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public static final long WRITE_WAIT_TIMEOUT = 30L;

    public static void init(String graph) {
        LockManager.instance().create(join(graph, INDEX_LABEL_DELETE));
        LockManager.instance().create(join(graph, EDGE_LABEL_DELETE));
        LockManager.instance().create(join(graph, VERTEX_LABEL_DELETE));
        LockManager.instance().create(join(graph, INDEX_LABEL_REBUILD));
        LockManager.instance().create(join(graph, INDEX_LABEL_ADD_UPDATE));
        LockManager.instance().create(join(graph, EDGE_LABEL_ADD_UPDATE));
        LockManager.instance().create(join(graph, VERTEX_LABEL_ADD_UPDATE));
        LockManager.instance().create(join(graph, PROPERTY_KEY_ADD_UPDATE));
    }

    public static void destroy(String graph) {
        LockManager.instance().destroy(join(graph, INDEX_LABEL_DELETE));
        LockManager.instance().destroy(join(graph, EDGE_LABEL_DELETE));
        LockManager.instance().destroy(join(graph, VERTEX_LABEL_DELETE));
        LockManager.instance().destroy(join(graph, INDEX_LABEL_REBUILD));
        LockManager.instance().destroy(join(graph, INDEX_LABEL_ADD_UPDATE));
        LockManager.instance().destroy(join(graph, EDGE_LABEL_ADD_UPDATE));
        LockManager.instance().destroy(join(graph, VERTEX_LABEL_ADD_UPDATE));
        LockManager.instance().destroy(join(graph, PROPERTY_KEY_ADD_UPDATE));
    }

    private static String join(String graph, String group) {
        return graph + "_" + group;
    }

    private static Lock lockRead(String group, String lock) {
        Lock readLock = LockManager.instance().get(group)
                                   .readWriteLock(lock).readLock();
        LOG.debug("Trying to get the read lock '{}' of LockGroup '{}'",
                  lock, group);
        if (!readLock.tryLock()) {
            throw new HugeException(
                      "Lock [%s:%s] is locked by other operation",
                      group, lock);
        }
        LOG.debug("Got the read lock '{}' of LockGroup '{}'", lock, group);
        return readLock;
    }

    private static Lock lockWrite(String group, String lock, long time) {
        Lock writeLock = LockManager.instance().get(group)
                                    .readWriteLock(lock).writeLock();
        LOG.debug("Trying to get the write lock '{}' of LockGroup '{}'",
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
        LOG.debug("Got the write lock '{}' of LockGroup '{}'", lock, group);
        return writeLock;
    }

    public static List<Lock> lock(String... locks) {
        List<Lock> lockList = new ArrayList<>();
        E.checkArgument(locks.length % 3 == 0,
                        "Invalid arguments number, expect multiple of 3.");
        for (int i = 0; i < locks.length; i += 3) {
            switch (locks[i]) {
                case WRITE:
                    lockList.add(lockWrite(locks[i + 1], locks[i + 2],
                                           WRITE_WAIT_TIMEOUT));
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

    /**
     * Locks aggregate some locks that will be locked or unlocked together,
     * which means Locks can only be used in scenario where one Locks object
     * won't be accessed in different multiple threads.
     */
    public static class Locks {

        private final String graph;
        private final List<Lock> lockList;

        public Locks(String graph) {
            this.graph = graph;
            this.lockList = new ArrayList<>();
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void lockReads(String group, Id... locks) {
            for (Id lock : locks) {
                this.lockList.add(this.lockRead(group, lock));
            }
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void lockReads(String group, Collection<Id> locks) {
            for (Id lock : locks) {
                this.lockList.add(this.lockRead(group, lock));
            }
        }

        private Lock lockRead(String group, Id lock) {
            return LockUtil.lockRead(join(this.graph, group), lock.asString());
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void lockWrites(String group, Id... locks) {
            for (Id lock : locks) {
                this.lockList.add(this.lockWrite(group, lock));
            }
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void lockWrites(String group, Collection<Id> locks) {
            for (Id lock : locks) {
                this.lockList.add(this.lockWrite(group, lock));
            }
        }

        private Lock lockWrite(String group, Id lock) {
            return LockUtil.lockWrite(join(this.graph, group),
                                      lock.asString(),
                                      WRITE_WAIT_TIMEOUT);
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void unlock() {
            Collections.reverse(this.lockList);
            for (Lock lock : this.lockList) {
                lock.unlock();
            }
            this.lockList.clear();
        }
    }

    /**
     * LocksTable aggregate some locks that will be locked or unlocked together,
     * which means LocksTable can only be used in scenario where
     * one LocksTable object won't be accessed in different multiple threads.
     */
    public static class LocksTable {

        private Map<String, Set<Id>> table;
        private Locks locks;

        public LocksTable(String graph) {
            this.table = new HashMap<>();
            this.locks = new LockUtil.Locks(graph);
        }

        public void lockReads(String group, Id... locks) {
            this.lockReads(group, Arrays.asList(locks));
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void lockReads(String group, Collection<Id> locks) {
            List<Id> newLocks = new ArrayList<>(locks.size());
            Set<Id> locked = locksOfGroup(group);
            for (Id lock : locks) {
                if (!locked.contains(lock)) {
                    newLocks.add(lock);
                }
            }
            this.locks.lockReads(group, newLocks);
            locked.addAll(newLocks);
        }

        // NOTE: when used in multi-threads, should add `synchronized`
        public void unlock() {
            this.locks.unlock();
            this.table.clear();
        }

        private Set<Id> locksOfGroup(String group) {
            if (!this.table.containsKey(group)) {
                this.table.putIfAbsent(group, new HashSet<>());
            }
            return this.table.get(group);
        }
    }
}
