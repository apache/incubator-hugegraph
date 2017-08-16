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

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.concurrent.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class LockUtil {

    private static final Logger logger =
            LoggerFactory.getLogger(LockUtil.class);

    public static final String WRITE = "write";
    public static final String READ = "read";

    public static final String INDEX_LABEL = "indexLabel";
    public static final String EDGE_LABEL = "edgeLabel";
    public static final String VERTEX_LABEL = "vertexLabel";
    public static final String INDEX_REBUILD = "indexLabelRebuild";

    public static final long WRITE_WAIT_TIME = 30L;

    public static void init() {
        LockManager.instance().create(INDEX_LABEL);
        LockManager.instance().create(EDGE_LABEL);
        LockManager.instance().create(VERTEX_LABEL);
        LockManager.instance().create(INDEX_REBUILD);
    }

    private static Lock lockRead(String group, String lock) {
        Lock readLock = LockManager.instance().get(group)
                                   .readWriteLock(lock).readLock();
        logger.debug("Trying to get the read lock '%s' of LockGroup '%s'",
                     lock, group);
        if (!readLock.tryLock()) {
            throw new HugeException(
                      "Lock [%s:%s] is locked by other operation",
                      group, lock);
        }
        logger.debug("Got the read lock '%s' of LockGroup '%s'",
                     lock, group);
        return readLock;
    }

    private static Lock lockWrite(String group, String lock, long time) {
        Lock writeLock = LockManager.instance().get(group)
                                    .readWriteLock(lock).writeLock();
        logger.debug("Trying to get the write lock '%s' of LockGroup '%s'",
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
                logger.info("Trying to lock write of is interrupted!");
            }
        }
        logger.debug("Got the write lock '%s' of LockGroup '%s'",
                     lock, group);
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
                              "expected: 'write' or 'read'", locks[i], i));
            }
        }
        return lockList;
    }

    public static class Locks {

        public List<Lock> lockList;

        public Locks() {
            this.lockList = new ArrayList<>();
        }

        public void lockReads(String group, String... locks) {
            for (String lock : locks) {
                this.lockList.add(lockRead(group, lock));
            }
        }

        public void lockReads(String group, Collection<String> locks) {
            for (String lock : locks) {
                this.lockList.add(lockRead(group, lock));
            }
        }

        public void lockWrites(String group, String... locks) {
            for (String lock : locks) {
                this.lockList.add(lockWrite(group, lock, WRITE_WAIT_TIME));
            }
        }

        public void lockWrites(String group, Collection<String> locks) {
            for (String lock : locks) {
                this.lockList.add(lockWrite(group, lock, WRITE_WAIT_TIME));
            }
        }

        public void unlock() {
            for (Lock lock : this.lockList) {
                lock.unlock();
            }
        }
    }
}
