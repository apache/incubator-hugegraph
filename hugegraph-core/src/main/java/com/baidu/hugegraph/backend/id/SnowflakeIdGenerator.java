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
package com.baidu.hugegraph.backend.id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.TimeUtil;

public class SnowflakeIdGenerator extends IdGenerator {

    private static volatile SnowflakeIdGenerator instance;

    private IdWorker idWorker = null;

    public static SnowflakeIdGenerator instance() {
        if (instance == null) {
            synchronized (SnowflakeIdGenerator.class) {
                if (instance == null) {
                    // TODO: workerId, datacenterId should read from conf
                    instance = new SnowflakeIdGenerator(0, 0);
                }
            }
        }
        return instance;
    }

    public SnowflakeIdGenerator(long workerId, long datacenterId) {
        this.idWorker = new IdWorker(workerId, datacenterId);
    }

    public Id generate() {
        if (this.idWorker == null) {
            throw new HugeException("Please initialize before using it");
        }
        return generate(this.idWorker.nextId());
    }

    @Override
    public Id generate(SchemaElement entry) {
        return this.generate();
    }

    @Override
    public Id generate(HugeVertex entry) {
        return this.generate();
    }

    @Override
    public Id generate(HugeEdge entry) {
        return this.generate();
    }

    /*
     * Copyright 2010-2012 Twitter, Inc.
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *      http://www.apache.org/licenses/LICENSE-2.0
     * Unless required by applicable law or agreed to in writing,
     * software distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    static class IdWorker {

        protected static final Logger logger =
                  LoggerFactory.getLogger(IdWorker.class);

        private long workerId;
        private long datacenterId;
        private long sequence = 0L;

        private long workerIdBits = 5L;
        private long datacenterIdBits = 5L;
        private long maxWorkerId = -1L ^ (-1L << this.workerIdBits);
        private long maxDatacenterId = -1L ^ (-1L << this.datacenterIdBits);
        private long sequenceBits = 12L;

        private long workerIdShift = this.sequenceBits;
        private long datacenterIdShift = this.sequenceBits + this.workerIdBits;
        private long timestampLeftShift = this.sequenceBits +
                                          this.workerIdBits +
                                          this.datacenterIdBits;
        private long sequenceMask = -1L ^ (-1L << this.sequenceBits);

        private long lastTimestamp = -1L;

        public IdWorker(long workerId, long datacenterId) {
            // Sanity check for workerId
            if (workerId > this.maxWorkerId || workerId < 0) {
                throw new IllegalArgumentException(String.format(
                          "Worker id can't > %d or < 0",
                          this.maxWorkerId));
            }
            if (datacenterId > this.maxDatacenterId || datacenterId < 0) {
                throw new IllegalArgumentException(String.format(
                          "Datacenter id can't > %d or < 0",
                          this.maxDatacenterId));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
            logger.info("Worker starting. timestamp left shift {}," +
                        "datacenter id bits {}, worker id bits {}," +
                        "sequence bits {}, workerid {}",
                        this.timestampLeftShift,
                        this.datacenterIdBits,
                        this.workerIdBits,
                        this.sequenceBits,
                        workerId);
        }

        public synchronized long nextId() {
            long timestamp = TimeUtil.timeGen();

            if (timestamp < this.lastTimestamp) {
                logger.error("Clock is moving backwards, " +
                             "rejecting requests until {}.",
                             this.lastTimestamp);
                throw new HugeException("Clock moved backwards. Refusing to " +
                                        "generate id for %d milliseconds",
                                        this.lastTimestamp - timestamp);
            }

            if (this.lastTimestamp == timestamp) {
                this.sequence = (this.sequence + 1) & this.sequenceMask;
                if (this.sequence == 0) {
                    timestamp = TimeUtil.tillNextMillis(this.lastTimestamp);
                }
            } else {
                this.sequence = 0L;
            }

            this.lastTimestamp = timestamp;

            return (timestamp << this.timestampLeftShift) |
                   (this.datacenterId << this.datacenterIdShift) |
                   (this.workerId << this.workerIdShift) |
                   this.sequence;
        }

    }
}
