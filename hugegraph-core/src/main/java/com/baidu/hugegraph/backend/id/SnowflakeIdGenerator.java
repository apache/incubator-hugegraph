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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.TimeUtil;

public class SnowflakeIdGenerator extends IdGenerator {

    private static Map<String, SnowflakeIdGenerator> INSTANCES;

    private final boolean forceString;
    private IdWorker idWorker = null;

    public static SnowflakeIdGenerator instance(HugeGraph graph) {
        if (INSTANCES == null) {
            INSTANCES = new ConcurrentHashMap<>();
        }
        if (!INSTANCES.containsKey(graph.name())) {
            HugeConfig config = graph.configuration();
            SnowflakeIdGenerator instance = new SnowflakeIdGenerator(config);
            INSTANCES.put(graph.name(), instance);
        }
        return INSTANCES.get(graph.name());
    }

    private SnowflakeIdGenerator(HugeConfig config) {
        long workerId = config.get(CoreOptions.SNOWFLAKE_WORKER_ID);
        long datacenterId = config.get(CoreOptions.SNOWFLAKE_DATACENTER_ID);
        this.forceString = config.get(CoreOptions.SNOWFLAKE_FORCE_STRING);
        this.idWorker = new IdWorker(workerId, datacenterId);
    }

    public Id generate() {
        if (this.idWorker == null) {
            throw new HugeException("Please initialize before using");
        }
        Id id = this.generate(this.idWorker.nextId());
        if (!this.forceString) {
            return id;
        } else {
            return IdGenerator.of(id.asString());
        }
    }

    @Override
    public Id generate(HugeVertex vertex) {
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

        protected static final Logger LOG = Log.logger(IdWorker.class);

        private long workerId;
        private long datacenterId;
        private long sequence = 0L; // AtomicLong
        private long lastTimestamp = -1L;

        private static final long WORKER_BIT = 5L;
        private static final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_BIT);

        private static final long DC_BIT = 5L;
        private static final long MAX_DC_ID = -1L ^ (-1L << DC_BIT);

        private static final long SEQUENCE_BIT = 12L;
        private static final long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BIT);

        private static final long WORKER_SHIFT = SEQUENCE_BIT;
        private static final long DC_SHIFT = WORKER_SHIFT + WORKER_BIT;
        private static final long TIMESTAMP_SHIFT = DC_SHIFT + DC_BIT;

        public IdWorker(long workerId, long datacenterId) {
            // Sanity check for workerId
            if (workerId > MAX_WORKER_ID || workerId < 0) {
                throw new IllegalArgumentException(String.format(
                          "Worker id can't > %d or < 0",
                          MAX_WORKER_ID));
            }
            if (datacenterId > MAX_DC_ID || datacenterId < 0) {
                throw new IllegalArgumentException(String.format(
                          "Datacenter id can't > %d or < 0",
                          MAX_DC_ID));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
            LOG.debug("Id Worker starting. timestamp left shift {}," +
                      "datacenter id bits {}, worker id bits {}," +
                      "sequence bits {}",
                      TIMESTAMP_SHIFT, DC_BIT, WORKER_BIT, SEQUENCE_BIT);
            LOG.info("Id Worker starting. datacenter id {}, worker id {}",
                     datacenterId, workerId);
        }

        public synchronized long nextId() {
            long timestamp = TimeUtil.timeGen();

            if (timestamp > this.lastTimestamp) {
                this.sequence = 0L;
            } else if (timestamp == this.lastTimestamp) {
                this.sequence = (this.sequence + 1) & SEQUENCE_MASK;
                if (this.sequence == 0) {
                    timestamp = TimeUtil.tillNextMillis(this.lastTimestamp);
                }
            } else {
                assert timestamp < this.lastTimestamp;
                LOG.error("Clock is moving backwards, " +
                          "rejecting requests until {}.",
                          this.lastTimestamp);
                throw new HugeException("Clock moved backwards. Refusing to " +
                                        "generate id for %d milliseconds",
                                        this.lastTimestamp - timestamp);
            }

            this.lastTimestamp = timestamp;

            return (timestamp << TIMESTAMP_SHIFT) |
                   (this.datacenterId << DC_SHIFT) |
                   (this.workerId << WORKER_SHIFT) |
                   (this.sequence);
        }
    }
}
