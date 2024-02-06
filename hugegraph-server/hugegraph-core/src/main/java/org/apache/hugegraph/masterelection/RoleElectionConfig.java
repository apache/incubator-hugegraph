/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.masterelection;

public class RoleElectionConfig implements Config {

    private final String node;
    private final String url;
    private final int exceedsFailCount;
    private final long randomTimeoutMillisecond;
    private final long heartBeatIntervalSecond;
    private final int masterDeadTimes;
    private final long baseTimeoutMillisecond;

    public RoleElectionConfig(String node, String url, int exceedsFailCount,
                              long randomTimeoutMillisecond, long heartBeatIntervalSecond,
                              int masterDeadTimes, long baseTimeoutMillisecond) {
        this.node = node;
        this.url = url;
        this.exceedsFailCount = exceedsFailCount;
        this.randomTimeoutMillisecond = randomTimeoutMillisecond;
        this.heartBeatIntervalSecond = heartBeatIntervalSecond;
        this.masterDeadTimes = masterDeadTimes;
        this.baseTimeoutMillisecond = baseTimeoutMillisecond;
    }

    @Override
    public String node() {
        return this.node;
    }

    @Override
    public String url() {
        return this.url;
    }

    @Override
    public int exceedsFailCount() {
        return this.exceedsFailCount;
    }

    @Override
    public long randomTimeoutMillisecond() {
        return this.randomTimeoutMillisecond;
    }

    @Override
    public long heartBeatIntervalSecond() {
        return this.heartBeatIntervalSecond;
    }

    @Override
    public int masterDeadTimes() {
        return this.masterDeadTimes;
    }

    @Override
    public long baseTimeoutMillisecond() {
        return this.baseTimeoutMillisecond;
    }
}
