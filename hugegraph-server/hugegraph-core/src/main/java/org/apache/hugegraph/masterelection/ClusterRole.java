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

import java.util.Objects;

public class ClusterRole {

    private final String node;
    private long clock;
    private final int epoch;
    private final String url;

    public ClusterRole(String node, String url, int epoch) {
        this(node, url, epoch, 1);
    }

    public ClusterRole(String node, String url, int epoch, long clock) {
        this.node = node;
        this.url = url;
        this.epoch = epoch;
        this.clock = clock;
    }

    public void increaseClock() {
        this.clock++;
    }

    public boolean isMaster(String node) {
        return Objects.equals(this.node, node);
    }

    public int epoch() {
        return this.epoch;
    }

    public long clock() {
        return this.clock;
    }

    public void clock(long clock) {
        this.clock = clock;
    }

    public String node() {
        return this.node;
    }

    public String url() {
        return this.url;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ClusterRole)) {
            return false;
        }
        ClusterRole clusterRole = (ClusterRole) obj;
        return clock == clusterRole.clock &&
               epoch == clusterRole.epoch &&
               Objects.equals(node, clusterRole.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, clock, epoch);
    }

    @Override
    public String toString() {
        return "RoleStateData{" +
                "node='" + node + '\'' +
                ", clock=" + clock +
                ", epoch=" + epoch +
                '}';
    }
}
