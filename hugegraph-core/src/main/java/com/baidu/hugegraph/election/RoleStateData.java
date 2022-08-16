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

package com.baidu.hugegraph.election;

import java.util.Objects;

public class RoleStateData {

    private String node;
    private long count;
    private int epoch;

    public RoleStateData(String node, int epoch) {
        this(node, epoch, 1);
    }

    public RoleStateData(String node, int epoch, long count) {
        this.node = node;
        this.epoch = epoch;
        this.count = count;
    }

    public void increaseCount() {
        this.count++;
    }

    public boolean isMaster(String node) {
        return Objects.equals(this.node, node);
    }

    public int epoch() {
        return this.epoch;
    }

    public long count() {
        return this.count;
    }

    public void count(long count) {
        this.count = count;
    }

    public String node() {
        return this.node;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RoleStateData)) {
            return false;
        }
        RoleStateData metaData = (RoleStateData) o;
        return count == metaData.count &&
               epoch == metaData.epoch &&
               Objects.equals(node, metaData.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, count, epoch);
    }

    @Override
    public String toString() {
        return "RoleStateData{" +
                "node='" + node + '\'' +
                ", count=" + count +
                ", epoch=" + epoch +
                '}';
    }
}
