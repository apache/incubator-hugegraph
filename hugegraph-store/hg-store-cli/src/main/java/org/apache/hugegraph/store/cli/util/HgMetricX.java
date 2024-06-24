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

package org.apache.hugegraph.store.cli.util;

/**
 * 2022/1/29
 */
public class HgMetricX {

    private long start;
    private long end;

    private long waitStart = System.currentTimeMillis();
    private long waitTotal;

    private HgMetricX(long start) {
        this.start = start;
    }

    public static HgMetricX ofStart() {
        return new HgMetricX(System.currentTimeMillis());
    }

    public long start() {
        return this.start = System.currentTimeMillis();
    }

    public long end() {
        return this.end = System.currentTimeMillis();
    }

    public long past() {
        return this.end - this.start;
    }

    public long getWaitTotal() {
        return this.waitTotal;
    }

    public void startWait() {
        this.waitStart = System.currentTimeMillis();
    }

    public void appendWait() {
        this.waitTotal += System.currentTimeMillis() - waitStart;
    }
}
