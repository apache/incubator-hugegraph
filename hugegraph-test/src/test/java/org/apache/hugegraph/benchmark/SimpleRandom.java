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

package org.apache.hugegraph.benchmark;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Fairly fast random numbers
 */
public final class SimpleRandom {

    private static final long MULTIPLIER = 0x5DEECE66DL;
    private static final long ADD_END = 0xBL;
    private static final long MASK = (1L << 48) - 1;
    private static final AtomicLong SEG = new AtomicLong(-715159705);
    private long seed;

    public SimpleRandom() {
        this.seed = System.nanoTime() + SEG.getAndAdd(129);
    }

    public int next() {
        long nextSeed = (this.seed * MULTIPLIER + ADD_END) & MASK;
        this.seed = nextSeed;
        return ((int) (nextSeed >>> 17)) & 0x7FFFFFFF;
    }
}
