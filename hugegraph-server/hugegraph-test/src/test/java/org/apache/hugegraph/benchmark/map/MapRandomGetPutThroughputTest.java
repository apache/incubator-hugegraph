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

package org.apache.hugegraph.benchmark.map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.benchmark.BenchmarkConstants;
import org.apache.hugegraph.benchmark.SimpleRandom;
import org.apache.hugegraph.util.collection.IntMap;
import org.apache.hugegraph.util.collection.IntMapByDynamicHash;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(2)
public class MapRandomGetPutThroughputTest {

    @Param(value = {"1000", "10000", "100000", "1000000"})
    private int MAP_CAPACITY;

    private ConcurrentHashMap<Integer, Integer> concurrentHashMapWithoutCap;

    private ConcurrentHashMap<Integer, Integer> concurrentHashMapWithCap;

    private IntMap intMapBySegmentsWithCap;

    private IntMap intMapByDynamicHashWithoutCap;

    private IntMap intMapByDynamicHashWithCap;

    private static final int THREAD_COUNT = 8;

    private static final String OUTPUT_FILE_NAME = "map_random_get_put_result.json";

    @Setup(Level.Trial)
    public void prepareMap() {
        this.concurrentHashMapWithoutCap = new ConcurrentHashMap<>();
        this.concurrentHashMapWithCap = new ConcurrentHashMap<>(MAP_CAPACITY);
        this.intMapBySegmentsWithCap = new IntMap.IntMapBySegments(MAP_CAPACITY);
        this.intMapByDynamicHashWithoutCap = new IntMapByDynamicHash();
        this.intMapByDynamicHashWithCap = new IntMapByDynamicHash(MAP_CAPACITY);
    }

    /**
     * The instantiated @State annotation only supports public classes.
     */
    @State(Scope.Thread)
    public static class ThreadState {

        private final SimpleRandom random = new SimpleRandom();

        int next() {
            return random.next();
        }
    }

    @Benchmark
    @Threads(THREAD_COUNT)
    public void randomGetPutOfConcurrentHashMapWithNoneInitCap(ThreadState state) {
        int key = state.next();
        if (!this.concurrentHashMapWithoutCap.containsKey(key)) {
            this.concurrentHashMapWithoutCap.put(key, state.next());
        }
        this.concurrentHashMapWithoutCap.get(key);
    }

    @Benchmark
    @Threads(THREAD_COUNT)
    public void randomGetPutOfConcurrentHashMapWithInitCap(ThreadState state) {
        int key = state.next() & (MAP_CAPACITY - 1);
        if (!this.concurrentHashMapWithCap.containsKey(key)) {
            this.concurrentHashMapWithCap.put(key, state.next());
        }
        this.concurrentHashMapWithCap.get(key);
    }

    @Benchmark
    @Threads(THREAD_COUNT)
    public void randomGetPutOfIntMapBySegmentsWithInitCap(ThreadState state) {
        int key = state.next() & (MAP_CAPACITY - 1);
        if (!this.intMapBySegmentsWithCap.containsKey(key)) {
            this.intMapBySegmentsWithCap.put(key, state.next());
        }
        this.intMapBySegmentsWithCap.get(key);
    }

    @Benchmark
    @Threads(THREAD_COUNT)
    public void randomGetPutOfIntMapByDynamicHashWithNoneCap(ThreadState state) {
        int key = state.next();
        if (!this.intMapByDynamicHashWithoutCap.containsKey(key)) {
            this.intMapByDynamicHashWithoutCap.put(key, state.next());
        }
        this.intMapByDynamicHashWithoutCap.get(key);
    }

    @Benchmark
    @Threads(THREAD_COUNT)
    public void randomGetPutOfIntMapByDynamicHashWithInitCap(ThreadState state) {
        int key = state.next() & (MAP_CAPACITY - 1);
        if (!this.intMapByDynamicHashWithCap.containsKey(key)) {
            this.intMapByDynamicHashWithCap.put(key, state.next());
        }
        this.intMapByDynamicHashWithCap.get(key);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MapRandomGetPutThroughputTest.class.getSimpleName())
                .result(BenchmarkConstants.OUTPUT_PATH + OUTPUT_FILE_NAME)
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
