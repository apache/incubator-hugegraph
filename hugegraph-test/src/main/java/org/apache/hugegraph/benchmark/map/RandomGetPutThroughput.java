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

package org.apache.hugegraph.benchmark.map;

import static org.apache.hugegraph.benchmark.BenchMarkConstants.OUTPUT_PATH;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.benchmark.SimpleRandom;
import org.apache.hugegraph.util.collection.IntMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
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
@Fork(3)
public class RandomGetPutThroughput {

    private static final int MAP_CAPACITY = 100000;
    private final ConcurrentHashMap<Integer, Integer> concurrentHashMapNonCap =
        new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, Integer> concurrentHashMap =
        new ConcurrentHashMap<>(MAP_CAPACITY);

    private final IntMap.IntMapBySegments intMapBySegments =
        new IntMap.IntMapBySegments(MAP_CAPACITY);

    @State(Scope.Thread)
    public static class ThreadState {
        private final SimpleRandom random = new SimpleRandom();

        int next() {
            return random.next();
        }
    }

    @Benchmark
    @Threads(8)
    public void randomGetPutByConcurrentHashMapWithNoneCap(ThreadState state) {
        int key = state.next() & (MAP_CAPACITY - 1);
        if (!concurrentHashMapNonCap.containsKey(key)) {
            concurrentHashMapNonCap.put(key, state.next());
        }
        concurrentHashMapNonCap.get(key);
    }

    @Benchmark
    @Threads(8)
    public void randomGetPutByConcurrentHashMapWithCap(ThreadState state) {
        int key = state.next() & (MAP_CAPACITY - 1);
        if (!concurrentHashMap.containsKey(key)) {
            concurrentHashMap.put(key, state.next());
        }
        concurrentHashMap.get(key);
    }

    @Benchmark
    @Threads(8)
    public void randomGetPutByIntMapBySegments(ThreadState state) {
        int key = state.next() & (MAP_CAPACITY - 1);
        if (!intMapBySegments.containsKey(key)) {
            intMapBySegments.put(key, state.next());
        }
        intMapBySegments.get(key);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(RandomGetPutThroughput.class.getSimpleName())
            .result(OUTPUT_PATH + "random_get_put_result.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
        new Runner(opt).run();
    }
}
