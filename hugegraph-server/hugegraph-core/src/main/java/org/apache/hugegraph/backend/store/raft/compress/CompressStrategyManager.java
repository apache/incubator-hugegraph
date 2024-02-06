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

package org.apache.hugegraph.backend.store.raft.compress;

import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;

public class CompressStrategyManager {

    private static byte DEFAULT_STRATEGY = 1;
    public static final byte SERIAL_STRATEGY = 1;
    public static final byte PARALLEL_STRATEGY = 2;
    public static final byte MAX_STRATEGY = 5;
    private static CompressStrategy[] compressStrategies = new CompressStrategy[MAX_STRATEGY];

    static {
        addCompressStrategy(SERIAL_STRATEGY, new SerialCompressStrategy());
    }

    private CompressStrategyManager() {
    }

    public static void addCompressStrategy(int index, CompressStrategy compressStrategy) {
        if (compressStrategies.length <= index) {
            CompressStrategy[] newCompressStrategies = new CompressStrategy[index + MAX_STRATEGY];
            System.arraycopy(compressStrategies, 0, newCompressStrategies, 0,
                             compressStrategies.length);
            compressStrategies = newCompressStrategies;
        }
        compressStrategies[index] = compressStrategy;
    }

    public static CompressStrategy getDefault() {
        return compressStrategies[DEFAULT_STRATEGY];
    }

    public static void init(final HugeConfig config) {
        if (!config.get(CoreOptions.RAFT_SNAPSHOT_PARALLEL_COMPRESS)) {
            return;
        }
        // add parallel compress strategy
        if (compressStrategies[PARALLEL_STRATEGY] == null) {
            CompressStrategy compressStrategy = new ParallelCompressStrategy(
                config.get(CoreOptions.RAFT_SNAPSHOT_COMPRESS_THREADS),
                config.get(CoreOptions.RAFT_SNAPSHOT_DECOMPRESS_THREADS));
            CompressStrategyManager.addCompressStrategy(
                CompressStrategyManager.PARALLEL_STRATEGY, compressStrategy);
            DEFAULT_STRATEGY = PARALLEL_STRATEGY;
        }
    }
}
