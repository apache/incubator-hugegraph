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

package org.apache.hugegraph.backend.store.jvector;

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;
import org.apache.hugegraph.util.Bytes;

/**
 * Vector backend options for JVector configuration
 */
public class VectorOptions extends OptionHolder {

    private VectorOptions() {
        super();
    }

    private static volatile VectorOptions instance;

    public static synchronized VectorOptions instance() {
        if (instance == null) {
            instance = new VectorOptions();
            instance.registerOptions();
        }
        return instance;
    }

    // Vector data storage path
    public static final ConfigOption<String> DATA_PATH =
            new ConfigOption<>(
                    "vector.data_path",
                    "The path for storing vector index data.",
                    disallowEmpty(),
                    "vector-data"
            );

    // Vector dimension
    public static final ConfigOption<Integer> VECTOR_DIMENSION =
            new ConfigOption<>(
                    "vector.dimension",
                    "The dimension of vectors.",
                    rangeInt(1, 65536),
                    128
            );

    // Vector index type (HNSW, IVF, etc.)
    public static final ConfigOption<String> VECTOR_INDEX_TYPE =
            new ConfigOption<>(
                    "vector.index_type",
                    "The type of vector index: HNSW, IVF, etc.",
                    allowValues("HNSW", "IVF", "FLAT"),
                    "HNSW"
            );

    // HNSW specific parameters
    public static final ConfigOption<Integer> HNSW_M =
            new ConfigOption<>(
                    "vector.hnsw.m",
                    "HNSW parameter M: number of bi-directional links created for every new element.",
                    rangeInt(4, 64),
                    16
            );

    public static final ConfigOption<Integer> HNSW_EF_CONSTRUCTION =
            new ConfigOption<>(
                    "vector.hnsw.ef_construction",
                    "HNSW parameter ef_construction: size of the dynamic candidate list.",
                    rangeInt(4, 2000),
                    200
            );

    public static final ConfigOption<Integer> HNSW_EF_SEARCH =
            new ConfigOption<>(
                    "vector.hnsw.ef_search",
                    "HNSW parameter ef_search: size of the dynamic candidate list for search.",
                    rangeInt(1, 2000),
                    50
            );

    // IVF specific parameters
    public static final ConfigOption<Integer> IVF_NLIST =
            new ConfigOption<>(
                    "vector.ivf.nlist",
                    "IVF parameter nlist: number of clusters.",
                    rangeInt(1, 100000),
                    1000
            );

    public static final ConfigOption<Integer> IVF_NPROBE =
            new ConfigOption<>(
                    "vector.ivf.nprobe",
                    "IVF parameter nprobe: number of clusters to search.",
                    rangeInt(1, 1000),
                    10
            );

    // Performance parameters
    public static final ConfigOption<Integer> MAX_CONNECTIONS =
            new ConfigOption<>(
                    "vector.max_connections",
                    "Maximum number of connections per node in the graph.",
                    rangeInt(4, 128),
                    32
            );

    public static final ConfigOption<Integer> SEARCH_K =
            new ConfigOption<>(
                    "vector.search_k",
                    "Number of nearest neighbors to search for.",
                    rangeInt(1, 10000),
                    10
            );

    // Memory management
    public static final ConfigOption<Long> MAX_MEMORY_USAGE =
            new ConfigOption<>(
                    "vector.max_memory_usage",
                    "Maximum memory usage in bytes for vector index.",
                    rangeInt(0L, Long.MAX_VALUE),
                    1024L * 1024L * 1024L // 1GB
            );

    // Persistence options
    public static final ConfigOption<Boolean> PERSIST_INDEX =
            new ConfigOption<>(
                    "vector.persist_index",
                    "Whether to persist the vector index to disk.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<String> INDEX_FILE_PATH =
            new ConfigOption<>(
                    "vector.index_file_path",
                    "Path to the persisted vector index file.",
                    disallowEmpty(),
                    "vector-data/index.bin"
            );

    // Threading options
    public static final ConfigOption<Integer> NUM_THREADS =
            new ConfigOption<>(
                    "vector.num_threads",
                    "Number of threads to use for vector operations.",
                    rangeInt(1, 64),
                    4
            );

    // Distance metric
    public static final ConfigOption<String> DISTANCE_METRIC =
            new ConfigOption<>(
                    "vector.distance_metric",
                    "Distance metric for vector similarity: EUCLIDEAN, COSINE, DOT_PRODUCT.",
                    allowValues("EUCLIDEAN", "COSINE", "DOT_PRODUCT"),
                    "EUCLIDEAN"
            );

    // Batch size for operations
    public static final ConfigOption<Integer> BATCH_SIZE =
            new ConfigOption<>(
                    "vector.batch_size",
                    "Batch size for vector operations.",
                    rangeInt(1, 10000),
                    100
            );

    // Cache options
    public static final ConfigOption<Long> CACHE_SIZE =
            new ConfigOption<>(
                    "vector.cache_size",
                    "Cache size in bytes for vector index.",
                    rangeInt(0L, Long.MAX_VALUE),
                    128L * Bytes.MB
            );

    public static final ConfigOption<Boolean> ENABLE_CACHE =
            new ConfigOption<>(
                    "vector.enable_cache",
                    "Whether to enable caching for vector operations.",
                    disallowEmpty(),
                    true
            );
}
