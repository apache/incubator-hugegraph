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

package com.baidu.hugegraph.backend.store.rocksdb;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeDouble;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.util.Bytes;

public class RocksDBOptions extends OptionHolder {

    private RocksDBOptions() {
        super();
    }

    private static volatile RocksDBOptions instance;

    public static RocksDBOptions instance() {
        if (instance == null) {
            synchronized (RocksDBOptions.class) {
                if (instance == null) {
                    instance = new RocksDBOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

    public static final ConfigOption<String> DATA_PATH =
            new ConfigOption<>(
                    "rocksdb.data_path",
                    "The path for storing data of RocksDB.",
                    disallowEmpty(),
                    "rocksdb-data"
            );

    public static final ConfigOption<String> WAL_PATH =
            new ConfigOption<>(
                    "rocksdb.wal_path",
                    "The path for storing WAL of RocksDB.",
                    disallowEmpty(),
                    "rocksdb-data"
            );

    public static final ConfigOption<String> SST_PATH =
            new ConfigOption<>(
                    "rocksdb.sst_path",
                    "The path for ingesting SST file into RocksDB.",
                    null,
                    ""
            );

    // TODO: support ConfigOption<InfoLogLevel>
    public static final ConfigOption<String> LOG_LEVEL =
            new ConfigOption<>(
                    "rocksdb.log_level",
                    "The info log level of RocksDB.",
                    allowValues("DEBUG", "INFO", "WARN", "ERROR", "FATAL", "HEADER"),
                    "INFO"
            );

    public static final ConfigOption<Integer> NUM_LEVELS =
            new ConfigOption<>(
                    "rocksdb.num_levels",
                    "Set the number of levels for this database.",
                    rangeInt(1, Integer.MAX_VALUE),
                    7
            );

    public static final ConfigOption<String> COMPACTION_STYLE =
            new ConfigOption<>(
                    "rocksdb.compaction_style",
                    "Set compaction style for RocksDB: LEVEL/UNIVERSAL/FIFO.",
                    allowValues("LEVEL", "UNIVERSAL", "FIFO"),
                    "LEVEL"
            );

    public static final ConfigOption<Boolean> OPTIMIZE_MODE =
            new ConfigOption<>(
                    "rocksdb.optimize_mode",
                    "Optimize for heavy workloads and big datasets.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> BULKLOAD_MODE =
            new ConfigOption<>(
                    "rocksdb.bulkload_mode",
                    "Switch to the mode to bulk load data into RocksDB.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> COMPRESSION_TYPE =
            new ConfigOption<>(
                    "rocksdb.compression_type",
                    "The compression algorithm of RocksDB: snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    allowValues("snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    "snappy"
            );

    public static final ConfigOption<Integer> MAX_BG_COMPACTIONS =
            new ConfigOption<>(
                    "rocksdb.max_background_compactions",
                    "The maximum number of concurrent background compaction jobs.",
                    rangeInt(1, Integer.MAX_VALUE),
                    4
            );

    public static final ConfigOption<Integer> MAX_SUB_COMPACTIONS =
            new ConfigOption<>(
                    "rocksdb.max_subcompactions",
                    "The value represents the maximum number of threads per compaction job.",
                    rangeInt(1, Integer.MAX_VALUE),
                    4
            );

    public static final ConfigOption<Integer> MAX_BG_FLUSHES =
            new ConfigOption<>(
                    "rocksdb.max_background_flushes",
                    "The maximum number of concurrent background flush jobs.",
                    rangeInt(1, Integer.MAX_VALUE),
                    4
            );

    public static final ConfigOption<Integer> MAX_OPEN_FILES =
            new ConfigOption<>(
                    "rocksdb.max_open_files",
                    "The maximum number of open files that can be cached by RocksDB.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    -1
            );

    public static final ConfigOption<Long> MEMTABLE_SIZE =
            new ConfigOption<>(
                    "rocksdb.write_buffer_size",
                    "Amount of data in bytes to build up in memory.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    128 * Bytes.MB
            );

    public static final ConfigOption<Integer> MAX_MEMTABLES =
            new ConfigOption<>(
                    "rocksdb.max_write_buffer_number",
                    "The maximum number of write buffers that are built up in memory.",
                    rangeInt(1, Integer.MAX_VALUE),
                    6
            );

    public static final ConfigOption<Integer> MIN_MEMTABLES_TO_MERGE =
            new ConfigOption<>(
                    "rocksdb.min_write_buffer_number_to_merge",
                    "The minimum number of write buffers that will be merged together.",
                    rangeInt(1, Integer.MAX_VALUE),
                    2
            );

    public static final ConfigOption<Integer> MAX_MEMTABLES_TO_MAINTAIN =
            new ConfigOption<>(
                    "rocksdb.max_write_buffer_number_to_maintain",
                    "The total maximum number of write buffers to maintain in memory.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );

    public static final ConfigOption<Long> MAX_LEVEL1_BYTES =
            new ConfigOption<>(
                    "rocksdb.max_bytes_for_level_base",
                    "The upper-bound of the total size of level-1 files in bytes.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    512 * Bytes.MB
            );

    public static final ConfigOption<Double> MAX_LEVEL_BYTES_MULTIPLIER =
            new ConfigOption<>(
                    "rocksdb.max_bytes_for_level_multiplier",
                    "The ratio between the total size of level (L+1) files and " +
                    "the total size of level L files for all L",
                    rangeDouble(1.0, Double.MAX_VALUE),
                    10.0
            );

    public static final ConfigOption<Long> TARGET_FILE_SIZE_BASE =
            new ConfigOption<>(
                    "rocksdb.target_file_size_base",
                    "The target file size for compaction in bytes.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    64 * Bytes.MB
            );

    public static final ConfigOption<Integer> TARGET_FILE_SIZE_MULTIPLIER =
            new ConfigOption<>(
                    "rocksdb.target_file_size_multiplier",
                    "The size ratio between a level L file and a level (L+1) file.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1
            );

    public static final ConfigOption<Boolean> ALLOW_MMAP_WRITES =
            new ConfigOption<>(
                    "rocksdb.allow_mmap_writes",
                    "Allow the OS to mmap file for writing.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> ALLOW_MMAP_READS =
            new ConfigOption<>(
                    "rocksdb.allow_mmap_reads",
                    "Allow the OS to mmap file for reading sst tables.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> USE_DIRECT_READS =
            new ConfigOption<>(
                    "rocksdb.use_direct_reads",
                    "Enable the OS to use direct I/O for reading sst tables.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> USE_DIRECT_READS_WRITES_FC =
            new ConfigOption<>(
                    "rocksdb.use_direct_io_for_flush_and_compaction",
                    "Enable the OS to use direct reads and writes in flush and compaction.",
                    disallowEmpty(),
                    false
            );
}
