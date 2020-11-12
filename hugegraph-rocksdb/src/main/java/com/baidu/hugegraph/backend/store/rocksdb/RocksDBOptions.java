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
import static com.baidu.hugegraph.config.OptionChecker.inValues;
import static com.baidu.hugegraph.config.OptionChecker.rangeDouble;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

import com.baidu.hugegraph.config.ConfigConvOption;
import com.baidu.hugegraph.config.ConfigListConvOption;
import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.util.Bytes;
import com.google.common.collect.ImmutableList;

public class RocksDBOptions extends OptionHolder {

    private RocksDBOptions() {
        super();
    }

    private static volatile RocksDBOptions instance;

    public static synchronized RocksDBOptions instance() {
        if (instance == null) {
            instance = new RocksDBOptions();
            instance.registerOptions();
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

    public static final ConfigListOption<String> DATA_DISKS =
            new ConfigListOption<>(
                    "rocksdb.data_disks",
                    false,
                    "The optimized disks for storing data of RocksDB. " +
                    "The format of each element: `STORE/TABLE: /path/to/disk`." +
                    "Allowed keys are [graph/vertex, graph/edge_out, graph/edge_in, " +
                    "graph/secondary_index, graph/range_index]",
                    null,
                    String.class,
                    ImmutableList.of()
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

    public static final ConfigConvOption<String, CompactionStyle> COMPACTION_STYLE =
            new ConfigConvOption<>(
                    "rocksdb.compaction_style",
                    "Set compaction style for RocksDB: LEVEL/UNIVERSAL/FIFO.",
                    allowValues("LEVEL", "UNIVERSAL", "FIFO"),
                    CompactionStyle::valueOf,
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

    public static final ConfigListConvOption<String, CompressionType> LEVELS_COMPRESSIONS =
            new ConfigListConvOption<>(
                    "rocksdb.compression_per_level",
                    "The compression algorithms for different levels of RocksDB, " +
                    "allowed values are none/snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    inValues("none", "snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    CompressionType::getCompressionType,
                    "none", "none", "snappy", "snappy", "snappy", "snappy", "snappy"
            );

    public static final ConfigConvOption<String, CompressionType> BOTTOMMOST_COMPRESSION =
            new ConfigConvOption<>(
                    "rocksdb.bottommost_compression",
                    "The compression algorithm for the bottommost level of RocksDB, " +
                    "allowed values are none/snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    allowValues("none", "snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    CompressionType::getCompressionType,
                    "none"
            );

    public static final ConfigConvOption<String, CompressionType> COMPRESSION =
            new ConfigConvOption<>(
                    "rocksdb.compression",
                    "The compression algorithm for compressing blocks of RocksDB, " +
                    "allowed values are none/snappy/z/bzip2/lz4/lz4hc/xpress/zstd.",
                    allowValues("none", "snappy", "z", "bzip2", "lz4", "lz4hc", "xpress", "zstd"),
                    CompressionType::getCompressionType,
                    "snappy"
            );

    public static final ConfigOption<Integer> MAX_BG_JOBS =
            new ConfigOption<>(
                    "rocksdb.max_background_jobs",
                    "Maximum number of concurrent background jobs, including flushes and compactions.",
                    rangeInt(1, Integer.MAX_VALUE),
                    8
            );

    public static final ConfigOption<Integer> MAX_SUB_COMPACTIONS =
            new ConfigOption<>(
                    "rocksdb.max_subcompactions",
                    "The value represents the maximum number of threads per compaction job.",
                    rangeInt(1, Integer.MAX_VALUE),
                    4
            );

    public static final ConfigOption<Long> DELAYED_WRITE_RATE =
            new ConfigOption<>(
                    "rocksdb.delayed_write_rate",
                    "The rate limit in bytes/s of user write requests " +
                    "when need to slow down if the compaction gets behind.",
                    rangeInt(1L, Long.MAX_VALUE),
                    16L * Bytes.MB
            );

    public static final ConfigOption<Integer> MAX_OPEN_FILES =
            new ConfigOption<>(
                    "rocksdb.max_open_files",
                    "The maximum number of open files that can be cached by RocksDB, " +
                    "-1 means no limit.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    -1
            );

    public static final ConfigOption<Long> MAX_MANIFEST_FILE_SIZE =
            new ConfigOption<>(
                    "rocksdb.max_manifest_file_size",
                    "The max size of manifest file in bytes.",
                    rangeInt(1L, Long.MAX_VALUE),
                    100L * Bytes.MB
            );

    public static final ConfigOption<Boolean> SKIP_STATS_UPDATE_ON_DB_OPEN =
            new ConfigOption<>(
                    "rocksdb.skip_stats_update_on_db_open",
                    "Whether to skip statistics update when opening the database, " +
                    "setting this flag true allows us to not update statistics.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> MAX_FILE_OPENING_THREADS =
            new ConfigOption<>(
                    "rocksdb.max_file_opening_threads",
                    "The max number of threads used to open files.",
                    rangeInt(1, Integer.MAX_VALUE),
                    16
            );

    public static final ConfigOption<Long> MAX_TOTAL_WAL_SIZE =
            new ConfigOption<>(
                    "rocksdb.max_total_wal_size",
                    "Total size of WAL files in bytes. Once WALs exceed this size, " +
                    "we will start forcing the flush of column families related, " +
                    "0 means no limit.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> DB_MEMTABLE_SIZE =
            new ConfigOption<>(
                    "rocksdb.db_write_buffer_size",
                    "Total size of write buffers in bytes across all column families, " +
                    "0 means no limit.",
                    rangeInt(0L, Long.MAX_VALUE),
                    0L
            );

    public static final ConfigOption<Long> DELETE_OBSOLETE_FILE_PERIOD =
            new ConfigOption<>(
                    "rocksdb.delete_obsolete_files_period",
                    "The periodicity in seconds when obsolete files get deleted, " +
                    "0 means always do full purge.",
                    rangeInt(0L, Long.MAX_VALUE),
                    6L * 60 * 60
            );

    public static final ConfigOption<Long> MEMTABLE_SIZE =
            new ConfigOption<>(
                    "rocksdb.write_buffer_size",
                    "Amount of data in bytes to build up in memory.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    128L * Bytes.MB
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
                    512L * Bytes.MB
            );

    public static final ConfigOption<Double> MAX_LEVEL_BYTES_MULTIPLIER =
            new ConfigOption<>(
                    "rocksdb.max_bytes_for_level_multiplier",
                    "The ratio between the total size of level (L+1) files and " +
                    "the total size of level L files for all L.",
                    rangeDouble(1.0, Double.MAX_VALUE),
                    10.0
            );

    public static final ConfigOption<Long> TARGET_FILE_SIZE_BASE =
            new ConfigOption<>(
                    "rocksdb.target_file_size_base",
                    "The target file size for compaction in bytes.",
                    rangeInt(Bytes.MB, Long.MAX_VALUE),
                    64L * Bytes.MB
            );

    public static final ConfigOption<Integer> TARGET_FILE_SIZE_MULTIPLIER =
            new ConfigOption<>(
                    "rocksdb.target_file_size_multiplier",
                    "The size ratio between a level L file and a level (L+1) file.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1
            );

    public static final ConfigOption<Integer> LEVEL0_COMPACTION_TRIGGER =
            new ConfigOption<>(
                    "rocksdb.level0_file_num_compaction_trigger",
                    "Number of files to trigger level-0 compaction.",
                    rangeInt(0, Integer.MAX_VALUE),
                    2
            );

    public static final ConfigOption<Integer> LEVEL0_SLOWDOWN_WRITES_TRIGGER =
            new ConfigOption<>(
                    "rocksdb.level0_slowdown_writes_trigger",
                    "Soft limit on number of level-0 files for slowing down writes.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    20
            );

    public static final ConfigOption<Integer> LEVEL0_STOP_WRITES_TRIGGER =
            new ConfigOption<>(
                    "rocksdb.level0_stop_writes_trigger",
                    "Hard limit on number of level-0 files for stopping writes.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    36
            );

    public static final ConfigOption<Long> SOFT_PENDING_COMPACTION_LIMIT =
            new ConfigOption<>(
                    "rocksdb.soft_pending_compaction_bytes_limit",
                    "The soft limit to impose on pending compaction in bytes.",
                    rangeInt(Bytes.GB, Long.MAX_VALUE),
                    64L * Bytes.GB
            );

    public static final ConfigOption<Long> HARD_PENDING_COMPACTION_LIMIT =
            new ConfigOption<>(
                    "rocksdb.hard_pending_compaction_bytes_limit",
                    "The hard limit to impose on pending compaction in bytes.",
                    rangeInt(Bytes.GB, Long.MAX_VALUE),
                    256L * Bytes.GB
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
                    "Enable the OS to use direct read/writes in flush and compaction.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Long> BLOCK_CACHE_CAPACITY =
            new ConfigOption<>(
                    "rocksdb.block_cache_capacity",
                    "The amount of block cache in bytes that will be used by RocksDB, " +
                    "0 means no block cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    8L * Bytes.MB
            );

    public static final ConfigOption<Boolean> PIN_L0_FILTER_AND_INDEX_IN_CACHE =
            new ConfigOption<>(
                    "rocksdb.pin_l0_filter_and_index_blocks_in_cache",
                    "Indicating if we'd put index/filter blocks to the block cache.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> PUT_FILTER_AND_INDEX_IN_CACHE =
            new ConfigOption<>(
                    "rocksdb.cache_index_and_filter_blocks",
                    "Indicating if we'd put index/filter blocks to the block cache.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Integer> BLOOM_FILTER_BITS_PER_KEY =
            new ConfigOption<>(
                    "rocksdb.bloom_filter_bits_per_key",
                    "The bits per key in bloom filter, a good value is 10, " +
                    "which yields a filter with ~ 1% false positive rate, " +
                    "-1 means no bloom filter.",
                    rangeInt(-1, Integer.MAX_VALUE),
                    -1
            );

    public static final ConfigOption<Boolean> BLOOM_FILTER_MODE =
            new ConfigOption<>(
                    "rocksdb.bloom_filter_block_based_mode",
                    "Use block based filter rather than full filter.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> BLOOM_FILTER_WHOLE_KEY =
            new ConfigOption<>(
                    "rocksdb.bloom_filter_whole_key_filtering",
                    "True if place whole keys in the bloom filter, " +
                    "else place the prefix of keys.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> BLOOM_FILTERS_SKIP_LAST_LEVEL =
            new ConfigOption<>(
                    "rocksdb.optimize_filters_for_hits",
                    "This flag allows us to not store filters for the last level.",
                    disallowEmpty(),
                    false
            );
}
