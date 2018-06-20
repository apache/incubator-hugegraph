/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.TokenFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.util.Bytes;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableMap;

/**
 * CassandraShard is used for cassandra scanning operations.
 * Each shard represents a range of tokens for a node.
 * Reading data from a given shard does not cross multiple nodes.
 * Refer to AbstractColumnFamilyInputFormat from https://github.com/2013Commons/hive-cassandra/
 */
public class CassandraShard {

    // The minimal shard size should >= 1M to prevent too many number of shards
    private static final int MIN_SHARD_SIZE = (int) Bytes.MB;

    private CassandraSessionPool.Session session;
    private String keyspace;
    private String table;

    private IPartitioner partitioner;

    public CassandraShard(CassandraSessionPool.Session session,
                          String keyspace, String table) {
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;

        this.partitioner = new Murmur3Partitioner();
    }

    /**
     * Get splits of a table
     * @param splitPartitions: expected partitions count per split
     * @param splitSize: expected size(bytes) per split,
     *        splitPartitions will be ignored if splitSize is passed
     * @return a list of Shard
     */
    public List<Shard> getSplits(long splitPartitions, long splitSize) {
        // Canonical ranges, split into pieces, fetch the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(
                0, 128, 60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        List<Shard> splits = new ArrayList<>();
        try {
            List<Future<List<Shard>>> futures = new ArrayList<>();

            // Canonical ranges and nodes holding replicas
            Map<TokenRange, Set<Host>> masterRangeNodes = getRangeMap();

            for (TokenRange range : masterRangeNodes.keySet()) {
                /*
                 * For each token range, pick a live owner and ask it to
                 * compute bite-sized splits.
                 */
                futures.add(executor.submit(new SplitCallable(
                            range, splitPartitions, splitSize)));
            }

            // Wait until we have all the results back
            for (Future<List<Shard>> future : futures) {
                try {
                    splits.addAll(future.get());
                } catch (Exception e) {
                    throw new BackendException("Can't get cassandra shards", e);
                }
            }
            assert splits.size() > masterRangeNodes.size();
        } finally {
            executor.shutdownNow();
        }

        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    /**
     * Get splits of a table in specified range
     * NOTE: maybe we don't need this method
     * @param start, end: the specified range
     * @param splitPartitions: expected partitions count per split
     * @param splitSize: expected size(bytes) per split,
     *        splitPartitions will be ignored if splitSize is passed
     * @return a list of Shard
     */

    public List<Shard> getSplits(String start, String end,
                                 int splitPartitions, int splitSize) {

        ExecutorService executor = new ThreadPoolExecutor(
                0, 128, 60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        List<Shard> splits = new ArrayList<>();
        try {
            List<Future<List<Shard>>> futures = new ArrayList<>();
            TokenFactory tokenFactory = this.partitioner.getTokenFactory();
            TokenRange tokenRange = rangeToTokenRange(new Range<>(
                                    tokenFactory.fromString(start),
                                    tokenFactory.fromString(end)));

            // Canonical ranges and nodes holding replicas
            Map<TokenRange, Set<Host>> masterRangeNodes = getRangeMap();

            for (TokenRange range : masterRangeNodes.keySet()) {
                for (TokenRange r : range.intersectWith(tokenRange)) {
                    // For each tokenRange, pick a live owner and ask it
                    // to compute bite-sized splits
                    futures.add(executor.submit(new SplitCallable(
                                r, splitPartitions, splitSize)));
                }
            }

            // Wait until we have all the results back
            for (Future<List<Shard>> future : futures) {
                try {
                    splits.addAll(future.get());
                } catch (Exception e) {
                    throw new BackendException("Can't get cassandra shards", e);
                }
            }
            assert splits.size() >= masterRangeNodes.size();
        } finally {
            executor.shutdownNow();
        }

        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    private boolean isPartitionerOpp() {
        return this.partitioner instanceof OrderPreservingPartitioner ||
               this.partitioner instanceof ByteOrderedPartitioner;
    }

    private TokenRange rangeToTokenRange(Range<Token> range) {
        TokenFactory tokenFactory = this.partitioner.getTokenFactory();
        Metadata metadata = this.session.metadata();
        return metadata.newTokenRange(
                        metadata.newToken(tokenFactory.toString(range.left)),
                        metadata.newToken(tokenFactory.toString(range.right)));
    }

    private Map<TokenRange, Long> getSubSplits(TokenRange tokenRange,
                                               long splitPartitions,
                                               long splitSize) {
        try {
            return describeSplits(this.session, this.keyspace, this.table,
                                  splitPartitions, splitSize, tokenRange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<TokenRange, Set<Host>> getRangeMap() {
        Metadata metadata = this.session.metadata();
        return metadata.getTokenRanges().stream().collect(Collectors.toMap(
                p -> p,
                p -> metadata.getReplicas('"' + this.keyspace + '"', p)));
    }

    private static Map<TokenRange, Long> describeSplits(
            CassandraSessionPool.Session session,
            String keyspace,
            String table,
            long splitPartitions,
            long splitSize,
            TokenRange tokenRange) {

        String query = String.format(
                "SELECT mean_partition_size, partitions_count FROM %s.%s " +
                "WHERE keyspace_name = ? AND table_name = ? AND " +
                "range_start = ? AND range_end = ?",
                SchemaConstants.SYSTEM_KEYSPACE_NAME,
                SystemKeyspace.SIZE_ESTIMATES);

        ResultSet resultSet = session.execute(query, keyspace, table,
                                              tokenRange.getStart().toString(),
                                              tokenRange.getEnd().toString());
        Row row = resultSet.one();

        long meanPartitionSize = 0L;
        long partitionsCount = 0L;
        long splitCount = 0L;

        if (row != null) {
            meanPartitionSize = row.getLong("mean_partition_size");
            partitionsCount = row.getLong("partitions_count");
            assert splitSize <= 0 || splitSize >= MIN_SHARD_SIZE;
            splitCount = splitSize > 0 ?
                         (meanPartitionSize * partitionsCount / splitSize) :
                         (partitionsCount / splitPartitions);
        }

        /*
         * If we have no data on this split or the size estimate is 0,
         * return the full split i.e., do not sub-split
         * Assume smallest granularity of partition count available from
         * CASSANDRA-7688.
         */
        if (splitCount == 0) {
            return ImmutableMap.of(tokenRange, (long) 128);
        }

        List<TokenRange> ranges = tokenRange.splitEvenly((int) splitCount);
        Map<TokenRange, Long> rangesWithLength = new HashMap<>();
        for (TokenRange range : ranges) {
            // Add a sub-range (with its partitions count per sub-range)
            rangesWithLength.put(range, partitionsCount / splitCount);
        }
        return rangesWithLength;
    }

    /**
     * Gets a token tokenRange and splits it up according to the suggested size
     * into input splits that Hugegraph can use.
     */
    class SplitCallable implements Callable<List<Shard>> {

        private final TokenRange tokenRange;
        private final long splitPartitions;
        private final long splitSize;

        public SplitCallable(TokenRange tokenRange,
                             long splitPartitions, long splitSize) {
            if (splitSize <= 0 && splitPartitions <= 0) {
                throw new IllegalArgumentException(String.format(
                          "The split-partitions must be > 0, but got %s",
                          splitPartitions));
            }

            if (splitSize > 0 && splitSize < MIN_SHARD_SIZE) {
                // splitSize should be at least 1M if passed
                throw new IllegalArgumentException(String.format(
                          "The split-size must be >= %s bytes, but got %s",
                          MIN_SHARD_SIZE, splitSize));
            }

            this.tokenRange = tokenRange;
            this.splitPartitions = splitPartitions;
            this.splitSize = splitSize;
        }

        @Override
        public List<Shard> call() throws Exception {
            ArrayList<Shard> splits = new ArrayList<>();

            Map<TokenRange, Long> subSplits = getSubSplits(
                    this.tokenRange,
                    this.splitPartitions,
                    this.splitSize);
            for (Map.Entry<TokenRange, Long> entry : subSplits.entrySet()) {
                List<TokenRange> ranges = entry.getKey().unwrap();
                for (TokenRange subrange : ranges) {
                    String start = !isPartitionerOpp() ?
                                   subrange.getStart().toString() :
                                   subrange.getStart().toString().substring(2);
                    String end = !isPartitionerOpp() ?
                                 subrange.getEnd().toString() :
                                 subrange.getEnd().toString().substring(2);
                    long length = entry.getValue();
                    splits.add(new Shard(start, end, length));
                }
            }
            return splits;
        }
    }
}
