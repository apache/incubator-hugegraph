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

package org.apache.hugegraph.store.business.itrv2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.itrv2.io.SortShuffleSerializer;
import org.apache.hugegraph.store.util.SortShuffle;

/**
 * 目前应用：2个及以上的iterator去重 (大数据量）
 * 痛点： iterator 内部可能有重复的， 怎么处理 ？？
 */
public class IntersectionFilterIterator implements ScanIterator {

    private static final Integer MAX_SIZE = 100000;
    protected Map<Object, Integer> map;
    private ScanIterator iterator;
    private IntersectionWrapper wrapper;
    private boolean processed = false;
    private Iterator innerIterator;
    private SortShuffle<RocksDBSession.BackendColumn> sortShuffle;

    private int size = -1;

    @Deprecated
    public IntersectionFilterIterator(ScanIterator iterator, IntersectionWrapper<?> wrapper) {
        this.iterator = iterator;
        this.wrapper = wrapper;
        this.map = new HashMap<>();
    }

    /**
     * iterator中取交集（可以是multi list iterator
     * 问题：对于multi list iterator，无法做到每个都存在，需要外部去重。但是保证总数
     *
     * @param iterator 待遍历的iterator
     * @param wrapper  bitmap, 不在bitmap中的，丢弃
     * @param size     the element count in the iterator by filtering
     */
    public IntersectionFilterIterator(ScanIterator iterator, IntersectionWrapper<?> wrapper,
                                      int size) {
        this(iterator, wrapper);
        this.size = size;
    }

    @Override
    public boolean hasNext() {
        if (!processed) {
            try {
                dedup();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            processed = true;
        }

        return innerIterator.hasNext();
    }

    // TODO: 优化序列化器
    private void saveElements() throws IOException, ClassNotFoundException {
        for (var entry : this.map.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                sortShuffle.append((RocksDBSession.BackendColumn) entry.getKey());
            }
        }

        this.map.clear();
    }

    /**
     * todo: 如果一个iterator中存在重复的，目前是无解。 去重的代价太高了。
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void dedup() throws IOException, ClassNotFoundException {
        while (this.iterator.hasNext()) {
            var object = this.iterator.next();
            if (wrapper.contains(object)) {
                this.map.put(object, map.getOrDefault(object, 0) + 1);
                if (this.map.size() >= MAX_SIZE) {
                    if (this.sortShuffle == null) {
                        this.sortShuffle =
                                new SortShuffle<>((o1, o2) -> Arrays.compare(o1.name, o2.name),
                                                  SortShuffleSerializer.ofBackendColumnSerializer());
                    }
                    saveElements();
                }
            }
        }

        // last batch
        if (this.sortShuffle != null) {
            saveElements();
            this.sortShuffle.finish();
        }

        if (this.sortShuffle == null) {
            // map 没填满
            this.innerIterator =
                    new MapValueFilterIterator<>(this.map, x -> x == size || size == -1 && x > 1);
        } else {
            // 需要读取文件
            var fileIterator =
                    (Iterator<RocksDBSession.BackendColumn>) this.sortShuffle.getIterator();
            this.innerIterator = new ReduceIterator<>(fileIterator,
                                                      (o1, o2) -> Arrays.compare(o1.name, o2.name),
                                                      this.size);
        }
    }

    @Override
    public boolean isValid() {
        if (this.processed) {
            return false;
        }
        return iterator.isValid();
    }

    @Override
    public <T> T next() {
        return (T) this.innerIterator.next();
    }

    @Override
    public void close() {
        this.iterator.close();
        this.map.clear();
    }

    @Override
    public long count() {
        return this.iterator.count();
    }

    @Override
    public byte[] position() {
        return this.iterator.position();
    }

    @Override
    public void seek(byte[] position) {
        this.iterator.seek(position);
    }

    /**
     * 只保留有重复元素的
     *
     * @param <E>
     */
    public static class ReduceIterator<E> implements Iterator<E> {

        private E prev = null;

        private E current = null;

        private E data = null;

        private int count = 0;

        private Iterator<E> iterator;

        private Comparator<E> comparator;

        private int adjacent;

        public ReduceIterator(Iterator<E> iterator, Comparator<E> comparator, int adjacent) {
            this.count = 0;
            this.iterator = iterator;
            this.comparator = comparator;
            this.adjacent = adjacent;
        }

        /**
         * 连续重复结果消除. 当prev == current的时候，记录data
         * 当不等的时候，放回之前的data.
         * 注意最后的结果，可能重复
         */
        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                if (prev == null) {
                    prev = iterator.next();
                    continue;
                }

                current = iterator.next();
                if (comparator.compare(prev, current) == 0) {
                    data = current;
                    count += 1;
                } else {
                    // count starts from 0, so the size is count + 1
                    if (count > 0 && this.adjacent == -1 || count + 1 == this.adjacent) {
                        count = 0;
                        prev = current;
                        return true;
                    } else {
                        count = 0;
                        prev = current;
                    }
                }
            }

            // 最后一个结果
            if (count > 0) {
                count = 0;
                return true;
            }

            return false;
        }

        @Override
        public E next() {
            return data;
        }
    }

}
