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

import java.util.Set;

import org.apache.hugegraph.rocksdb.access.ScanIterator;

import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;

/**
 * 针对一个iterator做去重，前SET_MAX_SIZE做精确去重，后面的直接返回
 *
 * @param <T>
 */
public class MapLimitIterator<T> implements ScanIterator {

    private static final Integer SET_MAX_SIZE = 100000;
    private ScanIterator iterator;
    private Set<T> set;
    private T current = null;

    public MapLimitIterator(ScanIterator iterator) {
        this.iterator = iterator;
        set = new ConcurrentHashSet<>();
    }

    /**
     * {@inheritDoc}
     * 返回下一个元素是否存在。
     * 检查集合中是否还有下一个可用的元素，如果有则返回true，否则返回false。
     * 如果当前元素为空或者已在集合中，将跳过该元素继续检查下一个元素。
     * 在检查完所有符合条件的元素后，再次调用hasNext方法会重新检查一遍元素，
     * 如果满足条件（即不为null且未包含在集合中），则将当前元素添加到集合中并返回true。
     * 当集合中已经包含SET_MAX_SIZE个元素时，将不会再添加任何新元素，并且返回false。
     *
     * @return 下一个元素是否存在
     */
    @Override
    public boolean hasNext() {
        current = null;
        while (iterator.hasNext()) {
            var tmp = (T) iterator.next();
            if (tmp != null && !set.contains(tmp)) {
                current = tmp;
                break;
            }
        }

        // 控制set的大小
        if (current != null && set.size() <= SET_MAX_SIZE) {
            set.add(current);
        }

        return current != null;
    }

    /**
     * {@inheritDoc}
     * 返回当前对象。
     *
     * @return 当前对象的类型为T1的引用。
     */
    @Override
    public <T1> T1 next() {
        return (T1) current;
    }

    /**
     * 返回当前迭代器是否有效。
     *
     * @return 当前迭代器是否有效，即是否还有下一个元素。
     */
    @Override
    public boolean isValid() {
        return iterator.isValid();
    }

    /**
     * 返回集合的数量。
     *
     * @return 集合的数量。
     */
    @Override
    public long count() {
        return iterator.count();
    }

    /**
     * 返回当前迭代器的位置。
     *
     * @return 当前迭代器的位置。
     */
    @Override
    public byte[] position() {
        return iterator.position();
    }

    /**
     * {@inheritDoc}
     * 将文件指针移动到指定的位置。
     *
     * @param position 指定的字节数组，表示要移动到的位置。
     */
    @Override
    public void seek(byte[] position) {
        iterator.seek(position);
    }

    /**
     * 关闭迭代器。
     */
    @Override
    public void close() {
        iterator.close();
        this.set.clear();
    }
}
