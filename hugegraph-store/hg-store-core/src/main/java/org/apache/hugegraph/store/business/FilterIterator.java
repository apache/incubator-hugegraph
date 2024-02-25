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

package org.apache.hugegraph.store.business;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.structure.HugeElement;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FilterIterator<T extends BackendColumn> extends
                                                     AbstractSelectIterator
        implements ScanIterator {

    private final ConditionQuery query;
    T current = null;

    public FilterIterator(ScanIterator iterator, ConditionQuery query) {
        super();
        this.iterator = iterator;
        this.query = query;
        // log.info("operator sinking is used to filter data:{}",
        //         query.toString());
    }

    public static ScanIterator of(ScanIterator it, byte[] conditionQuery) {
        if (ArrayUtils.isEmpty(conditionQuery)) {
            return it;
        }
        ConditionQuery query = ConditionQuery.fromBytes(conditionQuery);
        return new FilterIterator(it, query);
    }

    @Override
    public boolean hasNext() {
        boolean match = false;
        if (this.query.resultType().isVertex() ||
            this.query.resultType().isEdge()) {
            BackendEntry entry = null;
            while (iterator.hasNext()) {
                current = iterator.next();
                BackendEntry.BackendColumn column =
                        BackendEntry.BackendColumn.of(
                                current.name, current.value);
                BackendEntry.BackendColumn[] columns =
                        new BackendEntry.BackendColumn[]{column};
                if (entry == null || !belongToMe(entry, column) ||
                    this.query.resultType().isEdge()) {
                    entry = new BinaryBackendEntry(query.resultType(),
                                                   current.name);
                    entry.columns(Arrays.asList(columns));
                } else {
                    // 有可能存在包含多个 column 的情况
                    entry.columns(Arrays.asList(columns));
                    continue;
                }
                HugeElement element = this.parseEntry(entry,
                                                      this.query.resultType()
                                                                .isVertex());
                match = query.test(element);
                if (match) {
                    break;
                }
            }
        } else {
            boolean has = iterator.hasNext();
            if (has) {
                current = iterator.next();
            }
            return has;
        }
        return match;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public <T> T next() {
        return (T) current;
    }

    @Override
    public long count() {
        return iterator.count();
    }

    @Override
    public byte[] position() {
        return iterator.position();
    }

    @Override
    public void seek(byte[] position) {
        this.iterator.seek(position);
    }

    @Override
    public void close() {
        iterator.close();
    }
}
