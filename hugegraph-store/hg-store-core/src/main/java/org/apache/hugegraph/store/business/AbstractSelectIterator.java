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

import org.apache.hugegraph.backend.serializer.AbstractSerializer;
import org.apache.hugegraph.backend.serializer.BinarySerializer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSelectIterator implements ScanIterator {

    protected ScanIterator iterator;
    protected AbstractSerializer serializer;

    public AbstractSelectIterator() {
        this.serializer = new BinarySerializer();
    }

    public boolean belongToMe(BackendEntry entry,
                              BackendEntry.BackendColumn column) {
        return Bytes.prefixWith(column.name, entry.id().asBytes());
    }

    public HugeElement parseEntry(BackendEntry entry, boolean isVertex) {
        try {
            if (isVertex) {
                return this.serializer.readVertex(null, entry);
            } else {
                CIter<Edge> itr =
                        this.serializer.readEdges(null, entry);

                // Iterator<HugeEdge> itr = this.serializer.readEdges(
                //         null, entry, true, false).iterator();
                HugeElement el = null;
                if (itr.hasNext()) {
                    el = (HugeElement) itr.next();
                }
                return el;
            }
        } catch (Exception e) {
            log.error("Failed to parse entry: {}", entry, e);
            throw e;
        }
    }
}
