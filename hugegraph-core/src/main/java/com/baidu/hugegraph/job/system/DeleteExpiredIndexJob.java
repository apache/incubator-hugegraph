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

package com.baidu.hugegraph.job.system;

import java.util.Iterator;
import java.util.Set;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public class DeleteExpiredIndexJob<V> extends DeleteExpiredJob<V> {

    private static final String JOB_TYPE = "delete_expired_index";

    private Set<HugeIndex> indexes;

    public DeleteExpiredIndexJob(Set<HugeIndex> indexes) {
        E.checkArgument(indexes != null && !indexes.isEmpty(),
                        "The indexes can't be null or empty");
        this.indexes = indexes;
    }

    @Override
    public String type() {
        return JOB_TYPE;
    }

    @Override
    public V execute() throws Exception {
        LOG.debug("Delete expired indexes: {}", this.indexes);

        HugeGraphParams graph = this.params();
        GraphTransaction tx = graph.graphTransaction();
        try {
            for (HugeIndex index : this.indexes) {
                this.deleteExpiredIndex(graph, index);
            }
            tx.commit();
        } catch (Throwable e) {
            tx.rollback();
            LOG.warn("Failed to delete expired indexes: {}", this.indexes);
            throw e;
        } finally {
            JOB_COUNTERS.jobCounter(graph).decrement();
        }
        return null;
    }

    /*
     * Delete expired element(if exist) of the index,
     * otherwise just delete expired index only
     */
    private void deleteExpiredIndex(HugeGraphParams graph, HugeIndex index) {
        GraphTransaction tx = graph.graphTransaction();
        HugeType type = index.indexLabel().queryType().isVertex()?
                        HugeType.VERTEX : HugeType.EDGE;
        IdQuery query = new IdQuery(type);
        query.query(index.elementId());
        query.showExpired(true);
        Iterator<?> elements = type.isVertex() ?
                               tx.queryVertices(query) :
                               tx.queryEdges(query);
        if (elements.hasNext()) {
            HugeElement element = (HugeElement) elements.next();
            if (element.expiredTime() == index.expiredTime()) {
                element.remove();
            } else {
                tx.removeIndex(index);
            }
        } else {
            tx.removeIndex(index);
        }
    }
}
