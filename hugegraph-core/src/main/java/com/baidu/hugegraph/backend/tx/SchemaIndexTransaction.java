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

package com.baidu.hugegraph.backend.tx;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

public class SchemaIndexTransaction extends AbstractTransaction {

    public SchemaIndexTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
    }

    @Watched(prefix = "index")
    public void updateNameIndex(SchemaElement element, boolean removed) {
        if (!this.needIndexForName()) {
            return;
        }

        IndexLabel indexLabel = IndexLabel.label(element.type());
        // Update name index if backend store not supports name-query
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(element.name());
        index.elementIds(element.id());

        if (removed) {
            this.doEliminate(this.serializer.writeIndex(index));
        } else {
            this.doAppend(this.serializer.writeIndex(index));
        }
    }

    private boolean needIndexForName() {
        return !this.store().features().supportsQuerySchemaByName();
    }

    @Watched(prefix = "index")
    @Override
    public QueryResults<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            ConditionQuery q = (ConditionQuery) query;
            if (q.allSysprop() && q.conditions().size() == 1 &&
                q.containsCondition(HugeKeys.NAME)) {
                return this.queryByName(q);
            }
        }
        return super.query(query);
    }

    @Watched(prefix = "index")
    private QueryResults<BackendEntry> queryByName(ConditionQuery query) {
        if (!this.needIndexForName()) {
            return super.query(query);
        }
        IndexLabel il = IndexLabel.label(query.resultType());
        String name = (String) query.condition(HugeKeys.NAME);
        E.checkState(name != null, "The name in condition can't be null " +
                     "when querying schema by name");

        ConditionQuery indexQuery;
        indexQuery = new ConditionQuery(HugeType.SECONDARY_INDEX, query);
        indexQuery.eq(HugeKeys.FIELD_VALUES, name);
        indexQuery.eq(HugeKeys.INDEX_LABEL_ID, il.id());

        IdQuery idQuery = new IdQuery(query.resultType(), query);
        Iterator<BackendEntry> entries = super.query(indexQuery).iterator();
        try {
            while (entries.hasNext()) {
                HugeIndex index = this.serializer.readIndex(graph(), indexQuery,
                                                            entries.next());
                idQuery.query(index.elementIds());
                Query.checkForceCapacity(idQuery.ids().size());
            }
        } finally {
            CloseableIterator.closeIterator(entries);
        }

        if (idQuery.ids().isEmpty()) {
            return QueryResults.empty();
        }

        assert idQuery.ids().size() == 1 : idQuery.ids();
        return super.query(idQuery);
    }
}
