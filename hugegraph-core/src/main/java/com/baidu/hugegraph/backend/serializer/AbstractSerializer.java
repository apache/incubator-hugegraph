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

package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public abstract class AbstractSerializer
                implements GraphSerializer, SchemaSerializer {

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    protected abstract BackendEntry newBackendEntry(HugeType type, Id id);

    protected abstract Id writeQueryId(HugeType type, Id id);

    protected abstract Id writeQueryEdgeCondition(Query query);

    protected abstract void writeQueryCondition(Query query);

    @Override
    public Query writeQuery(Query query) {
        HugeType type = query.resultType();

        // Serialize edge condition query (TODO: add VEQ(for EOUT/EIN))
        if (type == HugeType.EDGE && !query.conditions().isEmpty()) {
            if (!query.ids().isEmpty()) {
                throw new BackendException("Not supported query edge by id " +
                                           "and by condition at the same time");
            }

            Id id = this.writeQueryEdgeCondition(query);
            if (id != null) {
                return new IdQuery(query, id);
            }
        }

        // Serialize id in query
        if (query instanceof IdQuery && !query.ids().isEmpty()) {
            IdQuery result = (IdQuery) query.copy();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(this.writeQueryId(type, id));
            }
            return result;
        }

        // Serialize condition(key/value) in query
        if (query instanceof ConditionQuery && !query.conditions().isEmpty()) {
            this.writeQueryCondition(query);
        }

        return query;
    }
}
