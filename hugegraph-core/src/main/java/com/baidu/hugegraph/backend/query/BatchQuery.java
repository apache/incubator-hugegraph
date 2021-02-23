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

package com.baidu.hugegraph.backend.query;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import com.baidu.hugegraph.backend.query.Condition.RelationType;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class BatchQuery extends ConditionQuery {

    private Condition.Relation in = null;

    public BatchQuery(HugeType resultType) {
        super(resultType);
    }

    public void mergeWithIn(ConditionQuery query, HugeKeys key) {
        Object value = query.condition(key);

        if (this.in == null) {
            assert !this.containsRelation(RelationType.IN);
            this.resetConditions(new LinkedHashSet<>(query.conditions()));
            this.unsetCondition(key);

            List<Object> list = new LinkedList<>(ImmutableList.of(value));
            // TODO: ensure not flatten BatchQuery
            this.in = (Condition.Relation) Condition.in(key, list);
            this.query(this.in);
        } else {
            E.checkArgument(this.in.key().equals(key),
                            "Invalid key '%s'", key);
            E.checkArgument(keysEquals(query),
                            "Can't merge query with different keys");

            @SuppressWarnings("unchecked")
            List<Object> values = ((List<Object>) this.in.value());
            values.add(value);
        }
    }

    protected boolean keysEquals(ConditionQuery query) {
        List<Condition.Relation> relations = query.relations();
        if (relations.size() != this.relations().size()) {
            return false;
        }
        List<Object> keys = new ArrayList<>(relations.size());
        for (Condition.Relation r : this.relations()) {
            keys.add(r.key());
        }

        for (Condition.Relation r : this.relations()) {
            if (r.relation() == RelationType.IN) {
                continue;
            }
            Object key = r.key();
            if (!keys.contains(key)) {
                return false;
            }
            if (!this.condition(key).equals(query.condition(key))) {
                return false;
            }
        }
        return true;
    }
}
