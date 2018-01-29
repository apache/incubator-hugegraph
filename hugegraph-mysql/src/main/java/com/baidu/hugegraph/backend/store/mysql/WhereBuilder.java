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

package com.baidu.hugegraph.backend.store.mysql;

import java.util.List;

import com.baidu.hugegraph.util.E;

public class WhereBuilder {

    private StringBuilder builder;

    public WhereBuilder() {
        this.builder = new StringBuilder(" WHERE ");
    }

    /**
     * Concat as: key1 = value and key2 = value...
     */
    public void and(List<String> keys, String value) {
        this.and(keys, " = ", value);
    }

    /**
     * Concat as: key1 op value and key2 op value...
     */
    public void and(List<String> keys, String operator, String value) {
        for (int i = 0, n = keys.size(); i < n; i++) {
            this.builder.append(keys.get(i));
            this.builder.append(operator);
            this.builder.append(value);
            if (i != n - 1) {
                this.builder.append(" AND ");
            }
        }
    }

    /**
     * Concat as: key1 = value1 and key2 = value2...
     */
    public void and(List<String> keys, List<Object> values) {
        this.and(keys, " = ", values);
    }

    /**
     * Concat as: key1 op value1 and key2 op value2...
     */
    public void and(List<String> keys, String operator, List<Object> values) {
        E.checkArgument(keys.size() == values.size(),
                        "The size of keys '%s' is not equal with " +
                        "values size '%s'",
                        keys.size(), values.size());

        for (int i = 0, n = keys.size(); i < n; i++) {
            this.builder.append(keys.get(i));
            this.builder.append(operator);
            Object value = values.get(i);
            if (value instanceof String) {
                this.builder.append(MysqlUtil.escapeString((String) value));
            } else {
                this.builder.append(value);
            }
            if (i != n - 1) {
                this.builder.append(" AND ");
            }
        }
    }

    /**
     * Concat as: clause1 and clause2...
     */
    public void and(List<StringBuilder> clauses) {
        E.checkArgument(clauses != null && !clauses.isEmpty(),
                        "The clauses can't be empty");

        int size = clauses.size();
        int i = 0;
        for (StringBuilder cluase : clauses) {
            this.builder.append(cluase);
            if (++i != size) {
                this.builder.append(" AND ");
            }
        }
    }

    /**
     * Concat as: key in (value1, value2...)
     */
    public void in(String key, List<Object> values) {
        this.builder.append(key).append(" IN (");
        for (int i = 0, n = values.size(); i < n; i++) {
            Object value = values.get(i);
            if (value instanceof String) {
                this.builder.append(MysqlUtil.escapeString((String) value));
            } else {
                this.builder.append(value);
            }
            if (i != n - 1) {
                this.builder.append(", ");
            }
        }
        this.builder.append(")");
    }

    public String build() {
        return this.builder.toString();
    }

    @Override
    public String toString() {
        return this.builder.toString();
    }
}
