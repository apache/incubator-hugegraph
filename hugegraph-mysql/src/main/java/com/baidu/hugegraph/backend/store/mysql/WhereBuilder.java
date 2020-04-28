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

import com.baidu.hugegraph.backend.query.Condition.RelationType;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.util.E;

public class WhereBuilder {

    private StringBuilder builder;

    public WhereBuilder() {
        this(true);
    }

    public WhereBuilder(boolean startWithWhere) {
        if (startWithWhere) {
            this.builder = new StringBuilder(" WHERE ");
        } else {
            this.builder = new StringBuilder(" ");
        }
    }

    public WhereBuilder relation(String key, RelationType type, Object value) {
        String operator = null;
        switch (type) {
            case EQ:
                operator = "=";
                break;
            case NEQ:
                operator = "!=";
                break;
            case GT:
                operator = ">";
                break;
            case GTE:
                operator = ">=";
                break;
            case LT:
                operator = "<";
                break;
            case LTE:
                operator = "<=";
                break;
            case IN:
                @SuppressWarnings("unchecked")
                List<Object> values = (List<Object>) value;
                this.in(key, values);
                break;
            case CONTAINS_VALUE:
            case CONTAINS_KEY:
            case SCAN:
            default:
                throw new NotSupportException("relation '%s'", type);
        }
        if (operator != null) {
            this.builder.append(key);
            this.builder.append(operator);
            this.builder.append(wrapStringIfNeeded(value));
        }
        return this;
    }

    /**
     * Concat as: cond1 and cond2...
     * @return WhereBuilder
     */
    public WhereBuilder and() {
        this.builder.append(" AND ");
        return this;
    }

    /**
     * Concat as: key1 op ? and key2 op ?...
     * @param keys the keys to be concatted with value
     * @param operator the operator to link every key and value pair
     * @return WhereBuilder
     */
    public WhereBuilder and(List<String> keys, String operator) {
        for (int i = 0, n = keys.size(); i < n; i++) {
            this.builder.append(keys.get(i));
            this.builder.append(operator);
            this.builder.append("?");
            if (i != n - 1) {
                this.builder.append(" AND ");
            }
        }
        return this;
    }

    /**
     * Concat as: key1 = value1 and key2 = value2...
     * @param keys the keys to be concatted with values according to the
     *             same index
     * @param values the values to be concatted with every keys according to
     *               the same index
     * @return WhereBuilder
     */
    public WhereBuilder and(List<String> keys, List<Object> values) {
        return this.and(keys, "=", values);
    }

    /**
     * Concat as: key1 op value1 and key2 op value2...
     * @param keys the keys to be concatted with values according to the
     *             same index
     * @param operator the operator to link every key and value pair
     * @param values the values to be concatted with every keys according to
     *               the same index
     * @return WhereBuilder
     */
    public WhereBuilder and(List<String> keys,
                            String operator,
                            List<Object> values) {
        E.checkArgument(keys.size() == values.size(),
                        "The size of keys '%s' is not equal with " +
                        "values size '%s'",
                        keys.size(), values.size());

        for (int i = 0, n = keys.size(); i < n; i++) {
            this.builder.append(keys.get(i));
            this.builder.append(operator);
            this.builder.append(wrapStringIfNeeded(values.get(i)));
            if (i != n - 1) {
                this.builder.append(" AND ");
            }
        }
        return this;
    }

    /**
     * Concat as: key1 op1 value1 and key2 op2 value2...
     * @param keys the keys to be concatted with values according to the
     *             same index
     * @param operators the operators to link every key and value pair
     *                  according to the same index
     * @param values the values to be concatted with every keys according to
     *               the same index
     * @return WhereBuilder
     */
    public WhereBuilder and(List<String> keys,
                            List<String> operators,
                            List<Object> values) {
        E.checkArgument(keys.size() == operators.size(),
                        "The size of keys '%s' is not equal with " +
                        "operators size '%s'",
                        keys.size(), operators.size());
        E.checkArgument(keys.size() == values.size(),
                        "The size of keys '%s' is not equal with " +
                        "values size '%s'",
                        keys.size(), values.size());

        for (int i = 0, n = keys.size(); i < n; i++) {
            this.builder.append(keys.get(i));
            this.builder.append(operators.get(i));
            this.builder.append(wrapStringIfNeeded(values.get(i)));
            if (i != n - 1) {
                this.builder.append(" AND ");
            }
        }
        return this;
    }

    /**
     * Concat as: clause1 and clause2...
     * @param clauses the clauses to be concatted with 'AND' operator
     * @return WhereBuilder
     */
    public WhereBuilder and(List<StringBuilder> clauses) {
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
        return this;
    }

    /**
     * Concat as: key in (value1, value2...)
     * @param key the key to be concatted with 'IN' operator
     * @param values the values to be concated with ',' and wappred by '()'
     * @return WhereBuilder
     */
    public WhereBuilder in(String key, List<Object> values) {
        this.builder.append(key).append(" IN (");
        for (int i = 0, n = values.size(); i < n; i++) {
            this.builder.append(wrapStringIfNeeded(values.get(i)));
            if (i != n - 1) {
                this.builder.append(", ");
            }
        }
        this.builder.append(")");
        return this;
    }

    /**
     * Concat as: (key1, key2...keyn) {@code >=} (val1, val2...valn)
     * @param keys the keys to be concatted with {@code >=} operator
     * @param values the values to be concatted with {@code >=} operator
     * @return WhereBuilder
     */
    public WhereBuilder gte(List<String> keys, List<Object> values) {
        E.checkArgument(keys.size() == values.size(),
                        "The size of keys '%s' is not equal with " +
                        "values size '%s'",
                        keys.size(), values.size());
        this.builder.append("(");
        for (int i = 0, n = keys.size(); i < n; i++) {
            this.builder.append(keys.get(i));
            if (i != n - 1) {
                this.builder.append(", ");
            }
        }
        this.builder.append(") >= (");
        for (int i = 0, n = values.size(); i < n; i++) {
            this.builder.append(wrapStringIfNeeded(values.get(i)));
            if (i != n - 1) {
                this.builder.append(", ");
            }
        }
        this.builder.append(")");
        return this;
    }

    public WhereBuilder gte(String key, Object value) {
        this.builder.append(" ")
                    .append(key)
                    .append(" >= ")
                    .append(wrapStringIfNeeded(value))
                    .append(" ");
        return this;
    }

    public WhereBuilder lt(String key, Object value) {
        this.builder.append(" ")
                    .append(key)
                    .append(" < ")
                    .append(wrapStringIfNeeded(value))
                    .append(" ");
        return this;
    }

    public StringBuilder build() {
        return this.builder;
    }

    @Override
    public String toString() {
        return this.builder.toString();
    }

    protected String wrapStringIfNeeded(Object value) {
        if (value instanceof String) {
            return this.escapeAndWrapString((String) value);
        } else {
            return String.valueOf(value);
        }
    }

    protected String escapeAndWrapString(String value) {
        return MysqlUtil.escapeAndWrapString(value);
    }
}
