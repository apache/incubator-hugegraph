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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public abstract class Condition {

    public enum ConditionType {
        NONE,
        RELATION,
        AND,
        OR;
    }

    public enum RelationType {
        EQ("==", (v1, v2) -> { return v1.equals(v2); }),
        GT(">", (v1, v2) -> { return compare(v1, v2) > 0; }),
        GTE(">=", (v1, v2) -> { return compare(v1, v2) >= 0; }),
        LT("<", (v1, v2) -> { return compare(v1, v2) < 0; }),
        LTE("<=", (v1, v2) -> { return compare(v1, v2) <= 0; }),
        NEQ("!=", (v1, v2) -> { return compare(v1, v2) != 0; }),
        IN("in", (v1, v2) -> { return ((List<?>) v2).contains(v1); }),
        NOT_IN("notin", (v1, v2) -> { return !((List<?>) v2).contains(v1); }),
        CONTAINS_KEY("containskey", null),
        SCAN("scan", null);

        private final String operator;
        private final BiFunction<Object, Object, Boolean> tester;

        private RelationType(String op,
                             BiFunction<Object, Object, Boolean> tester) {
            this.operator = op;
            this.tester = tester;
        }

        public String string() {
            return this.operator;
        }

        protected static int compare(final Object first, final Object second) {
            if (first == null || second == null) {
                throw new BackendException(
                          "Can't compare between %s and %s", first, second);
            }

            Function<Object, Number> toBig = (number) -> {
                try {
                    return new BigDecimal(number.toString());
                } catch (NumberFormatException e) {
                    throw new BackendException(
                              "Can't compare between %s and %s, " +
                              "must be numbers", first, second);
                }
            };

            Number n1 = (first instanceof Number) ?
                        (Number) first :
                        toBig.apply(first);

            Number n2 = (second instanceof Number) ?
                        (Number) second :
                        toBig.apply(second);

            /*
             * It is proberly that the `first` is serialized to String. Hence,
             * Convert the `second` to String too, and then to Big.
             */
            if (!n1.getClass().equals(n2.getClass()) &&
                first.getClass().equals(String.class)) {
                n2 = toBig.apply(second);
            }

            // Check they are the same type
            if (!n1.getClass().equals(n2.getClass())) {
                throw new BackendException(
                          "Can't compare class %s with class %s",
                          first.getClass().getSimpleName(),
                          second.getClass().getSimpleName());
            }

            @SuppressWarnings("unchecked")
            Comparable<Number> n1cmp = (Comparable<Number>) n1;
            assert n1.getClass().equals(n2.getClass());
            return n1cmp.compareTo(n2);
        }

        public boolean test(Object value1, Object value2) {
            E.checkState(this.tester != null, "Can't test %s", this.name());
            return this.tester.apply(value1, value2);
        }

        public boolean isSearchType() {
            return ImmutableSet.of(GT, GTE, LT, LTE, NEQ).contains(this);
        }
    }

    /*************************************************************************/

    public abstract ConditionType type();

    public abstract boolean isSysprop();

    public abstract List<? extends Relation> relations();

    public abstract boolean test(Object value);

    public abstract Condition copy();

    public abstract Condition replace(Relation from, Relation to);

    /*************************************************************************/

    public Condition and(Condition other) {
        return new And(this, other);
    }

    public Condition or(Condition other) {
        return new Or(this, other);
    }

    public boolean isRelation() {
        return this.type() == ConditionType.RELATION;
    }

    public boolean isLogic() {
        return this.type() == ConditionType.AND ||
               this.type() == ConditionType.OR;
    }

    /*************************************************************************/

    public static Condition and(Condition left, Condition right) {
        return new And(left, right);
    }

    public static Condition or(Condition left, Condition right) {
        return new Or(left, right);
    }

    public static Relation eq(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.EQ, value);
    }

    public static Relation gt(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.GT, value);
    }

    public static Relation gte(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.GTE, value);
    }

    public static Relation lt(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.LT, value);
    }

    public static Relation lte(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.LTE, value);
    }

    public static Relation neq(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.NEQ, value);
    }

    public static Condition in(HugeKeys key, List<?> value) {
        return new SyspropRelation(key, RelationType.IN, value);
    }

    public static Condition nin(HugeKeys key, List<?> value) {
        return new SyspropRelation(key, RelationType.NOT_IN, value);
    }

    public static Condition containsKey(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.CONTAINS_KEY, value);
    }

    public static Condition scan(String start, String end) {
        SyspropRelation s = new SyspropRelation(null, RelationType.SCAN, end);
        s.key(start);
        return s;
    }

    public static Relation eq(String key, Object value) {
        return new UserpropRelation(key, RelationType.EQ, value);
    }

    public static Relation gt(String key, Object value) {
        return new UserpropRelation(key, RelationType.GT, value);
    }

    public static Relation gte(String key, Object value) {
        return new UserpropRelation(key, RelationType.GTE, value);
    }

    public static Relation lt(String key, Object value) {
        return new UserpropRelation(key, RelationType.LT, value);
    }

    public static Relation lte(String key, Object value) {
        return new UserpropRelation(key, RelationType.LTE, value);
    }

    public static Relation neq(String key, Object value) {
        return new UserpropRelation(key, RelationType.NEQ, value);
    }

    public static Condition in(String key, List<?> value) {
        return new UserpropRelation(key, RelationType.IN, value);
    }

    public static Condition nin(String key, List<?> value) {
        return new UserpropRelation(key, RelationType.NOT_IN, value);
    }

    /*************************************************************************/

    /**
     * Condition defines
     */
    public static abstract class BinCondition extends Condition {
        private Condition left;
        private Condition right;

        public BinCondition(Condition left, Condition right) {
            E.checkNotNull(left, "left condition");
            E.checkNotNull(right, "right condition");
            this.left = left;
            this.right = right;
        }

        public Condition left() {
            return this.left;
        }

        public Condition right() {
            return this.right;
        }

        @Override
        public boolean isSysprop() {
            return this.left.isSysprop() && this.right.isSysprop();
        }

        @Override
        public List<? extends Relation> relations() {
            List<Relation> list = new ArrayList<>(this.left.relations());
            list.addAll(this.right.relations());
            return list;
        }

        @Override
        public Condition replace(Relation from, Relation to) {
            this.left = this.left.replace(from, to);
            this.right = this.right.replace(from, to);
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s",
                                 this.left,
                                 this.type().name(),
                                 this.right);
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof BinCondition)) {
                return false;
            }
            BinCondition other = (BinCondition) object;
            return this.type().equals(other.type()) &&
                   this.left().equals(other.left()) &&
                   this.right().equals(other.right());
        }

        @Override
        public int hashCode() {
            return this.type().hashCode() ^
                   this.left().hashCode() ^
                   this.right().hashCode();
        }
    }

    /*************************************************************************/

    public static class And extends BinCondition {
        public And(Condition left, Condition right) {
            super(left, right);
        }

        @Override
        public ConditionType type() {
            return ConditionType.AND;
        }

        @Override
        public boolean test(Object value) {
            return this.left().test(value) && this.right().test(value);
        }

        @Override
        public Condition copy() {
            return new And(this.left().copy(), this.right().copy());
        }
    }

    public static class Or extends BinCondition {
        public Or(Condition left, Condition right) {
            super(left, right);
        }

        @Override
        public ConditionType type() {
            return ConditionType.OR;
        }

        @Override
        public boolean test(Object value) {
            return this.left().test(value) || this.right().test(value);
        }

        @Override
        public Condition copy() {
            return new Or(this.left().copy(), this.right().copy());
        }
    }

    /*************************************************************************/

    public abstract static class Relation extends Condition {
        // Relational operator (like: =, >, <, in, ...)
        protected RelationType relation;
        // Single-type value or a list of single-type value
        protected Object value;

        @Override
        public ConditionType type() {
            return ConditionType.RELATION;
        }

        public RelationType relation() {
            return this.relation;
        }

        public Object value() {
            return this.value;
        }

        public void value(Object value) {
            this.value = value;
        }

        @Override
        public boolean test(Object value) {
            return this.relation.test(value, this.value);
        }

        @Override
        public List<? extends Relation> relations() {
            return ImmutableList.of(this);
        }

        @Override
        public Condition replace(Relation from, Relation to) {
            if (this == from) {
                return to;
            } else {
                return this;
            }
        }

        @Override
        public String toString() {
            return String.format("%s %s %s",
                                 this.key(),
                                 this.relation.string(),
                                 this.value);
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Relation)) {
                return false;
            }
            Relation other = (Relation) object;
            return this.relation().equals(other.relation()) &&
                   this.key().equals(other.key()) &&
                   this.value().equals(other.value());
        }

        @Override
        public int hashCode() {
            return this.type().hashCode() ^
                   this.relation().hashCode() ^
                   this.key().hashCode() ^
                   this.value().hashCode();
        }

        @Override
        public abstract boolean isSysprop();

        public abstract Object key();

        public abstract void key(Object key);
    }

    public static class SyspropRelation extends Relation {

        /*
         * Column name. TODO: the key should be serialized(code/string) by
         * backend store private Object key.
         */
        private Object key;

        public SyspropRelation(HugeKeys key, Object value) {
            this((Object) key, RelationType.EQ, value);
        }

        public SyspropRelation(HugeKeys key, RelationType op, Object value) {
            this((Object) key, op, value);
        }

        private SyspropRelation(Object key, RelationType op, Object value) {
            E.checkNotNull(op, "relation type");
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public Object key() {
            return this.key;
        }

        @Override
        public void key(Object key) {
            this.key = key;
        }

        @Override
        public boolean isSysprop() {
            return true;
        }

        @Override
        public Condition copy() {
            return new SyspropRelation(this.key, this.relation(), this.value);
        }
    }

    public static class UserpropRelation extends Relation {
        // Column name
        private String key;

        public UserpropRelation(String key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public UserpropRelation(String key, RelationType op, Object value) {
            E.checkNotNull(op, "relation type");
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public String key() {
            return this.key;
        }

        @Override
        public void key(Object key) {
            this.key = key.toString();
        }

        @Override
        public boolean isSysprop() {
            return false;
        }

        @Override
        public Condition copy() {
            return new UserpropRelation(this.key, this.relation(), this.value);
        }
    }
}
