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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.type.Shard;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;
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
        EQ("==", (v1, v2) -> {return equals(v1, v2); }),
        GT(">", (v1, v2) -> { return compare(v1, v2) > 0; }),
        GTE(">=", (v1, v2) -> { return compare(v1, v2) >= 0; }),
        LT("<", (v1, v2) -> { return compare(v1, v2) < 0; }),
        LTE("<=", (v1, v2) -> { return compare(v1, v2) <= 0; }),
        NEQ("!=", (v1, v2) -> { return compare(v1, v2) != 0; }),
        IN("in", (v1, v2) -> {
            return ((Collection<?>) v2).contains(v1);
        }),
        NOT_IN("notin", (v1, v2) -> {
            return !((Collection<?>) v2).contains(v1);
        }),
        CONTAINS("contains", (v1, v2) -> {
            return ((Map<?, ?>) v1).containsValue(v2);
        }),
        CONTAINS_KEY("containskey", (v1, v2) -> {
            return ((Map<?, ?>) v1).containsKey(v2);
        }),
        SCAN("scan", (v1, v2) -> true);

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

        /**
         * Determine two values of any type equal
         * @Param first is actual value
         * @Param second is value in query condition
         */
        protected static boolean equals(final Object first,
                                        final Object second) {
            if (first == null) {
                return second == null;
            } else if (first instanceof Id) {
                if (second instanceof String) {
                    return second.equals(((Id) first).asString());
                } else if (second instanceof Long) {
                    return second.equals(((Id) first).asLong());
                }
            } else if (first instanceof Number || second instanceof Number) {
                return compare(first, second) == 0;
            }

            return first.equals(second);
        }

        /**
         * Determine two numbers equal
         * @param first is actual value, might be Number/Date or String, It is
         *              probably that the `first` is serialized to String.
         * @param second is value in query condition, must be Number/Date
         */
        protected static int compare(final Object first, final Object second) {
            if (second instanceof Number) {
                return NumericUtil.compareNumber(first, (Number) second);
            } else if (second instanceof Date) {
                return compareDate(first, (Date) second);
            } else {
                throw new BackendException("Can't compare between %s and %s",
                                           first, second);
            }
        }

        protected static int compareDate(Object first, Date second) {
            if (first instanceof Date) {
                return ((Date) first).compareTo(second);
            } else {
                throw new BackendException(
                          "Can't compare between %s and %s, they must be date",
                          first, second);
            }
        }

        public boolean test(Object first, Object second) {
            E.checkState(this.tester != null, "Can't test %s", this.name());
            return this.tester.apply(first, second);
        }

        public boolean isRangeType() {
            return ImmutableSet.of(GT, GTE, LT, LTE, NEQ).contains(this);
        }
    }

    /*************************************************************************/

    public abstract ConditionType type();

    public abstract boolean isSysprop();

    public abstract List<? extends Relation> relations();

    public abstract boolean test(Object value);

    public abstract boolean test(HugeElement element);

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

    public static Condition contains(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.CONTAINS, value);
    }

    public static Condition containsKey(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.CONTAINS_KEY, value);
    }

    public static Condition scan(String start, String end) {
        Shard value = new Shard(start, end, 0);
        return new SyspropRelation(HugeKeys.ID, RelationType.SCAN, value);
    }

    public static Relation eq(Id key, Object value) {
        return new UserpropRelation(key, RelationType.EQ, value);
    }

    public static Relation gt(Id key, Object value) {
        return new UserpropRelation(key, RelationType.GT, value);
    }

    public static Relation gte(Id key, Object value) {
        return new UserpropRelation(key, RelationType.GTE, value);
    }

    public static Relation lt(Id key, Object value) {
        return new UserpropRelation(key, RelationType.LT, value);
    }

    public static Relation lte(Id key, Object value) {
        return new UserpropRelation(key, RelationType.LTE, value);
    }

    public static Relation neq(Id key, Object value) {
        return new UserpropRelation(key, RelationType.NEQ, value);
    }

    public static Condition in(Id key, List<?> value) {
        return new UserpropRelation(key, RelationType.IN, value);
    }

    public static Condition nin(Id key, List<?> value) {
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
        public boolean test(HugeElement element) {
            return this.left().test(element) && this.right().test(element);
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
        public boolean test(HugeElement element) {
            return this.left().test(element) || this.right().test(element);
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

        // The key serialized(code/string) by backend store.
        protected Object serialKey;
        // The value serialized(code/string) by backend store.
        protected Object serialValue;

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

        public void serialKey(Object key) {
            this.serialKey = key;
        }

        public Object serialKey() {
            return this.serialKey != null ? this.serialKey : this.key();
        }

        public void serialValue(Object value) {
            this.serialValue = value;
        }

        public Object serialValue() {
            return this.serialValue != null ? this.serialValue : this.value();
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
    }

    public static class SyspropRelation extends Relation {

        private HugeKeys key;

        public SyspropRelation(HugeKeys key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public SyspropRelation(HugeKeys key, RelationType op, Object value) {
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
        public boolean isSysprop() {
            return true;
        }

        @Override
        public boolean test(HugeElement element) {
            if (this.relation == RelationType.SCAN) {
                return true;
            }
            Object value = element.sysprop(this.key);
            return this.relation.test(value, this.value);
        }

        @Override
        public Condition copy() {
            return new SyspropRelation(this.key, this.relation(), this.value);
        }
    }

    public static class UserpropRelation extends Relation {
        // Id of property key
        private Id key;

        public UserpropRelation(Id key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public UserpropRelation(Id key, RelationType op, Object value) {
            E.checkNotNull(op, "relation type");
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public Id key() {
            return this.key;
        }

        @Override
        public boolean isSysprop() {
            return false;
        }

        @Override
        public boolean test(HugeElement element) {
            HugeProperty<?> prop = element.getProperty(this.key());
            Object value = prop != null ? prop.value() : null;
            return this.relation.test(value, this.value);
        }

        @Override
        public Condition copy() {
            return new UserpropRelation(this.key, this.relation(), this.value);
        }
    }
}
