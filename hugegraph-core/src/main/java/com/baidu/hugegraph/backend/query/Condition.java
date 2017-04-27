package com.baidu.hugegraph.backend.query;

import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.base.Preconditions;

public abstract class Condition {

    public enum ConditionType {
        NONE,
        RELATION,
        AND,
        OR;
    }

    public enum RelationType {
        EQ("=="),
        GT(">"),
        GTE(">="),
        LT("<"),
        LTE("<="),
        NEQ("!=");

        private final String operator;

        private RelationType(String op) {
            this.operator = op;
        }

        public String string() {
            return this.operator;
        }
    }

    public abstract ConditionType type();

    public abstract boolean isSysprop();

    public Condition and(Condition other) {
        return new And(this, other);
    }

    public Condition or(Condition other) {
        return new Or(this, other);
    }

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

    // Condition defines
    public static abstract class BinCondition extends Condition {
        private Condition left;
        private Condition right;

        public BinCondition(Condition left, Condition right) {
            Preconditions.checkNotNull(left, "Condition can't be null");
            Preconditions.checkNotNull(right, "Condition can't be null");
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
        public String toString() {
            return String.format("%s %s %s",
                    this.left, this.type().name(), this.right);
        }
    }

    public static class And extends BinCondition {
        public And(Condition left, Condition right) {
            super(left, right);
        }

        @Override
        public ConditionType type() {
            return ConditionType.AND;
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
    }

    public abstract static class Relation extends Condition {
        // relational operator (like: =, >, <, in, ...)
        protected RelationType relation;
        // single-type value or a list of single-type value
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

        @Override
        public String toString() {
            return String.format("%s%s%s",
                    this.key(), this.relation.string(), this.value);
        }

        @Override
        public abstract boolean isSysprop();

        public abstract Object key();
    }

    public static class SyspropRelation extends Relation {
        // column name
        private HugeKeys key;

        public SyspropRelation(HugeKeys key) {
            this(key, null, null);
        }

        public SyspropRelation(HugeKeys key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public SyspropRelation(HugeKeys key, RelationType op, Object value) {
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public HugeKeys key() {
            return this.key;
        }

        @Override
        public boolean isSysprop() {
            return true;
        }
    }

    public static class UserpropRelation extends Relation {
        // column name
        private String key;

        public UserpropRelation(String key) {
            this(key, null, null);
        }

        public UserpropRelation(String key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public UserpropRelation(String key, RelationType op, Object value) {
            this.key = key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public String key() {
            return this.key;
        }

        @Override
        public boolean isSysprop() {
            return false;
        }
    }
}
