package com.baidu.hugegraph.backend.query;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;

import com.baidu.hugegraph.type.define.HugeKeys;

public abstract class Condition {

    public enum ConditionType {
        NONE,
        RELATION,
        AND,
        OR;
    }

    public abstract ConditionType type();

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
        return new Relation(key, Compare.eq, value);
    }

    public static Relation gt(HugeKeys key, Object value) {
        return new Relation(key, Compare.gt, value);
    }

    public static Relation gte(HugeKeys key, Object value) {
        return new Relation(key, Compare.gte, value);
    }

    public static Relation lt(HugeKeys key, Object value) {
        return new Relation(key, Compare.lt, value);
    }

    public static Relation lte(HugeKeys key, Object value) {
        return new Relation(key, Compare.lte, value);
    }

    public static Relation neq(HugeKeys key, Object value) {
        return new Relation(key, Compare.neq, value);
    }

    public static Condition none() {
        return NONE;
    }

    // Condition defines
    public static abstract class BinCondition extends Condition {
        private Condition left;
        private Condition right;

        public BinCondition(Condition left, Condition right) {
            this.left = left;
            this.right = right;
        }

        public Condition left() {
            return this.left;
        }

        public Condition right() {
            return this.right;
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

    public static class Relation extends Condition {
        // column name
        private HugeKeys key;
        // relational operator (like: =, >, <, in, ...)
        private Compare relation;
        // single-type value or a list of single-type value
        private Object value;

        public Relation(HugeKeys key) {
            this(key, null, null);
        }

        public Relation(HugeKeys key, Object value) {
            this(key, Compare.eq, value);
        }

        public Relation(HugeKeys key, Compare op, Object value) {
            this.key =key;
            this.relation = op;
            this.value = value;
        }

        @Override
        public ConditionType type() {
            return ConditionType.RELATION;
        }

        public HugeKeys key() {
            return this.key;
        }

        public Compare relation() {
            return this.relation;
        }

        public Object value() {
            return this.value;
        }
    }

    private static final Condition NONE = new Condition() {
        @Override
        public ConditionType type() {
            return ConditionType.NONE;
        }
    };
}
