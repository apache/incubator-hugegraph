package com.baidu.hugegraph.backend.query;

import com.baidu.hugegraph.type.define.HugeKeys;

public abstract class Condition {

    public enum ConditionType {
        NONE,
        RELATION,
        AND,
        OR;
    }

    public enum RelationType {
        EQ,
        GT,
        GTE,
        LT,
        LTE,
        NEQ;
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
        return new Relation(key, RelationType.EQ, value);
    }

    public static Relation gt(HugeKeys key, Object value) {
        return new Relation(key, RelationType.GT, value);
    }

    public static Relation gte(HugeKeys key, Object value) {
        return new Relation(key, RelationType.GTE, value);
    }

    public static Relation lt(HugeKeys key, Object value) {
        return new Relation(key, RelationType.LT, value);
    }

    public static Relation lte(HugeKeys key, Object value) {
        return new Relation(key, RelationType.LTE, value);
    }

    public static Relation neq(HugeKeys key, Object value) {
        return new Relation(key, RelationType.NEQ, value);
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

    public static class Relation extends Condition {
        // column name
        private HugeKeys key;
        // relational operator (like: =, >, <, in, ...)
        private RelationType relation;
        // single-type value or a list of single-type value
        private Object value;

        public Relation(HugeKeys key) {
            this(key, null, null);
        }

        public Relation(HugeKeys key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public Relation(HugeKeys key, RelationType op, Object value) {
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

        public RelationType relation() {
            return this.relation;
        }

        public Object value() {
            return this.value;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s",
                    this.key, this.relation.name(), this.value);
        }
    }

    private static final Condition NONE = new Condition() {
        @Override
        public ConditionType type() {
            return ConditionType.NONE;
        }

        @Override
        public String toString() {
            return "<NONE>";
        }
    };
}
