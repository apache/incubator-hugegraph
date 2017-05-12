package com.baidu.hugegraph.backend.query;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public abstract class Condition {

    /*************************************************************************/

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
        HAS_KEY(" haskey ", null);

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

        public static int compare(final Object first, final Object second) {
            if (first == null || second == null) {
                throw new BackendException(String.format(
                        "Can't compare between %s and %s",
                        first, second));
            }

            if (!first.getClass().equals(second.getClass())) {
                throw new BackendException(String.format(
                        "Can't compare class %s with class %s",
                        first.getClass().getSimpleName(),
                        second.getClass().getSimpleName()));
            }

            Function<Object, Number> toBig = (number) -> {
                try {
                    return new BigDecimal(number.toString());
                } catch (NumberFormatException e) {
                    throw new BackendException(String.format(
                            "Can't compare between %s and %s, must be numbers",
                            first, second));
                }
            };

            Number n1 = (first instanceof Number)
                    ? (Number) first : toBig.apply(first);

            Number n2 = (second instanceof Number)
                    ? (Number) second : toBig.apply(second);

            assert n1.getClass().equals(n2.getClass());
            @SuppressWarnings("unchecked")
            Comparable<Number> n1cmp = (Comparable<Number>) n1;
            return n1cmp.compareTo(n2);
        }

        public boolean test(Object value1, Object value2) {
            Preconditions.checkNotNull(this.tester,
                    "Can't test " + this.name());
            return this.tester.apply(value1, value2);
        }
    }

    /*************************************************************************/

    public abstract ConditionType type();

    public abstract boolean isSysprop();

    public abstract List<? extends Relation> relations();

    /*************************************************************************/

    public Condition and(Condition other) {
        return new And(this, other);
    }

    public Condition or(Condition other) {
        return new Or(this, other);
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

    public static Condition hasKey(HugeKeys key, Object value) {
        return new SyspropRelation(key, RelationType.HAS_KEY, value);
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

    /*************************************************************************/

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
        public List<? extends Relation> relations() {
            List<Relation> list = new LinkedList<>(this.left.relations());
            list.addAll(this.right.relations());
            return list;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s",
                    this.left, this.type().name(), this.right);
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

    /*************************************************************************/

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

        public void value(Object value) {
            this.value = value;
        }

        public boolean test(Object value) {
            return this.relation.test(value, this.value);
        }

        @Override
        public List<? extends Relation> relations() {
            return ImmutableList.of(this);
        }

        @Override
        public String toString() {
            return String.format("%s%s%s",
                    this.key(), this.relation.string(), this.value);
        }

        @Override
        public abstract boolean isSysprop();

        public abstract Object key();

        public abstract void key(Object key);
    }

    public static class SyspropRelation extends Relation {
        // column name
        // TODO: the key should be serialized(code/string) by back-end store
        private Object key;

        public SyspropRelation(HugeKeys key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public SyspropRelation(HugeKeys key, RelationType op, Object value) {
            Preconditions.checkNotNull(op);
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
    }

    public static class UserpropRelation extends Relation {
        // column name
        private String key;

        public UserpropRelation(String key, Object value) {
            this(key, RelationType.EQ, value);
        }

        public UserpropRelation(String key, RelationType op, Object value) {
            Preconditions.checkNotNull(op);
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
    }
}
