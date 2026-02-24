package org.apache.hugegraph.traversal.optimize;

import org.apache.hugegraph.backend.query.Condition;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

public class ConditionP extends P<Object> {

    private static final long serialVersionUID = 9094970577400072902L;

    private ConditionP(final Condition.RelationType type, final Object value) {
        super(new PBiPredicate<Object, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(final Object first, final Object second) {
                return type.test(first, second);
            }

            @Override
            public String getPredicateName() {
                // Helps with serialization/debug readability
                return type.name();
            }
        }, value);
    }

    public static ConditionP textContains(final Object value) {
        return new ConditionP(Condition.RelationType.TEXT_CONTAINS, value);
    }

    public static ConditionP contains(final Object value) {
        return new ConditionP(Condition.RelationType.CONTAINS, value);
    }

    public static ConditionP containsK(final Object value) {
        return new ConditionP(Condition.RelationType.CONTAINS_KEY, value);
    }

    public static ConditionP containsV(final Object value) {
        return new ConditionP(Condition.RelationType.CONTAINS_VALUE, value);
    }

    public static ConditionP eq(final Object value) {
        return new ConditionP(Condition.RelationType.EQ, value);
    }
}
