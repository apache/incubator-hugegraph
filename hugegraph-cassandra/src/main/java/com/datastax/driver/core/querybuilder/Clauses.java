package com.datastax.driver.core.querybuilder;

import java.util.List;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.querybuilder.Clause.ContainsClause;
import com.datastax.driver.core.querybuilder.Clause.ContainsKeyClause;

public class Clauses {

    public static boolean needAllowFiltering(Clause clause) {
        return (ContainsKeyClause.class.isInstance(clause)
                || ContainsClause.class.isInstance(clause));
    }

    public static AndClause and(Clause left, Clause right) {
        return new AndClause(left, right);
    }

    static class BinClause extends Clause {

        private Clause left;
        private String op;
        private Clause right;

        public BinClause(Clause left, String op, Clause right) {
            this.left = left;
            this.op = op;
            this.right = right;
        }

        @Override
        String name() {
            return null;
        }

        @Override
        Object firstValue() {
            return null;
        }

        @Override
        boolean containsBindMarker() {
            if (Utils.containsBindMarker(this.left)
                    || Utils.containsBindMarker(this.right)) {
                return true;
            }
            return false;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables,
                CodecRegistry codecRegistry) {
            // NOTE: '(? AND ?)' is not supported by Cassandra:
            // SyntaxError: line xx missing ')' at 'AND'
            // sb.append("(");
            this.left.appendTo(sb, variables, codecRegistry);
            sb.append(" ");
            sb.append(this.op);
            sb.append(" ");
            this.right.appendTo(sb, variables, codecRegistry);
            // sb.append(")");
        }
    }

    static class AndClause extends BinClause {
        public AndClause(Clause left, Clause right) {
            super(left, "AND",  right);
        }
    }
}
