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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.datastax.driver.core.querybuilder;

import java.util.List;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.querybuilder.Clause.ContainsClause;
import com.datastax.driver.core.querybuilder.Clause.ContainsKeyClause;

public class Clauses {

    public static boolean needAllowFiltering(Clause clause) {
        return ContainsKeyClause.class.isInstance(clause) ||
               ContainsClause.class.isInstance(clause);
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
            if (Utils.containsBindMarker(this.left) ||
                Utils.containsBindMarker(this.right)) {
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
