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

package com.baidu.hugegraph.core;

import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public class FakeObjects {

    public static class FakeVertex {

        private String label;
        private Map<String, Object> values;

        public FakeVertex(Object... keyValues) {
            this.label = ElementHelper.getLabelValue(keyValues).get();
            this.values = ElementHelper.asMap(keyValues);

            this.values.remove("label");
        }

        public boolean equalsVertex(Vertex v) {
            if (!v.label().equals(this.label)) {
                return false;
            }
            for (Map.Entry<String, Object> i : this.values.entrySet()) {
                if (!v.property(i.getKey()).value().equals(i.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class FakeEdge {

        private String label;
        private Vertex outVertex;
        private Vertex inVertex;
        private Map<String, Object> values;

        public FakeEdge(String label, Vertex outVertex, Vertex inVertex,
                Object... keyValues) {
            this.label = label;
            this.outVertex = outVertex;
            this.inVertex = inVertex;
            this.values = ElementHelper.asMap(keyValues);
        }

        public boolean equalsEdge(Edge e) {
            if (!e.label().equals(this.label)
                    || !e.outVertex().id().equals(this.outVertex.id())
                    || !e.inVertex().id().equals(this.inVertex.id())) {
                return false;
            }
            for (Map.Entry<String, Object> i : this.values.entrySet()) {
                if (!e.property(i.getKey()).value().equals(i.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }
}
