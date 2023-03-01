/*
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

package org.apache.hugegraph.election;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.auth.SchemaDefine;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

public class StandardRoleTypeDataAdapter implements RoleTypeDataAdapter {

    private final HugeGraphParams graphParams;
    private final Schema schema;

    private static final Logger LOG = Log.logger(StandardRoleTypeDataAdapter.class);

    private boolean first;

    public StandardRoleTypeDataAdapter(HugeGraphParams graph) {
        this.graphParams = graph;
        this.schema = new Schema(graph);
        this.schema.initSchemaIfNeeded();
        this.first = true;
    }

    @Override
    public boolean updateIfNodePresent(RoleTypeData stateData) {
        // if epoch increase, update and return true
        // if epoch equal, ignore different node, return false
        Optional<Vertex> oldTypeDataOpt = this.queryVertex();
        if (oldTypeDataOpt.isPresent()) {
            RoleTypeData oldTypeData = this.from(oldTypeDataOpt.get());
            if (stateData.epoch() < oldTypeData.epoch()) {
                return false;
            }

            if (stateData.epoch() == oldTypeData.epoch() &&
                !Objects.equals(stateData.node(), oldTypeData.node())) {
                return false;
            }
            LOG.trace("Server {} epoch {} begin remove data old epoch {}, ", stateData.node(), stateData.epoch(), oldTypeData.epoch());
            this.graphParams.systemTransaction().removeVertex((HugeVertex) oldTypeDataOpt.get());
            this.graphParams.systemTransaction().commitOrRollback();
            LOG.trace("Server {} epoch {} success remove data old epoch {}, ", stateData.node(), stateData.epoch(), oldTypeData.epoch());
        }
        try {
            GraphTransaction tx = this.graphParams.systemTransaction();
            tx.doUpdateIfAbsent(this.constructEntry(stateData));
            tx.commitOrRollback();
            LOG.trace("Server {} epoch {} success update data", stateData.node(), stateData.epoch());
        } catch (Throwable ignore){
            LOG.trace("Server {} epoch {} fail update data", stateData.node(), stateData.epoch());
            return false;
        }

        return true;
    }

    private BackendEntry constructEntry(RoleTypeData stateData) {
        List<Object> list = new ArrayList<>(8);
        list.add(T.label);
        list.add(P.ROLE_DATA);

        list.add(P.NODE);
        list.add(stateData.node());

        list.add(P.URL);
        list.add(stateData.url());

        list.add(P.CLOCK);
        list.add(stateData.clock());

        list.add(P.EPOCH);
        list.add(stateData.epoch());

        list.add(P.TYPE);
        list.add("default");

        HugeVertex vertex = this.graphParams.systemTransaction()
                                            .constructVertex(false, list.toArray());

        return this.graphParams.serializer().writeVertex(vertex);
    }

    @Override
    public Optional<RoleTypeData> query() {
        Optional<Vertex> vertex = this.queryVertex();
        if (!vertex.isPresent() && !this.first) {
            // If query nothing, retry once
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignore) {
            }

            vertex = this.queryVertex();
        }
        this.first = false;
        return vertex.map(this::from);
    }

    private RoleTypeData from(Vertex vertex) {
        String node = (String) vertex.property(P.NODE).value();
        String url = (String) vertex.property(P.URL).value();
        Long clock = (Long) vertex.property(P.CLOCK).value();
        Integer epoch = (Integer) vertex.property(P.EPOCH).value();

        RoleTypeData roleTypeData = new RoleTypeData(node, url, epoch, clock);
        return roleTypeData;
    }

    private Optional<Vertex> queryVertex() {
        GraphTransaction tx = this.graphParams.systemTransaction();
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        VertexLabel vl = this.graphParams.graph().vertexLabel(P.ROLE_DATA);
        query.eq(HugeKeys.LABEL, vl.id());
        query.query(Condition.eq(vl.primaryKeys().get(0), "default"));
        query.showHidden(true);
        Iterator<Vertex> vertexIterator = tx.queryVertices(query);
        if (vertexIterator.hasNext()) {
            return Optional.of(vertexIterator.next());
        }

        return Optional.empty();
    }

    public static final class P {

        public static final String ROLE_DATA = Graph.Hidden.hide("role_data");

        public static final String LABEL = T.label.getAccessor();

        public static final String NODE = Graph.Hidden.hide("role_node");

        public static final String CLOCK = Graph.Hidden.hide("role_clock");

        public static final String EPOCH = Graph.Hidden.hide("role_epoch");

        public static final String URL = Graph.Hidden.hide("role_url");

        public static final String TYPE = Graph.Hidden.hide("role_type");
    }


    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.ROLE_DATA);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            VertexLabel label = this.schema().vertexLabel(this.label)
                                             .enableLabelIndex(true)
                                             .usePrimaryKeyId()
                                             .primaryKeys(P.TYPE)
                                             .properties(properties)
                                             .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NODE, DataType.TEXT));
            props.add(createPropertyKey(P.URL, DataType.TEXT));
            props.add(createPropertyKey(P.CLOCK, DataType.LONG));
            props.add(createPropertyKey(P.EPOCH, DataType.INT));
            props.add(createPropertyKey(P.TYPE, DataType.TEXT));

            return super.initProperties(props);
        }
    }
}
