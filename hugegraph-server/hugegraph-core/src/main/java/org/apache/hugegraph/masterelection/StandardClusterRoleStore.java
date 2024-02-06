/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.masterelection;

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

public class StandardClusterRoleStore implements ClusterRoleStore {

    private static final Logger LOG = Log.logger(StandardClusterRoleStore.class);
    private static final int RETRY_QUERY_TIMEOUT = 200;

    private final HugeGraphParams graph;

    private boolean firstTime;

    public StandardClusterRoleStore(HugeGraphParams graph) {
        this.graph = graph;
        Schema schema = new Schema(graph);
        schema.initSchemaIfNeeded();
        this.firstTime = true;
    }

    @Override
    public boolean updateIfNodePresent(ClusterRole clusterRole) {
        // if epoch increase, update and return true
        // if epoch equal, ignore different node, return false
        Optional<Vertex> oldClusterRoleOpt = this.queryVertex();
        if (oldClusterRoleOpt.isPresent()) {
            ClusterRole oldClusterRole = this.from(oldClusterRoleOpt.get());
            if (clusterRole.epoch() < oldClusterRole.epoch()) {
                return false;
            }

            if (clusterRole.epoch() == oldClusterRole.epoch() &&
                !Objects.equals(clusterRole.node(), oldClusterRole.node())) {
                return false;
            }
            LOG.trace("Server {} epoch {} begin remove data old epoch {}, ",
                      clusterRole.node(), clusterRole.epoch(), oldClusterRole.epoch());
            this.graph.systemTransaction().removeVertex((HugeVertex) oldClusterRoleOpt.get());
            this.graph.systemTransaction().commitOrRollback();
            LOG.trace("Server {} epoch {} success remove data old epoch {}, ",
                      clusterRole.node(), clusterRole.epoch(), oldClusterRole.epoch());
        }
        try {
            GraphTransaction tx = this.graph.systemTransaction();
            tx.doUpdateIfAbsent(this.constructEntry(clusterRole));
            tx.commitOrRollback();
            LOG.trace("Server {} epoch {} success update data",
                      clusterRole.node(), clusterRole.epoch());
        } catch (Throwable ignore) {
            LOG.trace("Server {} epoch {} fail update data",
                      clusterRole.node(), clusterRole.epoch());
            return false;
        }

        return true;
    }

    private BackendEntry constructEntry(ClusterRole clusterRole) {
        List<Object> list = new ArrayList<>(8);
        list.add(T.label);
        list.add(P.ROLE_DATA);

        list.add(P.NODE);
        list.add(clusterRole.node());

        list.add(P.URL);
        list.add(clusterRole.url());

        list.add(P.CLOCK);
        list.add(clusterRole.clock());

        list.add(P.EPOCH);
        list.add(clusterRole.epoch());

        list.add(P.TYPE);
        list.add("default");

        HugeVertex vertex = this.graph.systemTransaction()
                                      .constructVertex(false, list.toArray());

        return this.graph.serializer().writeVertex(vertex);
    }

    @Override
    public Optional<ClusterRole> query() {
        Optional<Vertex> vertex = this.queryVertex();
        if (!vertex.isPresent() && !this.firstTime) {
            // If query nothing, retry once
            try {
                Thread.sleep(RETRY_QUERY_TIMEOUT);
            } catch (InterruptedException ignored) {
            }

            vertex = this.queryVertex();
        }
        this.firstTime = false;
        return vertex.map(this::from);
    }

    private ClusterRole from(Vertex vertex) {
        String node = (String) vertex.property(P.NODE).value();
        String url = (String) vertex.property(P.URL).value();
        Long clock = (Long) vertex.property(P.CLOCK).value();
        Integer epoch = (Integer) vertex.property(P.EPOCH).value();

        return new ClusterRole(node, url, epoch, clock);
    }

    private Optional<Vertex> queryVertex() {
        GraphTransaction tx = this.graph.systemTransaction();
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        VertexLabel vl = this.graph.graph().vertexLabel(P.ROLE_DATA);
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

            VertexLabel label = this.schema()
                                    .vertexLabel(this.label)
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
