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
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.api.schema;

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

@Path("graphs/{graph}/schema/edgelabels")
@Singleton
public class EdgeLabelAPI extends API {

    private static final Logger LOG = Log.logger(EdgeLabelAPI.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonEdgeLabel jsonEdgeLabel) {
        E.checkArgumentNotNull(jsonEdgeLabel, "The request json body to " +
                               "create EdgeLabel can't be null or empty");

        LOG.debug("Graph [{}] create edge label: {}", graph, jsonEdgeLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        EdgeLabel edgeLabel = jsonEdgeLabel.convert2EdgeLabel();
        edgeLabel = g.schema().edgeLabel(edgeLabel).create();

        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @PUT
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @QueryParam("action") String action,
                         JsonEdgeLabel jsonEdgeLabel) {
        E.checkArgumentNotNull(jsonEdgeLabel, "The request json body to " +
                               "create EdgeLabel can't be null or empty");

        LOG.debug("Graph [{}] %s edge label: {}",
                  graph, action, jsonEdgeLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        EdgeLabel edgeLabel = jsonEdgeLabel.convert2EdgeLabel();
        if (action.equals("append")) {
            edgeLabel = g.schema().edgeLabel(edgeLabel).append();
        } else if (action.equals("eliminate")) {
            edgeLabel = g.schema().edgeLabel(edgeLabel).eliminate();
        } else {
            throw new NotSupportedException(
                      String.format("Not support action '%s'", action));
        }
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get edge labels", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<EdgeLabel> labels = g.schemaTransaction().getEdgeLabels();

        return manager.serializer(g).writeEdgeLabels(labels);
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        EdgeLabel edgeLabel = g.schemaTransaction().getEdgeLabel(name);
        checkExists(HugeType.EDGE_LABEL, edgeLabel, name);
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove edge label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schemaTransaction().removeEdgeLabel(name);
    }

    private static class JsonEdgeLabel {

        public String name;
        public String sourceLabel;
        public String targetLabel;
        public Frequency frequency;
        public String[] sortKeys;
        public String[] nullableKeys;
        public String[] indexNames;
        public String[] properties;
        public boolean checkExist;

        @Override
        public String toString() {
            return String.format("JsonEdgeLabel{" +
                   "name=%s, sourceLabel=%s, targetLabel=%s, frequency=%s, " +
                   "sortKeys=%s, nullableKeys=%s, indexNames=%s, " +
                   "properties=%s}",
                   this.name, this.sourceLabel, this.targetLabel,
                   this.frequency, this.sortKeys, this.nullableKeys,
                   this.indexNames, this.properties);
        }

        public EdgeLabel convert2EdgeLabel() {
            EdgeLabel edgeLabel = new EdgeLabel(this.name);
            edgeLabel.sourceLabel(this.sourceLabel);
            edgeLabel.targetLabel(this.targetLabel);
            edgeLabel.frequency(this.frequency);
            edgeLabel.sortKeys(this.sortKeys);
            edgeLabel.nullableKeys(this.nullableKeys);
            edgeLabel.indexNames(this.indexNames);
            edgeLabel.properties(this.properties);
            edgeLabel.checkExist(this.checkExist);
            return edgeLabel;
        }
    }
}
