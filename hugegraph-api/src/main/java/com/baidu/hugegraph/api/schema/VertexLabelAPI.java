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
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

@Path("graphs/{graph}/schema/vertexlabels")
@Singleton
public class VertexLabelAPI extends API {

    private static final Logger LOG = Log.logger(VertexLabelAPI.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertexLabel jsonVertexLabel) {
        E.checkArgumentNotNull(jsonVertexLabel, "The request json body to " +
                               "create VertexLabel can't be null or empty");

        LOG.debug("Graph [{}] create vertex label: {}",
                  graph, jsonVertexLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        VertexLabel vertexLabel = jsonVertexLabel.convert2VertexLabel();
        vertexLabel = g.schema().vertexLabel(vertexLabel).create();

        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @PUT
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @QueryParam("action") String action,
                         JsonVertexLabel jsonVertexLabel) {
        E.checkArgumentNotNull(jsonVertexLabel, "The request json body to " +
                               "update VertexLabel can't be null or empty");

        LOG.debug("Graph [{}] %s vertex label: {}",
                  graph, action, jsonVertexLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        VertexLabel vertexLabel = jsonVertexLabel.convert2VertexLabel();
        if (action.equals("append")) {
            vertexLabel = g.schema().vertexLabel(vertexLabel).append();
        } else if (action.equals("eliminate")) {
            vertexLabel = g.schema().vertexLabel(vertexLabel).eliminate();
        } else {
            throw new NotSupportedException(
                      String.format("Not support action '%s'", action));
        }
        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get vertex labels", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<VertexLabel> labels = g.schemaTransaction().getVertexLabels();

        return manager.serializer(g).writeVertexLabels(labels);
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get vertex label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        VertexLabel vertexLabel = g.schemaTransaction().getVertexLabel(name);
        checkExists(HugeType.VERTEX_LABEL, vertexLabel, name);
        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove vertex label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schemaTransaction().removeVertexLabel(name);
    }

    private static class JsonVertexLabel {

        public String name;
        public IdStrategy idStrategy;
        public String[] primaryKeys;
        public String[] nullableKeys;
        public String[] indexNames;
        public String[] properties;
        public boolean checkExist;

        @Override
        public String toString() {
            return String.format("JsonVertexLabel{" +
                   "name=%s, idStrategy=%s, primaryKeys=%s, " +
                   "nullableKeys=%s, indexNames=%s, properties=%s}",
                   this.name, this.idStrategy, this.primaryKeys,
                   this.nullableKeys, this.indexNames, this.properties);
        }

        public VertexLabel convert2VertexLabel() {
            VertexLabel vertexLabel = new VertexLabel(this.name);
            vertexLabel.idStrategy(this.idStrategy);
            vertexLabel.primaryKeys(this.primaryKeys);
            vertexLabel.nullableKeys(this.nullableKeys);
            vertexLabel.indexNames(this.indexNames);
            vertexLabel.properties(this.properties);
            vertexLabel.checkExist(this.checkExist);
            return vertexLabel;
        }
    }
}
