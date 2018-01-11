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
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.annotation.JsonProperty;

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
        LOG.debug("Graph [{}] create edge label: {}", graph, jsonEdgeLabel);
        checkCreatingBody(jsonEdgeLabel);

        HugeGraph g = graph(manager, graph);
        EdgeLabel.Builder builder = jsonEdgeLabel.convert2Builder(g);
        EdgeLabel edgeLabel = builder.create();
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @PUT
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         JsonEdgeLabel jsonEdgeLabel) {
        LOG.debug("Graph [{}] {} edge label: {}",
                  graph, action, jsonEdgeLabel);
        checkUpdatingBody(jsonEdgeLabel);
        E.checkArgument(name.equals(jsonEdgeLabel.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonEdgeLabel.name);
        // Parse action param
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graph);
        EdgeLabel.Builder builder = jsonEdgeLabel.convert2Builder(g);
        EdgeLabel edgeLabel = append ? builder.append() : builder.eliminate();
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get edge labels", graph);

        HugeGraph g = graph(manager, graph);
        List<EdgeLabel> labels = g.schema().getEdgeLabels();
        return manager.serializer(g).writeEdgeLabels(labels);
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        EdgeLabel edgeLabel = g.schema().getEdgeLabel(name);
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        // Just check exists
        g.schema().getEdgeLabel(name);
        g.schema().edgeLabel(name).remove();
    }

    /**
     * JsonEdgeLabel is only used to receive create and append requests
     */
    private static class JsonEdgeLabel implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("source_label")
        public String sourceLabel;
        @JsonProperty("target_label")
        public String targetLabel;
        @JsonProperty("frequency")
        public Frequency frequency;
        @JsonProperty("sort_keys")
        public String[] sortKeys;
        @JsonProperty("nullable_keys")
        public String[] nullableKeys;
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("user_data")
        public Map<String, Object> userData;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of edge label can't be null");
        }

        private EdgeLabel.Builder convert2Builder(HugeGraph g) {
            EdgeLabel.Builder builder = g.schema().edgeLabel(this.name);
            if (this.sourceLabel != null) {
                builder.sourceLabel(this.sourceLabel);
            }
            if (this.targetLabel != null) {
                builder.targetLabel(this.targetLabel);
            }
            if (this.frequency != null) {
                builder.frequency(this.frequency);
            }
            if (this.sortKeys != null) {
                builder.sortKeys(this.sortKeys);
            }
            if (this.nullableKeys != null) {
                builder.nullableKeys(this.nullableKeys);
            }
            if (this.properties != null) {
                builder.properties(this.properties);
            }
            if (this.userData != null) {
                builder.userData(this.userData);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonEdgeLabel{" +
                   "name=%s, sourceLabel=%s, targetLabel=%s, frequency=%s, " +
                   "sortKeys=%s, nullableKeys=%s, properties=%s}",
                   this.name, this.sourceLabel, this.targetLabel,
                   this.frequency, this.sortKeys, this.nullableKeys,
                   this.properties);
        }
    }
}
