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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
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

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.Userdata;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/schema/edgelabels")
@Singleton
public class EdgeLabelAPI extends API {

    private static final Logger LOG = Log.logger(EdgeLabelAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_write"})
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
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_write"})
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
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        boolean listAll = CollectionUtils.isEmpty(names);
        if (listAll) {
            LOG.debug("Graph [{}] list edge labels", graph);
        } else {
            LOG.debug("Graph [{}] get edge labels by names {}", graph, names);
        }

        HugeGraph g = graph(manager, graph);
        List<EdgeLabel> labels;
        if (listAll) {
            labels = g.schema().getEdgeLabels();
        } else {
            labels = new ArrayList<>(names.size());
            for (String name : names) {
                labels.add(g.schema().getEdgeLabel(name));
            }
        }
        return manager.serializer(g).writeEdgeLabels(labels);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_read"})
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        EdgeLabel edgeLabel = g.schema().getEdgeLabel(name);
        return manager.serializer(g).writeEdgeLabel(edgeLabel);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_delete"})
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        // Throw 404 if not exists
        g.schema().getEdgeLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().edgeLabel(name).remove());
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
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("sort_keys")
        public String[] sortKeys;
        @JsonProperty("nullable_keys")
        public String[] nullableKeys;
        @JsonProperty("enable_label_index")
        public Boolean enableLabelIndex;
        @JsonProperty("user_data")
        public Userdata userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of edge label can't be null");
        }

        private EdgeLabel.Builder convert2Builder(HugeGraph g) {
            EdgeLabel.Builder builder = g.schema().edgeLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "edge label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept edge label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.sourceLabel != null) {
                builder.sourceLabel(this.sourceLabel);
            }
            if (this.targetLabel != null) {
                builder.targetLabel(this.targetLabel);
            }
            if (this.frequency != null) {
                builder.frequency(this.frequency);
            }
            if (this.properties != null) {
                builder.properties(this.properties);
            }
            if (this.sortKeys != null) {
                builder.sortKeys(this.sortKeys);
            }
            if (this.nullableKeys != null) {
                builder.nullableKeys(this.nullableKeys);
            }
            if (this.enableLabelIndex != null) {
                builder.enableLabelIndex(this.enableLabelIndex);
            }
            if (this.userdata != null) {
                builder.userdata(this.userdata);
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
