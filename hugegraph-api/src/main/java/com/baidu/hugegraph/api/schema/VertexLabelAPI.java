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
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/schema/vertexlabels")
@Singleton
public class VertexLabelAPI extends API {

    private static final Logger LOG = Log.logger(VertexLabelAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertexLabel jsonVertexLabel) {
        LOG.debug("Graph [{}] create vertex label: {}",
                  graph, jsonVertexLabel);
        checkCreatingBody(jsonVertexLabel);

        HugeGraph g = graph(manager, graph);
        VertexLabel.Builder builder = jsonVertexLabel.convert2Builder(g);
        VertexLabel vertexLabel = builder.create();
        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("name") String name,
                         @QueryParam("action") String action,
                         JsonVertexLabel jsonVertexLabel) {
        LOG.debug("Graph [{}] {} vertex label: {}",
                  graph, action, jsonVertexLabel);

        checkUpdatingBody(jsonVertexLabel);
        E.checkArgument(name.equals(jsonVertexLabel.name),
                        "The name in url(%s) and body(%s) are different",
                        name, jsonVertexLabel.name);
        // Parse action parameter
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graph);
        VertexLabel.Builder builder = jsonVertexLabel.convert2Builder(g);
        VertexLabel vertexLabel = append ?
                                  builder.append() :
                                  builder.eliminate();
        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("names") List<String> names) {
        if (CollectionUtils.isEmpty(names)) {
            LOG.debug("Graph [{}] list vertex labels", graph);
        } else {
            LOG.debug("Graph [{}] get vertex labels by names {}", graph, names);
        }

        HugeGraph g = graph(manager, graph);
        List<VertexLabel> labels;
        if (CollectionUtils.isEmpty(names)) {
            labels = g.schema().getVertexLabels();
        } else {
            labels = new ArrayList<>(names.size());
            for (String name : names) {
                labels.add(g.schema().getVertexLabel(name));
            }
        }
        return manager.serializer(g).writeVertexLabels(labels);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get vertex label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        VertexLabel vertexLabel = g.schema().getVertexLabel(name);
        return manager.serializer(g).writeVertexLabel(vertexLabel);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Id> delete(@Context GraphManager manager,
                                  @PathParam("graph") String graph,
                                  @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove vertex label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        // Throw 404 if not exists
        g.schema().getVertexLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().vertexLabel(name).remove());
    }

    /**
     * JsonVertexLabel is only used to receive create and append requests
     */
    private static class JsonVertexLabel implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("id_strategy")
        public IdStrategy idStrategy;
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("primary_keys")
        public String[] primaryKeys;
        @JsonProperty("nullable_keys")
        public String[] nullableKeys;
        @JsonProperty("enable_label_index")
        public Boolean enableLabelIndex;
        @JsonProperty("user_data")
        public Map<String, Object> userdata;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of vertex label can't be null");
        }

        private VertexLabel.Builder convert2Builder(HugeGraph g) {
            VertexLabel.Builder builder = g.schema().vertexLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "vertex label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept vertex label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            if (this.idStrategy != null) {
                builder.idStrategy(this.idStrategy);
            }
            if (this.properties != null) {
                builder.properties(this.properties);
            }
            if (this.primaryKeys != null) {
                builder.primaryKeys(this.primaryKeys);
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
            return String.format("JsonVertexLabel{" +
                   "name=%s, idStrategy=%s, primaryKeys=%s, " +
                   "nullableKeys=%s, properties=%s}",
                   this.name, this.idStrategy, this.primaryKeys,
                   this.nullableKeys, this.properties);
        }
    }
}
