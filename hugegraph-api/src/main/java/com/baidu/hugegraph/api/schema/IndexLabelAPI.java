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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.GremlinGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/schema/indexlabels")
@Singleton
public class IndexLabelAPI extends API {

    private static final Logger LOG = Log.logger(IndexLabelAPI.class);

    @POST
    @Timed
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_write"})
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonIndexLabel jsonIndexLabel) {
        LOG.debug("Graph [{}] create index label: {}", graph, jsonIndexLabel);
        checkCreatingBody(jsonIndexLabel);

        GremlinGraph g = graph(manager, graph);
        IndexLabel.Builder builder = jsonIndexLabel.convert2Builder(g);
        IndexLabel.CreatedIndexLabel il = builder.createWithTask();
        il.indexLabel(mapIndexLabel(il.indexLabel()));
        return manager.serializer(g).writeCreatedIndexLabel(il);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=graph $action=schema_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get edge labels", graph);

        GremlinGraph g = graph(manager, graph);
        List<IndexLabel> labels = g.schema().getIndexLabels();
        return manager.serializer(g).writeIndexlabels(mapIndexLabels(labels));
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

        GremlinGraph g = graph(manager, graph);
        IndexLabel indexLabel = g.schema().getIndexLabel(name);
        return manager.serializer(g).writeIndexlabel(mapIndexLabel(indexLabel));
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
        LOG.debug("Graph [{}] remove index label by name '{}'", graph, name);

        GremlinGraph g = graph(manager, graph);
        // Throw 404 if not exists
        g.schema().getIndexLabel(name);
        return ImmutableMap.of("task_id",
                               g.schema().indexLabel(name).remove());
    }

    private static List<IndexLabel> mapIndexLabels(List<IndexLabel> labels) {
        List<IndexLabel> results = new ArrayList<>(labels.size());
        for (IndexLabel il : labels) {
            results.add(mapIndexLabel(il));
        }
        return results;
    }

    /**
     * Map RANGE_INT/RANGE_FLOAT/RANGE_LONG/RANGE_DOUBLE to RANGE
     */
    private static IndexLabel mapIndexLabel(IndexLabel label) {
        if (label.indexType().isRange()) {
            label = (IndexLabel) label.copy();
            label.indexType(IndexType.RANGE);
        }
        return label;
    }

    /**
     * JsonIndexLabel is only used to receive create and append requests
     */
    private static class JsonIndexLabel implements Checkable {

        @JsonProperty("id")
        public long id;
        @JsonProperty("name")
        public String name;
        @JsonProperty("base_type")
        public HugeType baseType;
        @JsonProperty("base_value")
        public String baseValue;
        @JsonProperty("index_type")
        public IndexType indexType;
        @JsonProperty("fields")
        public String[] fields;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of index label can't be null");
            E.checkArgumentNotNull(this.baseType,
                                   "The base type of index label '%s' " +
                                   "can't be null", this.name);
            E.checkArgument(this.baseType == HugeType.VERTEX_LABEL ||
                            this.baseType == HugeType.EDGE_LABEL,
                            "The base type of index label '%s' can only be " +
                            "either VERTEX_LABEL or EDGE_LABEL", this.name);
            E.checkArgumentNotNull(this.baseValue,
                                   "The base value of index label '%s' " +
                                   "can't be null", this.name);
        }

        private IndexLabel.Builder convert2Builder(GremlinGraph g) {
            IndexLabel.Builder builder = g.schema().indexLabel(this.name);
            if (this.id != 0) {
                E.checkArgument(this.id > 0,
                                "Only positive number can be assign as " +
                                "index label id");
                E.checkArgument(g.mode() == GraphMode.RESTORING,
                                "Only accept index label id when graph in " +
                                "RESTORING mode, but '%s' is in mode '%s'",
                                g, g.mode());
                builder.id(this.id);
            }
            builder.on(this.baseType, this.baseValue);
            if (this.indexType != null) {
                builder.indexType(this.indexType);
            }
            if (this.fields != null) {
                builder.by(this.fields);
            }
            if (this.checkExist != null) {
                builder.checkExist(this.checkExist);
            }
            return builder;
        }

        @Override
        public String toString() {
            return String.format("JsonIndexLabel{name=%s, baseType=%s," +
                                 "baseValue=%s, indexType=%s, fields=%s}",
                                 this.name, this.baseType, this.baseValue,
                                 this.indexType, this.fields);
        }
    }
}
