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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/schema/indexlabels")
@Singleton
public class IndexLabelAPI extends API {

    private static final Logger LOG = Log.logger(IndexLabelAPI.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonIndexLabel jsonIndexLabel) {
        LOG.debug("Graph [{}] create index label: {}", graph, jsonIndexLabel);
        checkCreatingBody(jsonIndexLabel);

        HugeGraph g = graph(manager, graph);
        IndexLabel.Builder builder = jsonIndexLabel.convert2Builder(g);
        IndexLabel indexLabel = builder.create();
        return manager.serializer(g).writeIndexlabel(indexLabel);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get edge labels", graph);

        HugeGraph g = graph(manager, graph);
        List<IndexLabel> labels = g.schema().getIndexLabels();
        return manager.serializer(g).writeIndexlabels(labels);
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get edge label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        IndexLabel indexLabel = g.schema().getIndexLabel(name);
        return manager.serializer(g).writeIndexlabel(indexLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove index label by name '{}'", graph, name);

        HugeGraph g = graph(manager, graph);
        // Throw 404 if not exists
        g.schema().getIndexLabel(name);
        g.schema().indexLabel(name).remove();
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

        private IndexLabel.Builder convert2Builder(HugeGraph g) {
            IndexLabel.Builder builder = g.schema().indexLabel(this.name);
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
