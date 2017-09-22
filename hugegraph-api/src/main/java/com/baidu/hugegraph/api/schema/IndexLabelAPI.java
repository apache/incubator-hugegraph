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
                         IndexLabelAPI.JsonIndexLabel jsonIndexLabel) {
        E.checkArgumentNotNull(jsonIndexLabel, "The request json body to " +
                               "create IndexLabel can't be null or empty");

        LOG.debug("Graph [{}] create index label: {}", graph, jsonIndexLabel);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        IndexLabel indexLabel = jsonIndexLabel.convert2IndexLabel();
        indexLabel = g.schema().indexLabel(indexLabel).create();

        return manager.serializer(g).writeIndexlabel(indexLabel);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get edge labels", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
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

        HugeGraph g = (HugeGraph) graph(manager, graph);
        IndexLabel indexLabel = g.schema().getIndexLabel(name);
        checkExists(HugeType.INDEX_LABEL, indexLabel, name);
        return manager.serializer(g).writeIndexlabel(indexLabel);
    }

    @DELETE
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove index label by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        g.schema().indexLabel(name).remove();
    }

    private static class JsonIndexLabel {

        public String name;
        public HugeType baseType;
        public String baseValue;
        public IndexType indexType;
        public String[] fields;
        public Boolean checkExist;

        private IndexLabel convert2IndexLabel() {
            E.checkArgumentNotNull(this.name, "The name of index label " +
                                   "can't be null");
            IndexLabel indexLabel = new IndexLabel(this.name);
            if (this.baseType != null) {
                indexLabel.baseType(this.baseType);
            }
            if (this.baseValue != null) {
                indexLabel.baseValue(this.baseValue);
            }
            if (this.indexType != null) {
                indexLabel.indexType(this.indexType);
            }
            if (this.fields != null) {
                indexLabel.indexFields(this.fields);
            }
            if (this.checkExist != null) {
                indexLabel.checkExist(this.checkExist);
            }
            return indexLabel;
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
