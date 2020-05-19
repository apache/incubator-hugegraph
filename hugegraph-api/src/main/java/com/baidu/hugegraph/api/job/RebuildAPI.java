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

package com.baidu.hugegraph.api.job;

import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/jobs/rebuild")
@Singleton
public class RebuildAPI extends API {

    private static final Logger LOG = Log.logger(RebuildAPI.class);

    @PUT
    @Timed
    @Path("vertexlabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_write"})
    public Map<String, Id> vertexLabelRebuild(@Context GraphManager manager,
                                              @PathParam("graph") String graph,
                                              @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild vertex label: {}", graph, name);

        HugeGraph g = graph(manager, graph);
        return ImmutableMap.of("task_id",
                               g.schema().vertexLabel(name).rebuildIndex());
    }

    @PUT
    @Timed
    @Path("edgelabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_write"})
    public Map<String, Id> edgeLabelRebuild(@Context GraphManager manager,
                                            @PathParam("graph") String graph,
                                            @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild edge label: {}", graph, name);

        HugeGraph g = graph(manager, graph);
        return ImmutableMap.of("task_id",
                               g.schema().edgeLabel(name).rebuildIndex());
    }

    @PUT
    @Timed
    @Path("indexlabels/{name}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=index_write"})
    public Map<String, Id> indexLabelRebuild(@Context GraphManager manager,
                                             @PathParam("graph") String graph,
                                             @PathParam("name") String name) {
        LOG.debug("Graph [{}] rebuild index label: {}", graph, name);

        HugeGraph g = graph(manager, graph);
        return ImmutableMap.of("task_id",
                               g.schema().indexLabel(name).rebuild());
    }
}