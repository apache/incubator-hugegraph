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

package com.baidu.hugegraph.api.traversers;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PAGE_LIMIT;

import java.util.Iterator;
import java.util.List;

import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/vertices")
@Singleton
public class VerticesAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("ids") List<String> stringIds) {
        LOG.debug("Graph [{}] get vertices by ids: {}", graph, stringIds);

        E.checkArgument(stringIds != null && !stringIds.isEmpty(),
                        "The ids parameter can't be null or empty");

        Object[] ids = new Id[stringIds.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = VertexAPI.checkAndParseVertexId(stringIds.get(i));
        }

        HugeGraph g = graph(manager, graph);

        Iterator<Vertex> vertices = g.vertices(ids);
        return manager.serializer(g).writeVertices(vertices, false);
    }

    @GET
    @Timed
    @Path("shards")
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String shards(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @QueryParam("split_size") long splitSize) {
        LOG.debug("Graph [{}] get vertex shards with split size '{}'",
                  graph, splitSize);

        HugeGraph g = graph(manager, graph);
        List<Shard> shards = g.metadata(HugeType.VERTEX, "splits", splitSize);
        return manager.serializer(g).writeList("shards", shards);
    }

    @GET
    @Timed
    @Path("scan")
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String scan(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("start") String start,
                       @QueryParam("end") String end,
                       @QueryParam("page") String page,
                       @QueryParam("page_limit")
                       @DefaultValue(DEFAULT_PAGE_LIMIT) long pageLimit) {
        LOG.debug("Graph [{}] query vertices by shard(start: {}, end: {}, " +
                  "page: {}) ", graph, start, end, page);

        HugeGraph g = graph(manager, graph);

        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.scan(start, end);
        query.page(page);
        if (query.paging()) {
            query.limit(pageLimit);
        }
        Iterator<Vertex> vertices = g.vertices(query);

        return manager.serializer(g).writeVertices(vertices, query.paging());
    }
}
