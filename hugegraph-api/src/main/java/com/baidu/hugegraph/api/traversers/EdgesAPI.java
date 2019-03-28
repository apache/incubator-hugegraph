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

import java.util.Iterator;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/edges")
@Singleton
public class EdgesAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("ids") List<String> stringIds) {
        LOG.debug("Graph [{}] get edges by ids: {}", graph, stringIds);

        E.checkArgument(stringIds != null && !stringIds.isEmpty(),
                        "The ids parameter can't be null or empty");

        Object[] ids = new Id[stringIds.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = HugeEdge.getIdValue(stringIds.get(i));
        }

        HugeGraph g = graph(manager, graph);

        Iterator<Edge> edges = g.edges(ids);
        return manager.serializer(g).writeEdges(edges, false);
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
        List<Shard> shards = g.graphTransaction()
                              .metadata(HugeType.EDGE_OUT, "splits", splitSize);
        return manager.serializer(g).writeShards(shards);
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
                       @QueryParam("page") String page) {
        LOG.debug("Graph [{}] query edges by shard(start: {}, end: {}, " +
                  "page: {}) ", graph, start, end, page);

        HugeGraph g = graph(manager, graph);

        ConditionQuery query = new ConditionQuery(HugeType.EDGE_OUT);
        query.scan(start, end);
        query.page(page);
        if (query.paging()) {
            query.limit(Query.DEFAULT_CAPACITY);
        }
        Iterator<Edge> edges = g.edges(query);

        return manager.serializer(g).writeEdges(edges, query.paging());
    }
}
