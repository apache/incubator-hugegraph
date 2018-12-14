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

import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.RankAlgorithm;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/traversers/neighborrank")
@Singleton
public class NeighborRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String neighborRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               @QueryParam("source") String source,
                               @QueryParam("label_dirs") String labelDirs,
                               @QueryParam("alpha") double alpha,
                               @QueryParam("max_depth") int depth,
                               @QueryParam("sort_result") @DefaultValue("true")
                               boolean sortResult) {
        LOG.debug("Graph [{}] get neighbor rank from '{}' with " +
                  "edge label '{}', alpha '{}', max depth '{}'",
                  graph, source, labelDirs, alpha, depth);

        E.checkNotNull(source, "source vertex id");
        E.checkNotNull(labelDirs, "label dirs");
        E.checkArgument(alpha >= 0.0 && alpha <= 1.0,
                        "The alpha must between [0, 1], but got '%s'", alpha);
        E.checkArgument(depth >= 1,
                        "The max depth must >= 1, but got '%s'", depth);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        HugeGraph g = graph(manager, graph);

        List<Pair<Id, Directions>> dirs = this.parseLabelDirs(g, labelDirs);
        E.checkArgument(depth <= dirs.size(),
                        "The label-dir pair size must equal with depth");

        RankAlgorithm algorithm = new RankAlgorithm(g, alpha, depth);
        Map<Id, Double> ranks = algorithm.neighborRank(sourceId, dirs);
        if (sortResult) {
            ranks = CollectionUtil.sortByValue(ranks, false);
        }
        return JsonUtil.toJson(ranks);
    }

    // TODO: 先假设labelDirs是有值的
    private List<Pair<Id, Directions>> parseLabelDirs(HugeGraph graph,
                                                      String labelDirStr) {
        if (labelDirStr == null) {
            return null;
        }
        if (labelDirStr.isEmpty()) {
            return ImmutableList.of();
        }
        try {
            Map<String, Object> labelDirMap = JsonUtil.fromJson(labelDirStr, Map.class);
            List<Pair<Id, Directions>> labelDirs = InsertionOrderUtil.newList();
            for (Map.Entry<String, Object> entry : labelDirMap.entrySet()) {
                EdgeLabel label = graph.edgeLabel(entry.getKey());
                Directions dir = Directions.valueOf(entry.getValue().toString());
                labelDirs.add(Pair.of(label.id(), dir));
            }
            return labelDirs;
        } catch (Exception e) {
            throw new HugeException("解析labelDirs失败", e);
        }
        // If properties is the string "null", props will be null
//        E.checkArgument(dirs != null, "Invalid request with none properties");
//        return dirs;
    }
}
