/*
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

package org.apache.hugegraph.api.traversers;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_CAPACITY;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.Iterator;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.hugegraph.core.GraphManager;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser;
import org.apache.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/weightedshortestpath")
@Singleton
@Tag(name = "WeightedShortestPathAPI")
public class WeightedShortestPathAPI extends API {

    private static final Logger LOG = Log.logger(WeightedShortestPathAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("target") String target,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("weight") String weight,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("with_vertex") boolean withVertex) {
        LOG.debug("Graph [{}] get weighted shortest path between '{}' and " +
                  "'{}' with direction {}, edge label {}, weight property {}, " +
                  "max degree '{}', skip degree '{}', capacity '{}', " +
                  "and with vertex '{}'",
                  graph, source, target, direction, edgeLabel, weight,
                  maxDegree, skipDegree, capacity, withVertex);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));
        E.checkArgumentNotNull(weight, "The weight property can't be null");

        HugeGraph g = graph(manager, graph);
        SingleSourceShortestPathTraverser traverser =
                new SingleSourceShortestPathTraverser(g);

        NodeWithWeight path = traverser.weightedShortestPath(
                              sourceId, targetId, dir, edgeLabel, weight,
                              maxDegree, skipDegree, capacity);
        Iterator<Vertex> iterator = QueryResults.emptyIterator();
        if (path != null && withVertex) {
            assert !path.node().path().isEmpty();
            iterator = g.vertices(path.node().path().toArray());
        }
        return manager.serializer(g).writeWeightedPath(path, iterator);
    }
}
