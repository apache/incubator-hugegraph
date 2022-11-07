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

package com.baidu.hugegraph.job.algorithm;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.StandardHugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.job.UserJob;
import com.baidu.hugegraph.job.algorithm.cent.BetweenessCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.ClosenessCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.DegreeCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.EigenvectorCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.ClusterCoeffcientAlgorithm;
import com.baidu.hugegraph.job.algorithm.path.RingsDetectAlgorithm;
import com.baidu.hugegraph.job.algorithm.rank.PageRankAlgorithm;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.optimize.HugeScriptTraversal;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

public class SubgraphStatAlgorithm extends AbstractAlgorithm {

    public static final String KEY_SUBGRAPH = "subgraph";
    public static final String KEY_COPY_SCHEMA = "copy_schema";

    private static final Logger LOG = Log.logger(SubgraphStatAlgorithm.class);

    @Override
    public String name() {
        return "subgraph_stat";
    }

    @Override
    public String category() {
        return CATEGORY_AGGR;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        subgraph(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        HugeGraph graph = this.createTempGraph(job);
        try (Traverser traverser = new Traverser(job)) {
            this.initGraph(job.graph(), graph,
                           subgraph(parameters), copySchema(parameters));
            UserJob<Object> tmpJob = new TempJob<>(graph, job, job.task());
            return traverser.subgraphStat(tmpJob);
        } finally {
            graph.truncateBackend();
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.warn("Can't close subgraph_stat temp graph {}: {}",
                         graph, e.getMessage(), e);
            }
        }
    }

    private HugeGraph createTempGraph(UserJob<Object> job) {
        Id id = job.task().id();
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(CoreOptions.BACKEND.name(), "memory");
        config.setProperty(CoreOptions.STORE.name(), "tmp_" + id);
        config.setDelimiterParsingDisabled(true);
        return new StandardHugeGraph(new HugeConfig(config));
    }

    @SuppressWarnings("resource")
    private void initGraph(HugeGraph parent, HugeGraph graph,
                           String script, boolean copySchema) {
        if (copySchema) {
            graph.schema().copyFrom(parent.schema());
        }
        new HugeScriptTraversal<>(graph.traversal(), "gremlin-groovy",
                                  script, ImmutableMap.of(),
                                  ImmutableMap.of()).iterate();
        graph.tx().commit();
    }

    protected static String subgraph(Map<String, Object> parameters) {
        Object subgraph = parameters.get(KEY_SUBGRAPH);
        E.checkArgument(subgraph != null,
                        "Must pass parameter '%s'", KEY_SUBGRAPH);
        E.checkArgument(subgraph instanceof String,
                        "Invalid parameter '%s', expect a String, but got %s",
                        KEY_SUBGRAPH, subgraph.getClass().getSimpleName());
        return (String) subgraph;
    }

    protected static boolean copySchema(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_COPY_SCHEMA)) {
            return false;
        }
        return parameterBoolean(parameters, KEY_COPY_SCHEMA);
    }

    private static class Traverser extends AlgoTraverser {

        private static Map<String, Object> PARAMS = ImmutableMap.of(
                                                    "depth", 10L,
                                                    "degree", -1L,
                                                    "sample", -1L,
                                                    "top", -1L /* sorted */,
                                                    "workers", 0);

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object subgraphStat(UserJob<Object> job) {
            Map<String, Object> results = InsertionOrderUtil.newMap();

            GraphTraversalSource g = job.graph().traversal();
            results.put("vertices_count", g.V().count().next());
            results.put("edges_count", g.E().count().next());

            Algorithm algo = new DegreeCentralityAlgorithm();
            Map<String, Object> parameters = ImmutableMap.copyOf(PARAMS);
            results.put("degrees", algo.call(job, parameters));

            algo = new BetweenessCentralityAlgorithm();
            results.put("betweeness", algo.call(job, parameters));

            algo = new EigenvectorCentralityAlgorithm();
            results.put("eigenvectors", algo.call(job, parameters));

            algo = new ClosenessCentralityAlgorithm();
            results.put("closeness", algo.call(job, parameters));

            results.put("page_ranks", pageRanks(job));

            algo = new ClusterCoeffcientAlgorithm();
            results.put("cluster_coeffcient", algo.call(job, parameters));

            algo = new RingsDetectAlgorithm();
            parameters = ImmutableMap.<String, Object>builder()
                                     .putAll(PARAMS)
                                     .put("count_only", true)
                                     .put("each_limit", NO_LIMIT)
                                     .put("limit", NO_LIMIT)
                                     .build();
            results.put("rings", algo.call(job, parameters));

            return results;
        }

        private Map<Object, Double> pageRanks(UserJob<Object> job) {
            PageRankAlgorithm algo = new PageRankAlgorithm();
            algo.call(job, ImmutableMap.of("alpha", 0.15));

            // Collect page ranks
            Map<Object, Double> ranks = InsertionOrderUtil.newMap();
            Iterator<Vertex> vertices = job.graph().vertices();
            while (vertices.hasNext()) {
                Vertex vertex = vertices.next();
                ranks.put(vertex.id(), vertex.value(R_RANK));
            }
            ranks = HugeTraverser.topN(ranks, true, NO_LIMIT);
            return ranks;
        }
    }

    private static class TempJob<V> extends UserJob<V> {

        private final UserJob<V> parent;

        public TempJob(HugeGraph graph, UserJob<V> job, HugeTask<V> task) {
            this.graph(graph);
            this.task(task);
            this.parent = job;
        }

        @Override
        public String type() {
            return "temp";
        }

        @Override
        public V execute() throws Exception {
            return null;
        }

        @Override
        public void updateProgress(int progress) {
            this.parent.updateProgress(progress);
        }
    }
}
