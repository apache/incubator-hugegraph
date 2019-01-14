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

package com.baidu.hugegraph.api.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.metric.MetricsModule;
import com.baidu.hugegraph.metric.ServerReporter;
import com.baidu.hugegraph.metric.SystemMetrics;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.Metric;
import com.codahale.metrics.annotation.Timed;

@Singleton
@Path("metrics")
public class MetricsAPI extends API {

    private static final Logger LOG = Log.logger(MetricsAPI.class);

    private SystemMetrics systemMetrics;

    static {
        JsonUtil.registerModule(new MetricsModule(SECONDS, MILLISECONDS, false));
    }

    public MetricsAPI() {
        this.systemMetrics = new SystemMetrics();
    }

    @GET
    @Timed
    @Path("system")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String system() {
        return JsonUtil.toJson(this.systemMetrics.metrics());
    }

    @GET
    @Timed
    @Path("backend")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String backend(@Context GraphManager manager) {
        Map<String, Map<String, Object>> results = InsertionOrderUtil.newMap();
        for (String graph : manager.graphs()) {
            GraphTransaction tx = manager.graph(graph).graphTransaction();
            Map<String, Object> metrics = InsertionOrderUtil.newMap();
            metrics.put(BackendMetrics.BACKEND, tx.store().provider().type());
            try {
                metrics.putAll(tx.metadata(null, "metrics"));
            } catch (Throwable e) {
                metrics.put(BackendMetrics.EXCEPTION, e.toString());
                LOG.debug("Failed to get backend metrics", e);
            }
            results.put(graph, metrics);
        }
        return JsonUtil.toJson(results);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String all() {
        ServerReporter reporter = ServerReporter.instance();
        Map<String, Map<String, ? extends Metric>> result = new LinkedHashMap<>();
        result.put("gauges", reporter.gauges());
        result.put("counters", reporter.counters());
        result.put("histograms", reporter.histograms());
        result.put("meters", reporter.meters());
        result.put("timers", reporter.timers());
        return JsonUtil.toJson(result);
    }

    @GET
    @Timed
    @Path("gauges")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String gauges() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.gauges());
    }

    @GET
    @Timed
    @Path("counters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String counters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.counters());
    }

    @GET
    @Timed
    @Path("histograms")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String histograms() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.histograms());
    }

    @GET
    @Timed
    @Path("meters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String meters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.meters());
    }

    @GET
    @Timed
    @Path("timers")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String timers() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.timers());
    }
}
