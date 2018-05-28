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

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.metric.ServerReporter;
import com.baidu.hugegraph.metric.SystemMetrics;
import com.baidu.hugegraph.util.JsonUtil;
import com.codahale.metrics.Metric;
import com.codahale.metrics.json.MetricsModule;

@Singleton
@Path("metrics")
public class MetricsAPI extends API {

    private SystemMetrics systemMetrics;

    static {
        JsonUtil.registerModule(new MetricsModule(SECONDS, MILLISECONDS, false));
    }

    public MetricsAPI() {
        this.systemMetrics = new SystemMetrics();
    }

    @GET
    @Path("system")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String system() {
        return JsonUtil.toJson(this.systemMetrics.metrics());
    }

    @GET
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
    @Path("gauges")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String gauges() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.gauges());
    }

    @GET
    @Path("counters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String counters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.counters());
    }

    @GET
    @Path("histograms")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String histograms() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.histograms());
    }

    @GET
    @Path("meters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String meters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.meters());
    }

    @GET
    @Path("times")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String times() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.timers());
    }
}
