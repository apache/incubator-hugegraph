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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import com.baidu.hugegraph.metrics.MetricManager;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.baidu.hugegraph.version.ApiVersion;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.metrics.MetricsModule;
import com.baidu.hugegraph.metrics.ServerReporter;
import com.baidu.hugegraph.metrics.SystemMetrics;
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

    private static final String strHelp = "# HELP ";
    private static final String strType = "# TYPE ";
    private static final String histogramType = "histogram";
    private static final String unTyped = "untyped";
    private static final String gaugeType = "gauge";
    private static final String endlStr = "\n";
    private static final String spaceStr = " ";
    private static final String countAttr = "{name=\"count\",} ";
    private static final String minAttr = "{name=\"min\",} ";
    private static final String maxAttr = "{name=\"max\",} ";
    private static final String meanAttr = "{name=\"mean\",} ";
    private static final String stddevAttr = "{name=\"stddev\",} ";
    private static final String p50Attr = "{name=\"p50\",} ";
    private static final String p75Attr = "{name=\"p75\",} ";
    private static final String p95Attr = "{name=\"p95\",} ";
    private static final String p98Attr = "{name=\"p98\",} ";
    private static final String p99Attr = "{name=\"p99\",} ";
    private static final String p999Attr = "{name=\"p999\",} ";
    private static final String meanRateAtrr = "{name=\"mean_rate\",} ";
    private static final String oneMinRateAtrr = "{name=\"m1_rate\",} ";
    private static final String fireMinRateAtrr = "{name=\"m5_rate\",} ";
    private static final String fiftMinRateAtrr = "{name=\"m15_rate\",} ";

    public MetricsAPI() {
        this.systemMetrics = new SystemMetrics();
    }

    @GET
    @Timed
    @Path("system")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$graphspace=$graphspace $owner= " +
                            "$action=metrics_read"})
    public String system() {
        return JsonUtil.toJson(this.systemMetrics.metrics());
    }

    @GET
    @Timed
    @Path("backend")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$graphspace=$graphspace $owner= " +
                            "$action=metrics_read"})
    public String backend(@Context GraphManager manager) {
        Map<String, Map<String, Object>> results = InsertionOrderUtil.newMap();
        for (HugeGraph g : manager.graphs()) {
            Map<String, Object> metrics = InsertionOrderUtil.newMap();
            metrics.put(BackendMetrics.BACKEND, g.backend());
            try {
                metrics.putAll(g.metadata(null, "metrics"));
            } catch (Throwable e) {
                metrics.put(BackendMetrics.EXCEPTION, e.toString());
                LOG.debug("Failed to get backend metrics", e);
            }
            results.put(g.name(), metrics);
        }
        return JsonUtil.toJson(results);
    }


    private String all() {
        ServerReporter reporter = ServerReporter.instance();
        Map<String, Map<String, ? extends Metric>> result = new LinkedHashMap<>();
        result.put("gauges", reporter.gauges());
        result.put("counters", reporter.counters());
        result.put("histograms", reporter.histograms());
        result.put("meters", reporter.meters());
        result.put("timers", reporter.timers());
        return JsonUtil.toJson(result);
    }

    private String replaceDotDashInKey(String orgKey){
       return orgKey.replace(".", "_").replace("-", "_");
    }

    private String exportSnapshort(final String helpName, final Snapshot snapshot){
        if ( snapshot != null ) {
            StringBuilder snapMetrics = new StringBuilder();
            snapMetrics.append(helpName)
                    .append(minAttr)
                    .append(snapshot.getMin() + endlStr);
            snapMetrics.append(helpName)
                    .append(maxAttr)
                    .append(snapshot.getMax() + endlStr);
            snapMetrics.append(helpName)
                    .append(meanAttr)
                    .append(snapshot.getMean() + endlStr);
            snapMetrics.append(helpName)
                    .append(stddevAttr)
                    .append(snapshot.getStdDev() + endlStr);
            snapMetrics.append(helpName)
                    .append(p50Attr)
                    .append(snapshot.getMedian() + endlStr);
            snapMetrics.append(helpName)
                    .append(p75Attr)
                    .append(snapshot.get75thPercentile() + endlStr);
            snapMetrics.append(helpName)
                    .append(p95Attr)
                    .append(snapshot.get95thPercentile() + endlStr);
            snapMetrics.append(helpName)
                    .append(p98Attr)
                    .append(snapshot.get98thPercentile() + endlStr);
            snapMetrics.append(helpName)
                    .append(p99Attr)
                    .append(snapshot.get99thPercentile() + endlStr);
            snapMetrics.append(helpName)
                    .append(p999Attr)
                    .append(snapshot.get999thPercentile() + endlStr);
            return  snapMetrics.toString();
        }
        return "";
    }

    private String prometheuseAll() {
        StringBuilder promeMetrics = new StringBuilder();
        ServerReporter reporter = ServerReporter.instance();
        String helpName = "hugegraph_info";
        //version
        promeMetrics.append(strHelp)
                .append(helpName).append(endlStr);
        promeMetrics.append(strType)
                .append(helpName)
                .append(spaceStr+ unTyped + endlStr);
        promeMetrics.append(helpName)
                .append("{version=\"")
                .append(ApiVersion.VERSION.toString()).append("\",}")
                .append(spaceStr + "1.0" + endlStr);

        //gauges
        for (String key : reporter.gauges().keySet()) {
            final com.codahale.metrics.Gauge<?> gauge
                    = reporter.gauges().get(key);
            if (gauge != null) {
                helpName = replaceDotDashInKey(key);
                promeMetrics.append(strHelp)
                        .append(helpName).append(endlStr);
                promeMetrics.append(strType)
                        .append(helpName).append(spaceStr+ gaugeType + endlStr);
                promeMetrics.append(helpName)
                        .append(spaceStr + gauge.getValue() + endlStr);
            }
        }

        //histograms
        for (String hkey : reporter.histograms().keySet()) {
            final Histogram histogram = reporter.histograms().get(hkey);
            if (histogram != null) {
                helpName = replaceDotDashInKey(hkey);
                promeMetrics.append(strHelp)
                        .append(helpName).append(endlStr);
                promeMetrics.append(strType)
                        .append(helpName)
                        .append(spaceStr+ histogramType + endlStr);

                promeMetrics.append(helpName)
                        .append(countAttr)
                        .append(histogram.getCount() + endlStr);
                promeMetrics.append(
                        exportSnapshort(helpName, histogram.getSnapshot()));
            }
        }

        //meters
        for (String mkey : reporter.meters().keySet()) {
            final Meter metric = reporter.meters().get(mkey);
            if (metric != null) {
                helpName = replaceDotDashInKey(mkey);
                promeMetrics.append(strHelp)
                        .append(helpName).append(endlStr);
                promeMetrics.append(strType)
                        .append(helpName)
                        .append(spaceStr+ histogramType + endlStr);

                promeMetrics.append(helpName)
                        .append(countAttr)
                        .append(metric.getCount() + endlStr);
                promeMetrics.append(helpName)
                        .append(meanRateAtrr)
                        .append(metric.getMeanRate() + endlStr);
                promeMetrics.append(helpName)
                        .append(oneMinRateAtrr)
                        .append(metric.getOneMinuteRate() + endlStr);
                promeMetrics.append(helpName)
                        .append(fireMinRateAtrr)
                        .append(metric.getFiveMinuteRate() + endlStr);
                promeMetrics.append(helpName)
                        .append(fiftMinRateAtrr)
                        .append(metric.getFifteenMinuteRate() + endlStr);
            }
        }

        //timer
        for (String tkey : reporter.timers().keySet()) {
            final com.codahale.metrics.Timer timer = reporter.timers().get(tkey);
            if (timer != null) {
                helpName = replaceDotDashInKey(tkey);
                promeMetrics.append(strHelp)
                        .append(helpName).append(endlStr);
                promeMetrics.append(strType)
                        .append(helpName)
                        .append(spaceStr+ histogramType + endlStr);

                promeMetrics.append(helpName)
                        .append(countAttr)
                        .append(timer.getCount() + endlStr);
                promeMetrics.append(helpName)
                        .append(oneMinRateAtrr)
                        .append(timer.getOneMinuteRate() + endlStr);
                promeMetrics.append(helpName)
                        .append(fireMinRateAtrr)
                        .append(timer.getFiveMinuteRate() + endlStr);
                promeMetrics.append(helpName)
                        .append(fiftMinRateAtrr)
                        .append(timer.getFifteenMinuteRate() + endlStr);
                promeMetrics.append(
                        exportSnapshort(helpName, timer.getSnapshot()));
            }
        }

        MetricsUtil.writePrometheus(promeMetrics, MetricManager.INSTANCE.getRegistry());

        return promeMetrics.toString();
    }

    @GET
    @Timed
    @Produces(APPLICATION_TEXT_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String all(@Context GraphManager manager,
                      @QueryParam("type") String type) {
        if (type != null && type.equals("json")) {
            return all();
        } else {
            return prometheuseAll();
        }
    }

    @GET
    @Timed
    @Path("gauges")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String gauges() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.gauges());
    }

    @GET
    @Timed
    @Path("counters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String counters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.counters());
    }

    @GET
    @Timed
    @Path("histograms")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String histograms() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.histograms());
    }

    @GET
    @Timed
    @Path("meters")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String meters() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.meters());
    }

    @GET
    @Timed
    @Path("timers")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner= $action=metrics_read"})
    public String timers() {
        ServerReporter reporter = ServerReporter.instance();
        return JsonUtil.toJson(reporter.timers());
    }
}
