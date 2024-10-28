/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.filter;

import static org.apache.hugegraph.api.filter.PathFilter.REQUEST_TIME;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_FAILED_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_RESPONSE_TIME_HISTOGRAM;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_SUCCESS_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_TOTAL_COUNTER;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;

// TODO: should add test for this class
@Provider
@Singleton
public class AccessLogFilter implements ContainerResponseFilter {

    private static final Logger LOG = Log.logger(AccessLogFilter.class);

    private static final String DELIMITER = "/";
    private static final String GRAPHS = "graphs";
    private static final String GREMLIN = "gremlin";
    private static final String CYPHER = "cypher";

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    @Context
    private jakarta.inject.Provider<GraphManager> managerProvider;

    public static boolean needRecordLog(ContainerRequestContext context) {
        // TODO: add test for 'path' result ('/gremlin' or 'gremlin')
        String path = context.getUriInfo().getPath();

        // GraphsAPI/CypherAPI/Job GremlinAPI
        if (path.startsWith(GRAPHS)) {
            if (HttpMethod.GET.equals(context.getMethod()) || path.endsWith(CYPHER)) {
                return true;
            }
        }
        // Direct GremlinAPI
        return path.endsWith(GREMLIN);
    }

    private String join(String path1, String path2) {
        return String.join(DELIMITER, path1, path2);
    }

    private static String normalizePath(ContainerRequestContext requestContext) {
        // Replace variable parts of the path with placeholders
        String requestPath = requestContext.getUriInfo().getPath();
        // get uri params
        MultivaluedMap<String, String> pathParameters = requestContext.getUriInfo()
                                                                      .getPathParameters();

        String newPath = requestPath;
        for (Map.Entry<String, java.util.List<String>> entry : pathParameters.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().get(0);
            if ("graph".equals(key)) {
                newPath = newPath.replace(key, value);
            }
            newPath = newPath.replace(value, key);
        }

        LOG.trace("normalize path, original path: '{}', new path: '{}'", requestPath, newPath);
        return newPath;
    }

    /**
     * Use filter to log request info
     *
     * @param requestContext  requestContext
     * @param responseContext responseContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException {
        // Grab corresponding request / response info from context;
        URI uri = requestContext.getUriInfo().getRequestUri();
        String method = requestContext.getMethod();
        String path = normalizePath(requestContext);
        String metricsName = join(path, method);

        int status = responseContext.getStatus();
        if (status != 500 && status != 415) {
            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_TOTAL_COUNTER)).inc();
        }

        if (statusOk(responseContext.getStatus())) {
            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_SUCCESS_COUNTER)).inc();
        } else {
            // TODO: The return codes for compatibility need to be further detailed.
            LOG.trace("Failed Status: {}", status);
            if (status != 500 && status != 415) {
                MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_FAILED_COUNTER)).inc();
            }
        }

        Object requestTime = requestContext.getProperty(REQUEST_TIME);
        if (requestTime != null) {
            long now = System.currentTimeMillis();
            long start = (Long) requestTime;
            long executeTime = now - start;

            if (status != 500 && status != 415) {
                MetricsUtil.registerHistogram(join(metricsName,
                                                   METRICS_PATH_RESPONSE_TIME_HISTOGRAM))
                           .update(executeTime);
            }

            HugeConfig config = configProvider.get();
            long timeThreshold = config.get(ServerOptions.SLOW_QUERY_LOG_TIME_THRESHOLD);
            // Record slow query if meet needs, watch out the perf
            if (timeThreshold > 0 && executeTime > timeThreshold &&
                needRecordLog(requestContext)) {
                // TODO: set RequestBody null, handle it later & should record "client IP"
                LOG.info("[Slow Query] execTime={}ms, body={}, method={}, path={}, query={}",
                         executeTime, null, method, path, uri.getQuery());
            }
        }

        // Unset the context in "HugeAuthenticator", need distinguish Graph/Auth server lifecycle
        GraphManager manager = managerProvider.get();
        // TODO: transfer Authorizer if we need after.
        if (manager.requireAuthentication()) {
            manager.unauthorized(requestContext.getSecurityContext());
        }
    }

    private boolean statusOk(int status) {
        return status >= 200 && status < 300;
    }
}
