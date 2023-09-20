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

package org.apache.hugegraph.api.filter;

import static org.apache.hugegraph.api.filter.PathFilter.REQUEST_TIME;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_FAILED_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_RESPONSE_TIME_HISTOGRAM;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_SUCCESS_COUNTER;
import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_TOTAL_COUNTER;

import java.io.IOException;

import org.apache.hugegraph.metrics.MetricsUtil;

import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;


@Provider
@Singleton
public class AccessLogFilter implements ContainerResponseFilter {

    private static final String DELIMETER = "/";

    /**
     * Use filter to log request info
     *
     * @param requestContext  requestContext
     * @param responseContext responseContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        // Grab corresponding request / response info from context;
        String method = requestContext.getRequest().getMethod();
        String path = requestContext.getUriInfo().getPath();
        String metricsName = join(path, method);

        MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_TOTAL_COUNTER)).inc();
        if (statusOk(responseContext.getStatus())) {
            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_SUCCESS_COUNTER)).inc();
        } else {
            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_FAILED_COUNTER)).inc();
        }

        // get responseTime
        Object requestTime = requestContext.getProperty(REQUEST_TIME);
        if(requestTime!=null){
            long now = System.currentTimeMillis();
            long responseTime = (now - (long)requestTime);

            MetricsUtil.registerHistogram(
                               join(metricsName, METRICS_PATH_RESPONSE_TIME_HISTOGRAM))
                       .update(responseTime);
        }
    }

    private String join(String path1, String path2) {
        return String.join(DELIMETER, path1, path2);
    }

    private boolean statusOk(int status){
        return status == 200 || status == 201 || status == 202;
    }
}
