package org.apache.hugegraph.api.filter;

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
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext)
            throws IOException {
        // Grab corresponding request / response info from context;
        String method = requestContext.getRequest().getMethod();
        String path = requestContext.getUriInfo().getPath();
        String metricsName = join(path, method);

        MetricsUtil.registerCounter(
                           join(metricsName, METRICS_PATH_TOTAL_COUNTER))
                   .inc();
        if (responseContext.getStatus() == 200 ||
            responseContext.getStatus() == 201 ||
            responseContext.getStatus() == 202) {
            MetricsUtil.registerCounter(
                               join(metricsName, METRICS_PATH_SUCCESS_COUNTER))
                       .inc();
        } else {
            MetricsUtil.registerCounter(
                               join(metricsName, METRICS_PATH_FAILED_COUNTER))
                       .inc();
        }

        //  抓取响应的时间 单位/ms
        long now = System.currentTimeMillis();
        long resposeTime = (now - (long) requestContext.getProperty("RequestTime"));

        MetricsUtil.registerHistogram(
                           join(metricsName, METRICS_PATH_RESPONSE_TIME_HISTOGRAM))
                   .update(resposeTime);

    }

    private String join(String path1, String path2) {
        return String.join(DELIMETER, path1, path2);
    }
}
