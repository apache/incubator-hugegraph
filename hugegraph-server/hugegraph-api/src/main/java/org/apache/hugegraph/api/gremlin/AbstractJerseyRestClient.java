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

package org.apache.hugegraph.api.gremlin;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.util.ExecutorUtil;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.message.GZipEncoder;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

/**
 * This class is a simplified class of AbstractRestClient from hugegraph-common.
 * For some reason, we replace the rest implementation from jersey to okhttp.
 * But GremlinClient still uses jersey-client to forward request, so we copy the old
 * AbstractRestClient from hugegraph-common and rename the name to AbstractJerseyRestClient.
 * Because we don't need the full feature of AbstractRestClient, so we reduce some useless code.
 */
public abstract class AbstractJerseyRestClient {

    /**
     * Time unit: hours
     */
    private static final long TTL = 24L;
    /**
     * Time unit: ms
     */
    private static final long IDLE_TIME = 40L * 1000L;

    private static final String TOKEN_KEY = "tokenKey";

    private final Client client;
    private final WebTarget target;
    private final PoolingHttpClientConnectionManager pool;
    private ScheduledExecutorService cleanExecutor;

    public AbstractJerseyRestClient(String url, int timeout, int maxTotal, int maxPerRoute) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configPool(maxTotal, maxPerRoute)
                                     .build());
    }

    public AbstractJerseyRestClient(String url, ClientConfig config) {
        configConnectionManager(url, config);

        this.client = JerseyClientBuilder.newClient(config);
        this.client.register(GZipEncoder.class);
        this.target = this.client.target(url);
        this.pool = (PoolingHttpClientConnectionManager) config
            .getProperty(ApacheClientProperties.CONNECTION_MANAGER);

        if (this.pool != null) {
            this.cleanExecutor = ExecutorUtil.newScheduledThreadPool("conn-clean-worker-%d");
            Number idleTimeProp = (Number) config.getProperty("idleTime");
            final long idleTime = idleTimeProp == null ? IDLE_TIME : idleTimeProp.longValue();
            final long checkPeriod = idleTime / 2L;
            this.cleanExecutor.scheduleWithFixedDelay(() -> {
                PoolStats stats = this.pool.getTotalStats();
                int using = stats.getLeased() + stats.getPending();
                if (using > 0) {
                    // Do clean only when all connections are idle
                    return;
                }
                // Release connections when all clients are inactive
                this.pool.closeIdleConnections(idleTime, TimeUnit.MILLISECONDS);
                this.pool.closeExpiredConnections();
            }, checkPeriod, checkPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * parse user custom content-type, returns MediaType.APPLICATION_JSON_TYPE default.
     *
     * @param headers custom http header
     */
    private static MediaType parseCustomContentType(MultivaluedMap<String, Object> headers) {
        String customContentType = null;
        if (MapUtils.isNotEmpty(headers) && headers.get("Content-Type") != null) {
            List<?> contentTypeObj = headers.get("Content-Type");
            if (contentTypeObj != null && !contentTypeObj.isEmpty()) {
                customContentType = contentTypeObj.get(0).toString();
            }
            return MediaType.valueOf(customContentType);
        }
        return MediaType.APPLICATION_JSON_TYPE;
    }

    private static void configConnectionManager(String url, ClientConfig conf) {
        /*
         * Using httpclient with connection pooling, and configuring the
         * jersey connector. But the jersey that has been released in the maven central
         * repository seems to have a bug: https://github.com/jersey/jersey/pull/3752
         */
        PoolingHttpClientConnectionManager pool =
            new PoolingHttpClientConnectionManager(TTL, TimeUnit.HOURS);
        Object maxTotal = conf.getProperty("maxTotal");
        Object maxPerRoute = conf.getProperty("maxPerRoute");

        if (maxTotal != null) {
            pool.setMaxTotal((int) maxTotal);
        }
        if (maxPerRoute != null) {
            pool.setDefaultMaxPerRoute((int) maxPerRoute);
        }
        conf.property(ApacheClientProperties.CONNECTION_MANAGER, pool);
        conf.connectorProvider(new ApacheConnectorProvider());
    }

    protected Response request(Callable<Response> method) {
        try {
            return method.call();
        } catch (Exception e) {
            throw new ClientException("Failed to do request", e);
        }
    }

    public void close() {
        if (this.pool != null) {
            this.pool.close();
            this.cleanExecutor.shutdownNow();
        }
        this.client.close();
    }

    private static class ConfigBuilder {

        private final ClientConfig config;

        ConfigBuilder() {
            this.config = new ClientConfig();
        }

        public ConfigBuilder configTimeout(int timeout) {
            this.config.property(ClientProperties.CONNECT_TIMEOUT, timeout);
            this.config.property(ClientProperties.READ_TIMEOUT, timeout);
            return this;
        }

        public ConfigBuilder configUser(String username, String password) {
            /*
             * NOTE: don't use non-preemptive mode
             * In non-preemptive mode the authentication information is added
             * only when the server refuses the request with 401 status codes and
             * then the request is repeated.
             * Non-preemptive has a negative impact on the performance.
             * The advantage is it doesn't send credentials when they are not needed
             * https://jersey.github.io/documentation/latest/client.html#d0e5461
             */
            this.config.register(HttpAuthenticationFeature.basic(username, password));
            return this;
        }

        public ConfigBuilder configToken(String token) {
            this.config.property(TOKEN_KEY, token);
            this.config.register(BearerRequestFilter.class);
            return this;
        }

        public ConfigBuilder configPool(int maxTotal, int maxPerRoute) {
            this.config.property("maxTotal", maxTotal);
            this.config.property("maxPerRoute", maxPerRoute);
            return this;
        }

        public ConfigBuilder configIdleTime(int idleTime) {
            this.config.property("idleTime", idleTime);
            return this;
        }

        public ConfigBuilder configSSL(String trustStoreFile, String trustStorePassword) {
            if (trustStoreFile == null || trustStoreFile.isEmpty() ||
                trustStorePassword == null) {
                this.config.property("protocol", "http");
            } else {
                this.config.property("protocol", "https");
            }
            this.config.property("trustStoreFile", trustStoreFile);
            this.config.property("trustStorePassword", trustStorePassword);
            return this;
        }

        public ClientConfig build() {
            return this.config;
        }
    }

    public static class BearerRequestFilter implements ClientRequestFilter {

        @Override
        public void filter(ClientRequestContext context) {
            String token = context.getClient().getConfiguration().getProperty(TOKEN_KEY)
                                  .toString();
            context.getHeaders().add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        }
    }
}
