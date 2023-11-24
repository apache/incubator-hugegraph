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

import java.net.URI;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHeaders;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.ExecutorUtil;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.uri.UriComponent;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation.Builder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Variant;

/**
 * This class is a simplified class of AbstractRestClient from hugegraph-common.
 * For some reason we replace the rest implementation from jersey to okhttp.
 * But GremlinClient still use jersey-client to forward request, so we copy the old
 * AbstractRestClient from hugegraph-common and rename the name to AbstractJerseyRestClient.
 * Because we don't need the full feature of AbstractRestClient, so we reduce some useless code.
 */
public abstract class AbstractJerseyRestClient {

    // Time unit: hours
    private static final long TTL = 24L;
    // Time unit: ms
    private static final long IDLE_TIME = 40L * 1000L;

    private static final String TOKEN_KEY = "tokenKey";

    private final Client client;
    private final WebTarget target;
    private final ThreadLocal<String> authContext =
        new InheritableThreadLocal<>();
    private final PoolingHttpClientConnectionManager pool;
    private ScheduledExecutorService cleanExecutor;

    public AbstractJerseyRestClient(String url, int timeout,
                                    int maxTotal, int maxPerRoute) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configPool(maxTotal, maxPerRoute)
                                     .build());
    }

    public AbstractJerseyRestClient(String url, ClientConfig config) {
        configConnectionManager(url, config);

        this.client = JerseyClientBuilder.newClient(config);
        this.client.register(GZipEncoder.class);
        this.target = this.client.target(url);
        this.pool = (PoolingHttpClientConnectionManager) config.getProperty(
            ApacheClientProperties.CONNECTION_MANAGER);
        if (this.pool != null) {
            this.cleanExecutor = ExecutorUtil.newScheduledThreadPool(
                "conn-clean-worker-%d");
            Number idleTimeProp = (Number) config.getProperty("idleTime");
            final long idleTime = idleTimeProp == null ?
                                  IDLE_TIME : idleTimeProp.longValue();
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
        PoolingHttpClientConnectionManager pool = connectionManager(url, conf);
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

    private static PoolingHttpClientConnectionManager connectionManager(
        String url,
        ClientConfig conf) {
        String protocol = (String) conf.getProperty("protocol");
        if (protocol == null || "http".equals(protocol)) {
            return new PoolingHttpClientConnectionManager(TTL, TimeUnit.HOURS);
        }

        assert "https".equals(protocol);
        String trustStoreFile = (String) conf.getProperty("trustStoreFile");
        E.checkArgument(trustStoreFile != null && !trustStoreFile.isEmpty(),
                        "The trust store file must be set when use https");
        String trustStorePass = (String) conf.getProperty("trustStorePassword");
        E.checkArgument(trustStorePass != null,
                        "The trust store password must be set when use https");
        SSLContext context = SslConfigurator.newInstance()
                                            .trustStoreFile(trustStoreFile)
                                            .trustStorePassword(trustStorePass)
                                            .securityProtocol("SSL")
                                            .createSSLContext();
        TrustManager[] trustAllManager = NoCheckTrustManager.create();
        try {
            context.init(null, trustAllManager, new SecureRandom());
        } catch (KeyManagementException e) {
            throw new ClientException("Failed to init security management", e);
        }

        HostnameVerifier verifier = new HostNameVerifier(url);
        ConnectionSocketFactory httpSocketFactory, httpsSocketFactory;
        httpSocketFactory = PlainConnectionSocketFactory.getSocketFactory();
        httpsSocketFactory = new SSLConnectionSocketFactory(context, verifier);
        Registry<ConnectionSocketFactory> registry =
            RegistryBuilder.<ConnectionSocketFactory>create()
                           .register("http", httpSocketFactory)
                           .register("https", httpsSocketFactory)
                           .build();
        return new PoolingHttpClientConnectionManager(registry, null,
                                                      null, null, TTL,
                                                      TimeUnit.HOURS);
    }

    public static String encode(String raw) {
        return UriComponent.encode(raw, UriComponent.Type.PATH_SEGMENT);
    }

    protected abstract void checkStatus(Response response,
                                        Response.Status... statuses);

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

    public void resetAuthContext() {
        this.authContext.remove();
    }

    public String getAuthContext() {
        return this.authContext.get();
    }

    public void setAuthContext(String auth) {
        this.authContext.set(auth);
    }

    private void attachAuthToRequest(Builder builder) {
        // Add auth header
        String auth = this.getAuthContext();
        if (StringUtils.isNotEmpty(auth)) {
            builder.header(HttpHeaders.AUTHORIZATION, auth);
        }
    }

    private Pair<Builder, Entity<?>> buildRequest(
        String path, String id, Object object,
        MultivaluedMap<String, Object> headers,
        Map<String, Object> params) {
        WebTarget target = this.target;
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, Object> param : params.entrySet()) {
                target = target.queryParam(param.getKey(), param.getValue());
            }
        }

        Builder builder = id == null ? target.path(path).request() :
                          target.path(path).path(encode(id)).request();

        String encoding = null;
        if (headers != null && !headers.isEmpty()) {
            // Add headers
            builder = builder.headers(headers);
            encoding = (String) headers.getFirst("Content-Encoding");
        }
        // Add auth header
        this.attachAuthToRequest(builder);

        /*
         * We should specify the encoding of the entity object manually,
         * because Entity.json() method will reset "content encoding =
         * null" that has been set up by headers before.
         */
        MediaType customContentType = parseCustomContentType(headers);
        Entity<?> entity;
        if (encoding == null) {
            entity = Entity.entity(object, customContentType);
        } else {
            Variant variant = new Variant(customContentType,
                                          (String) null, encoding);
            entity = Entity.entity(object, variant);
        }
        return Pair.of(builder, entity);
    }

    public static class HostNameVerifier implements HostnameVerifier {

        private final String url;

        public HostNameVerifier(String url) {
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                url = "http://" + url;
            }
            url = URI.create(url).getHost();
            this.url = url;
        }

        @Override
        public boolean verify(String hostname, SSLSession session) {
            if (!this.url.isEmpty() && this.url.endsWith(hostname)) {
                return true;
            } else {
                HostnameVerifier verifier = HttpsURLConnection
                    .getDefaultHostnameVerifier();
                return verifier.verify(hostname, session);
            }
        }
    }

    private static class NoCheckTrustManager implements X509TrustManager {

        public static TrustManager[] create() {
            return new TrustManager[]{new NoCheckTrustManager()};
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
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
             * only when server refuses the request with 401 status code and
             * then the request is repeated.
             * Non-preemptive has negative impact on the performance. The
             * advantage is it doesn't send credentials when they are not needed
             * https://jersey.github.io/documentation/latest/client.html#d0e5461
             */
            this.config.register(HttpAuthenticationFeature.basic(username,
                                                                 password));
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

        public ConfigBuilder configSSL(String trustStoreFile,
                                       String trustStorePassword) {
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
            String token = context.getClient().getConfiguration()
                                  .getProperty(TOKEN_KEY).toString();
            context.getHeaders().add(HttpHeaders.AUTHORIZATION,
                                     "Bearer " + token);
        }
    }
}
