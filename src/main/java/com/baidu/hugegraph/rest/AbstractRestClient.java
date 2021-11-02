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

package com.baidu.hugegraph.rest;

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
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
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;

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
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.internal.util.collection.Ref;
import org.glassfish.jersey.internal.util.collection.Refs;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.uri.UriComponent;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.google.common.collect.ImmutableMap;

public abstract class AbstractRestClient implements RestClient {

    // Time unit: hours
    private static final long TTL = 24L;
    // Time unit: ms
    private static final long IDLE_TIME = 40L * 1000L;

    private static final String TOKEN_KEY = "tokenKey";

    private final Client client;
    private final WebTarget target;

    private PoolingHttpClientConnectionManager pool;
    private ScheduledExecutorService cleanExecutor;

    public AbstractRestClient(String url, int timeout) {
        this(url, new ConfigBuilder().configTimeout(timeout).build());
    }

    public AbstractRestClient(String url, String user, String password,
                              int timeout) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configUser(user, password)
                                     .build());
    }

    public AbstractRestClient(String url, int timeout,
                              int maxTotal, int maxPerRoute) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configPool(maxTotal, maxPerRoute)
                                     .build());
    }

    public AbstractRestClient(String url, int timeout, int idleTime,
                              int maxTotal, int maxPerRoute) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configIdleTime(idleTime)
                                     .configPool(maxTotal, maxPerRoute)
                                     .build());
    }

    public AbstractRestClient(String url, String user, String password,
                              int timeout, int maxTotal, int maxPerRoute) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configUser(user, password)
                                     .configPool(maxTotal, maxPerRoute)
                                     .build());
    }

    public AbstractRestClient(String url, String user, String password,
                              int timeout, int maxTotal, int maxPerRoute,
                              String trustStoreFile,
                              String trustStorePassword) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configUser(user, password)
                                     .configPool(maxTotal, maxPerRoute)
                                     .configSSL(trustStoreFile,
                                                trustStorePassword)
                                     .build());
    }

    public AbstractRestClient(String url, String token, int timeout) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configToken(token)
                                     .build());
    }

    public AbstractRestClient(String url, String token, int timeout,
                              int maxTotal, int maxPerRoute) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configToken(token)
                                     .configPool(maxTotal, maxPerRoute)
                                     .build());
    }

    public AbstractRestClient(String url, String token, int timeout,
                              int maxTotal, int maxPerRoute,
                              String trustStoreFile,
                              String trustStorePassword) {
        this(url, new ConfigBuilder().configTimeout(timeout)
                                     .configToken(token)
                                     .configPool(maxTotal, maxPerRoute)
                                     .configSSL(trustStoreFile,
                                                trustStorePassword)
                                     .build());
    }

    public AbstractRestClient(String url, ClientConfig config) {
        configConnectionManager(url, config);
        this.client = ClientBuilder.newClient(config);
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

    protected abstract void checkStatus(Response response,
                                        Response.Status... statuses);

    protected Response request(Callable<Response> method) {
        try {
            return method.call();
        } catch (Exception e) {
            throw new ClientException("Failed to do request", e);
        }
    }

    @Override
    public RestResult post(String path, Object object) {
        return this.post(path, object, null, null);
    }

    @Override
    public RestResult post(String path, Object object,
                           MultivaluedMap<String, Object> headers) {
        return this.post(path, object, headers, null);
    }

    @Override
    public RestResult post(String path, Object object,
                           Map<String, Object> params) {
        return this.post(path, object, null, params);
    }

    @Override
    public RestResult post(String path, Object object,
                           MultivaluedMap<String, Object> headers,
                           Map<String, Object> params) {
        Pair<Builder, Entity<?>> pair = this.buildRequest(path, null, object,
                                                          headers, params);
        Response response = this.request(() -> {
            // pair.getLeft() is builder, pair.getRight() is entity (http body)
            return pair.getLeft().post(pair.getRight());
        });
        // If check status failed, throw client exception.
        checkStatus(response, Response.Status.CREATED,
                    Response.Status.OK, Response.Status.ACCEPTED);
        return new RestResult(response);
    }

    @Override
    public RestResult put(String path, String id, Object object) {
        return this.put(path, id, object, ImmutableMap.of());
    }

    @Override
    public RestResult put(String path, String id, Object object,
                          MultivaluedMap<String, Object> headers) {
        return this.put(path, id, object, headers, null);
    }

    @Override
    public RestResult put(String path, String id, Object object,
                          Map<String, Object> params) {
        return this.put(path, id, object, null, params);
    }

    @Override
    public RestResult put(String path, String id, Object object,
                          MultivaluedMap<String, Object> headers,
                          Map<String, Object> params) {
        Pair<Builder, Entity<?>> pair = this.buildRequest(path, id, object,
                                                          headers, params);
        Response response = this.request(() -> {
            // pair.getLeft() is builder, pair.getRight() is entity (http body)
            return pair.getLeft().put(pair.getRight());
        });
        // If check status failed, throw client exception.
        checkStatus(response, Response.Status.OK, Response.Status.ACCEPTED);
        return new RestResult(response);
    }

    @Override
    public RestResult get(String path) {
        return this.get(path, null, ImmutableMap.of());
    }

    @Override
    public RestResult get(String path, Map<String, Object> params) {
        return this.get(path, null, params);
    }

    @Override
    public RestResult get(String path, String id) {
        return this.get(path, id, ImmutableMap.of());
    }

    private RestResult get(String path, String id, Map<String, Object> params) {
        Ref<WebTarget> target = Refs.of(this.target);
        for (String key : params.keySet()) {
            Object value = params.get(key);
            if (value instanceof Collection) {
                for (Object i : (Collection<?>) value) {
                    target.set(target.get().queryParam(key, i));
                }
            } else {
                target.set(target.get().queryParam(key, value));
            }
        }

        Response response = this.request(() -> {
            WebTarget webTarget = target.get();
            Builder builder = id == null ? webTarget.path(path).request() :
                              webTarget.path(path).path(encode(id)).request();
            this.attachAuthToRequest(builder);
            return builder.get();
        });

        checkStatus(response, Response.Status.OK);
        return new RestResult(response);
    }

    @Override
    public RestResult delete(String path, Map<String, Object> params) {
        return this.delete(path, null, params);
    }

    @Override
    public RestResult delete(String path, String id) {
        return this.delete(path, id, ImmutableMap.of());
    }

    private RestResult delete(String path, String id,
                              Map<String, Object> params) {
        Ref<WebTarget> target = Refs.of(this.target);
        for (String key : params.keySet()) {
            target.set(target.get().queryParam(key, params.get(key)));
        }

        Response response = this.request(() -> {
            WebTarget webTarget = target.get();
            Builder builder = id == null ? webTarget.path(path).request() :
                              webTarget.path(path).path(encode(id)).request();
            this.attachAuthToRequest(builder);
            return builder.delete();
        });

        checkStatus(response, Response.Status.NO_CONTENT,
                    Response.Status.ACCEPTED);
        return new RestResult(response);
    }

    @Override
    public void close() {
        if (this.pool != null) {
            this.pool.close();
            this.cleanExecutor.shutdownNow();
        }
        this.client.close();
    }

    private final ThreadLocal<String> authContext =
                                      new InheritableThreadLocal<>();

    public void setAuthContext(String auth) {
        this.authContext.set(auth);
    }

    public void resetAuthContext() {
        this.authContext.remove();
    }

    public String getAuthContext() {
        return this.authContext.get();
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
        Entity<?> entity;
        if (encoding == null) {
            entity = Entity.json(object);
        } else {
            Variant variant = new Variant(MediaType.APPLICATION_JSON_TYPE,
                                          (String) null, encoding);
            entity = Entity.entity(object, variant);
        }
        return Pair.of(builder, entity);
    }

    private static void configConnectionManager(String url, ClientConfig conf) {
        /*
         * Using httpclient with connection pooling, and configuring the
         * jersey connector, reference:
         * http://www.theotherian.com/2013/08/jersey-client-2.0-httpclient-timeouts-max-connections.html
         * https://stackoverflow.com/questions/43228051/memory-issue-with-jax-rs-using-jersey/46175943#46175943
         *
         * But the jersey that has been released in the maven central
         * repository seems to have a bug.
         * https://github.com/jersey/jersey/pull/3752
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
        if (protocol == null || protocol.equals("http")) {
            return new PoolingHttpClientConnectionManager(TTL, TimeUnit.HOURS);
        }

        assert protocol.equals("https");
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

        public static TrustManager[] create() {
            return new TrustManager[]{new NoCheckTrustManager()};
        }
    }

    private static class ConfigBuilder {

        private final ClientConfig config;

        public ConfigBuilder() {
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
        public void filter(ClientRequestContext context) throws IOException {
            String token = context.getClient().getConfiguration()
                                  .getProperty(TOKEN_KEY).toString();
            context.getHeaders().add(HttpHeaders.AUTHORIZATION,
                                     "Bearer " + token);
        }
    }
}
