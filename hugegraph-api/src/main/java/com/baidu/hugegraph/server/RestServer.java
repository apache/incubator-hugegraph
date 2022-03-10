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

package com.baidu.hugegraph.server;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.CompletionHandler;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.version.ApiVersion;

public class RestServer {

    private static final Logger LOG = Log.logger(RestServer.class);

    private final HugeConfig conf;
    private final EventHub eventHub;
    private HttpServer httpServer = null;

    public final static String EXECUTOR = "system";

    public RestServer(HugeConfig conf, EventHub hub) {
        this.conf = conf;
        this.eventHub = hub;
    }

    public void start() throws IOException {
        String url = this.conf.get(ServerOptions.REST_SERVER_URL);
        URI uri = UriBuilder.fromUri(url).build();
        Boolean k8sApiEnable = this.conf.get(ServerOptions.K8S_API_ENABLE);
        if (k8sApiEnable) {
            String namespace = this.conf.get(ServerOptions.K8S_NAMESPACE);
            String enableInternalAlgorithm = this.conf.get(
                   ServerOptions.K8S_ENABLE_INTERNAL_ALGORITHM);
            String internalAlgorithmImageUrl = this.conf.get(
                   ServerOptions.K8S_INTERNAL_ALGORITHM_IMAGE_URL);
            String internalAlgorithm = this.conf.get(
                   ServerOptions.K8S_INTERNAL_ALGORITHM);
            Map<String, String> algorithms = this.conf.getMap(
                   ServerOptions.K8S_ALGORITHMS);
            K8sDriverProxy.setConfig(namespace,
                                     enableInternalAlgorithm,
                                     internalAlgorithmImageUrl,
                                     internalAlgorithm, algorithms);
        }

        ResourceConfig rc = new ApplicationConfig(this.conf, this.eventHub);

        this.httpServer = this.configHttpServer(uri, rc);
        try {
            this.httpServer.start();
        } catch (Throwable e) {
            this.httpServer.shutdownNow();
            throw e;
        }
        this.calcMaxWriteThreads();
    }

    private HttpServer configHttpServer(URI uri, ResourceConfig rc) {
        String protocol = uri.getScheme();
        final HttpServer server;
        if ("https".equals(protocol)) {
            SSLContextConfigurator sslContext = new SSLContextConfigurator();
            String keystoreFile = this.conf.get(
                                  ServerOptions.SSL_KEYSTORE_FILE);
            String keystorePass = this.conf.get(
                                  ServerOptions.SSL_KEYSTORE_PASSWORD);
            sslContext.setKeyStoreFile(keystoreFile);
            sslContext.setKeyStorePass(keystorePass);
            SSLEngineConfigurator sslConfig = new SSLEngineConfigurator(
                                              sslContext);
            sslConfig.setClientMode(false);
            sslConfig.setWantClientAuth(true);
            server = GrizzlyHttpServerFactory.createHttpServer(uri, rc, true,
                                                               sslConfig);
        } else {
            server = GrizzlyHttpServerFactory.createHttpServer(uri, rc, false);
        }

        Collection<NetworkListener> listeners = server.getListeners();
        E.checkState(listeners.size() > 0,
                     "Http Server should have some listeners, but now is none");
        NetworkListener listener = listeners.iterator().next();

        // Option max_worker_threads
        int maxWorkerThreads = this.conf.get(ServerOptions.MAX_WORKER_THREADS);
        listener.getTransport()
                .getWorkerThreadPoolConfig()
                .setCorePoolSize(maxWorkerThreads)
                .setMaxPoolSize(maxWorkerThreads);

        // Option keep_alive
        int idleTimeout = this.conf.get(ServerOptions.CONN_IDLE_TIMEOUT);
        int maxRequests = this.conf.get(ServerOptions.CONN_MAX_REQUESTS);
        listener.getKeepAlive().setIdleTimeoutInSeconds(idleTimeout);
        listener.getKeepAlive().setMaxRequestsCount(maxRequests);

        // Option transaction timeout
        int transactionTimeout = this.conf.get(ServerOptions.REQUEST_TIMEOUT);
        listener.setTransactionTimeout(transactionTimeout);

        return server;
    }

    public Future<HttpServer> shutdown() {
        E.checkNotNull(this.httpServer, "http server");
        /*
         * Since 2.3.x shutdown() won't call shutdownNow(), so the event
         * ApplicationEvent.Type.DESTROY_FINISHED also won't be triggered,
         * which is listened by ApplicationConfig.GraphManagerFactory, we
         * manually call shutdownNow() here when the future is completed.
         * See shutdown() change:
         * https://github.com/javaee/grizzly/commit/182d8bcb4e45de5609ab92f6f1d5980f95d79b04
         * #diff-f6c130f38a1ec11bdf9d3cb7e0a81084c8788c79a00befe65e40a13bc989b098R388
         */
        CompletableFuture<HttpServer> future = new CompletableFuture<>();
        future.whenComplete((server, exception) -> {
            this.httpServer.shutdownNow();
        });

        GrizzlyFuture<HttpServer> grizzlyFuture = this.httpServer.shutdown();
        grizzlyFuture.addCompletionHandler(new CompletionHandler<HttpServer>() {
            @Override
            public void cancelled() {
                future.cancel(true);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void completed(HttpServer result) {
                future.complete(result);
            }

            @Override
            public void updated(HttpServer result) {
                // pass
            }
        });

        return future;
    }

    public void shutdownNow() {
        E.checkNotNull(this.httpServer, "http server");
        this.httpServer.shutdownNow();
    }

    public static RestServer start(String conf, EventHub hub) throws Exception {
        LOG.info("RestServer starting...");
        ApiVersion.check();

        HugeConfig config = new HugeConfig(conf);
        RestServer server = new RestServer(config, hub);
        server.start();
        LOG.info("RestServer started");

        return server;
    }

    public static RestServer start(HugeConfig config, EventHub hub) throws Exception {
        LOG.info("RestServer starting...");
        ApiVersion.check();

        RestServer server = new RestServer(config, hub);
        server.start();
        LOG.info("RestServer started");

        return server;
    }

    private void calcMaxWriteThreads() {
        int maxWriteThreads = this.conf.get(ServerOptions.MAX_WRITE_THREADS);
        if (maxWriteThreads > 0) {
            // Use the value of MAX_WRITE_THREADS option if it's not 0
            return;
        }

        assert maxWriteThreads == 0;

        int maxWriteRatio = this.conf.get(ServerOptions.MAX_WRITE_RATIO);
        assert maxWriteRatio >= 0 && maxWriteRatio <= 100;
        int maxWorkerThreads = this.conf.get(ServerOptions.MAX_WORKER_THREADS);
        maxWriteThreads = maxWorkerThreads * maxWriteRatio / 100;
        E.checkState(maxWriteThreads >= 0,
                     "Invalid value of maximum batch writing threads '%s'",
                     maxWriteThreads);
        if (maxWriteThreads == 0) {
            E.checkState(maxWriteRatio == 0,
                         "The value of maximum batch writing threads is 0 " +
                         "due to the max_write_ratio '%s' is too small, " +
                         "set to '%s' at least to ensure one thread." +
                         "If you want to disable batch write, " +
                         "please let max_write_ratio be 0", maxWriteRatio,
                         (int) Math.ceil(100.0 / maxWorkerThreads));
        }
        LOG.info("The maximum batch writing threads is {} (total threads {})",
                 maxWriteThreads, maxWorkerThreads);
        // NOTE: addProperty will make exist option's value become List
        this.conf.setProperty(ServerOptions.MAX_WRITE_THREADS.name(),
                              String.valueOf(maxWriteThreads));
    }
}
