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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.version.ApiVersion;

public class RestServer {

    private static final Logger LOG = Log.logger(RestServer.class);

    private final HugeConfig conf;
    private HttpServer httpServer = null;

    public RestServer(HugeConfig conf) {
        this.conf = conf;
    }

    public void start() throws IOException {
        String url = this.conf.get(ServerOptions.REST_SERVER_URL);
        URI uri = UriBuilder.fromUri(url).build();

        ResourceConfig rc = new ApplicationConfig(this.conf);

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
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(uri, rc,
                                                                      false);
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
        return this.httpServer.shutdown();
    }

    public void shutdownNow() {
        E.checkNotNull(this.httpServer, "http server");
        this.httpServer.shutdownNow();
    }

    public static RestServer start(String conf) throws Exception {
        LOG.info("RestServer starting...");
        ApiVersion.check();

        RestServer server = new RestServer(new HugeConfig(conf));
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
        this.conf.addProperty(ServerOptions.MAX_WRITE_THREADS.name(),
                              String.valueOf(maxWriteThreads));
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            LOG.error("RestServer need one config file, but given {}",
                      Arrays.asList(args));
            throw new HugeException("RestServer need one config file");
        }

        try {
            RestServer.start(args[0]);
            Thread.currentThread().join();
        } catch (Exception e) {
            LOG.error("RestServer error:", e);
            throw e;
        }
        LOG.info("RestServer stopped");
    }
}
