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

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.configuration.ConfigurationException;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import com.baidu.hugegraph.util.Log;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;
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

        this.httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, rc);
        this.httpServer.start();
    }

    @SuppressWarnings("deprecation") // TODO: use shutdown instead
    public void stop() {
        E.checkNotNull(this.httpServer, "http server");
        this.httpServer.stop();
    }

    public static RestServer start(String conf) throws Exception {
        LOG.info("RestServer starting...");
        ApiVersion.check();

        RestServer server = new RestServer(RestServer.loadConf(conf));
        server.start();
        LOG.info("RestServer started");

        return server;
    }

    protected static HugeConfig loadConf(String conf) throws HugeException {
        try {
            return new HugeConfig(conf);
        } catch (ConfigurationException e) {
            throw new HugeException("Failed to load config file", e);
        }
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
