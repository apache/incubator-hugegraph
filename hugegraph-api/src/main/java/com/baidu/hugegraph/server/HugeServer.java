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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.server;

import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.configuration.ConfigurationException;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;
import com.google.common.base.Preconditions;

public class HugeServer {

    private static final Logger logger = LoggerFactory.getLogger(HugeServer.class);

    private HugeConfig conf = null;

    private HttpServer httpServer = null;

    public HugeServer(HugeConfig conf) {
        this.conf = conf;
    }

    public void start() throws IllegalArgumentException, IOException {
        String url = conf.get(ServerOptions.HUGE_SERVER_URL);
        URI uri = UriBuilder.fromUri(url).build();

        ResourceConfig rc = new ApplicationConfig(this.conf);

        this.httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, rc);
        this.httpServer.start();
    }

    @SuppressWarnings("deprecation") // TODO: use shutdown instead
    public void stop() {
        Preconditions.checkNotNull(this.httpServer);
        this.httpServer.stop();
    }

    public static HugeServer start(String[] args) {
        logger.info("HugeServer starting...");
        HugeConfig conf = HugeServer.loadConf(args);

        HugeServer server = new HugeServer(conf);
        try {
            server.start();
            logger.info("HugeServer started");
        } catch (IllegalArgumentException | IOException e) {
            logger.error("Failed to start HugeServer", e);
        }

        return server;
    }

    protected static HugeConfig loadConf(String[] args) {
        E.checkArgument(args.length == 1,
                "HugeServer need one config file, but was given %s", args);

        HugeConfig conf = null;
        try {
            conf = new HugeConfig(args[0]);
        } catch (ConfigurationException e) {
            logger.error("Failed to load config file", e);
        }

        return conf;
    }

    public static void main(String[] args) throws Exception {
        HugeServer.start(args);
        Thread.currentThread().join();
        logger.info("HugeServer stopped");
    }
}
