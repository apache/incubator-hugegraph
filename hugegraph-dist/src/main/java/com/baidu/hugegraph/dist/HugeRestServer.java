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

package com.baidu.hugegraph.dist;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;

public class HugeRestServer {

    private static final Logger LOG = Log.logger(HugeRestServer.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            String msg = "HugeRestServer can only accept one config files";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        try {
            // Start HugeRestServer
            start(args[0]);
        } catch (Exception e) {
            LOG.error("HugeRestServer error:", e);
            throw e;
        }
        LOG.info("HugeRestServer stopped");
    }

    public static void register() {
        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
    }

    public static RestServer start(String conf) throws Exception {
        RegisterUtil.registerServer();

        // Start RestServer
        return RestServer.start(conf);
    }
}
