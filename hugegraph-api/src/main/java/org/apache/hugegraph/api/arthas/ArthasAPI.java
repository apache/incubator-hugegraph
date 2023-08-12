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

package org.apache.hugegraph.api.arthas;

import java.util.HashMap;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.util.JsonUtil;

import com.codahale.metrics.annotation.Timed;
import com.taobao.arthas.agent.attach.ArthasAgent;

import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("arthas")
@Singleton
public class ArthasAPI extends API {

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object arthasStart() {
        HugeConfig config = this.configProvider.get();
        HashMap<String, String> configMap = new HashMap<>(4);
        configMap.put("arthas.telnetPort", config.get(ServerOptions.ARTHAS_TELNET_PORT));
        configMap.put("arthas.httpPort", config.get(ServerOptions.ARTHAS_HTTP_PORT));
        configMap.put("arthas.ip", config.get(ServerOptions.ARTHAS_IP));
        configMap.put("arthas.disabledCommands",
                      config.get(ServerOptions.ARTHAS_DISABLED_COMMANDS));
        ArthasAgent.attach(configMap);

        DashResponse retPose = new DashResponse();
        retPose.setData(JsonUtil.toJson(configMap));
        retPose.setStatus(200);
        retPose.setMessage("arthas 启动成功");
        return retPose;
    }

    public static class DashResponse {

        private String data;
        private int status;
        private String message;

        public DashResponse() {
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
