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

package com.baidu.hugegraph.example;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import com.baidu.hugegraph.RegisterConfig;
import com.baidu.hugegraph.pd.client.DiscoveryClient;
import com.baidu.hugegraph.pd.client.DiscoveryClientImpl;
import com.baidu.hugegraph.pd.grpc.discovery.NodeInfos;
import com.baidu.hugegraph.pd.grpc.discovery.Query;
import com.baidu.hugegraph.registerimpl.PdRegister;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

public class PdRegisterExample {

    private static final Logger LOG = Log.logger(PdRegisterExample.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Example1 start!");

        String appName = "hugegraph"; //cluster name
        String peer = "127.0.0.1:8686";
        
        PdRegister register = PdRegister.getInstance();
        RegisterConfig config = new RegisterConfig();
        config.setAppName(appName);
        config.setGrpcAddress(peer);
        config.setUrls(new HashSet<>(Arrays.asList("127.0.0.1:8080")));
        config.setNodeName("127.0.0.1");
        config.setNodePort("23456");
        config.setLabelMap(ImmutableMap.of(
            "REGISTER_TYPE", "DDS", // DDS  NODE_PORT
            "GRAPHSPACE", "hg1",
            "SERVICE_NAME", "sv1" 
        ));
        config.setDdsHost("127.0.0.1:2399");
        
        String serviceId = register.registerService(config);

        try {
            Thread.sleep(1000 * 15);
        } catch (Throwable t) {

        }

        DiscoveryClient client = DiscoveryClientImpl.newBuilder().setAppName(appName).setCenterAddress(peer).build();
        Query query = Query.newBuilder().setAppName(appName).build();
        NodeInfos infos = client.getNodeInfos(query);

        
        /*
        SampleRegister register = new SampleRegister();
        String serviceId = register.init("hugegraph");
        */

        System.out.println(serviceId);

        try {
            Thread.sleep(30 * 1000);
        } catch (Exception e) {

        }


    }

}
