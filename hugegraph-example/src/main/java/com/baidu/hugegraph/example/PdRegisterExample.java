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

import com.baidu.hugegraph.RegisterConfig;
import com.baidu.hugegraph.registerimpl.PdRegister;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

public class PdRegisterExample {

    private static final Logger LOG = Log.logger(PdRegisterExample.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Example1 start!");

     
        
        PdRegister register = PdRegister.getInstance();
        RegisterConfig config = new RegisterConfig();
        config.setAppName("hugegraph");
        config.setGrpcAddress("127.0.0.1:8686");
        config.setNodeName("127.0.0.1");
        config.setNodePort("23456");
        config.setLabelMap(ImmutableMap.of("hello", "world"));
        String serviceId = register.registerService(config);
        

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
