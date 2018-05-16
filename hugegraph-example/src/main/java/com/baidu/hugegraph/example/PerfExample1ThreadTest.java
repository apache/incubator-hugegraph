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

import com.baidu.hugegraph.util.Log;

public class PerfExample1ThreadTest {

    private static final Logger LOG = Log.logger(PerfExample1ThreadTest.class);

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 5) {
            System.out.println("Usage: minThread maxThread threadStep " +
                               "times multiple");
            return;
        }

        int minThread = Integer.parseInt(args[0]);
        int maxThread = Integer.parseInt(args[1]);
        int threadStep = Integer.parseInt(args[2]);
        int times = Integer.parseInt(args[3]);
        int multiple = Integer.parseInt(args[4]);

        args = new String[3];
        for (int i = minThread; i <= maxThread; i += threadStep) {
            args[0] = String.valueOf(i);
            args[1] = String.valueOf(times);
            args[2] = String.valueOf(multiple);

            LOG.info("===================================");
            LOG.info("threads: {}, times: {}, multiple: {}",
                     i, times, multiple);
            PerfExample1.main(args);
        }

        System.exit(0);
    }
}
