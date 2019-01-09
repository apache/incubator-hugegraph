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

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.util.Log;

public class ThreadRangePerfTest {

    private static final Logger LOG = Log.logger(ThreadRangePerfTest.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println("Usage: minThread maxThread threadStep " +
                               "times multiple profile");
            return;
        }

        int minThread = Integer.parseInt(args[0]);
        int maxThread = Integer.parseInt(args[1]);
        int threadStep = Integer.parseInt(args[2]);
        int times = Integer.parseInt(args[3]);
        int multiple = Integer.parseInt(args[4]);
        boolean profile = Boolean.parseBoolean(args[5]);

        String[] newargs = new String[4];
        for (int i = minThread; i <= maxThread; i += threadStep) {
            int threads = i;
            newargs[0] = String.valueOf(threads);
            newargs[1] = String.valueOf(times);
            newargs[2] = String.valueOf(multiple);
            newargs[3] = String.valueOf(profile);

            LOG.info("===================================");
            LOG.info("threads: {}, times: {}, multiple: {}, profile: {}",
                     threads, times, multiple, profile);
            new PerfExample1().test(newargs);
        }

        // Stop daemon thread
        HugeGraph.shutdown(30L);
    }
}
