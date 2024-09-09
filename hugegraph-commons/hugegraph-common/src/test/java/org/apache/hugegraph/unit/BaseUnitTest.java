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

package org.apache.hugegraph.unit;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.util.ExceptionUtil;
import org.apache.hugegraph.util.TimeUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseUnitTest {

    @BeforeClass
    public static void init() {
        // pass
    }

    @AfterClass
    public static void clear() throws Exception {
        // pass
    }

    protected static void runWithThreads(int threads, Runnable task) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(task));
        }
        for (Future<?> future : futures) {
            ExceptionUtil.futureGet(future);
        }
    }

    protected static void waitTillNext(long seconds) {
        TimeUtil.tillNextMillis(TimeUtil.timeGen() + seconds * 1000);
    }

    public static void downloadFileByUrl(String url, String destPath) {
        int connectTimeout = 5000;
        int readTimeout = 5000;
        try {
            FileUtils.copyURLToFile(new URL(url), new File(destPath), connectTimeout, readTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
