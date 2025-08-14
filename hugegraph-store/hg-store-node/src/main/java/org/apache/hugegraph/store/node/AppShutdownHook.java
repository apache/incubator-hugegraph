/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;

import lombok.extern.slf4j.Slf4j;

/**
 * copy from web
 */
@Slf4j
public class AppShutdownHook extends Thread {

    private static final String msg =
            "there are still uninterruptible jobs that have not been completed and" +
            " will wait for them to complete";
    private final Thread mainThread;
    private boolean shutDownSignalReceived;

    public AppShutdownHook(Thread mainThread) {
        super();
        this.mainThread = mainThread;
        this.shutDownSignalReceived = false;
        Runtime.getRuntime().addShutdownHook(this);
    }

    @Override
    public void run() {
        System.out.println("Shut down signal received.");
        this.shutDownSignalReceived = true;
        mainThread.interrupt();

        doSomethingForShutdown();

        try {
            mainThread.join(); // Wait for mainThread to finish when a stop signal is received.
        } catch (InterruptedException ignored) {
        }
        System.out.println("Shut down complete.");
    }

    public boolean shouldShutDown() {
        return shutDownSignalReceived;
    }

    /**
     * shutdown method
     * 1. check uninterruptible job
     * 2. check compaction job
     */
    private void doSomethingForShutdown() {
        checkUninterruptibleJobs();
        checkCompactJob();
        //RocksDBFactory.getInstance().releaseAllGraphDB();
    }

    private void checkUninterruptibleJobs() {
        ThreadPoolExecutor jobs = HgStoreEngine.getUninterruptibleJobs();
        try {
            long lastPrint = System.currentTimeMillis() - 5000;
            log.info("check for ongoing background jobs that cannot be interrupted....");
            while (jobs.getActiveCount() != 0 || !jobs.getQueue().isEmpty()) {
                synchronized (AppShutdownHook.class) {
                    if (System.currentTimeMillis() - lastPrint > 5000) {
                        log.warn(msg);
                        lastPrint = System.currentTimeMillis();
                    }
                    try {
                        AppShutdownHook.class.wait(200);
                    } catch (InterruptedException e) {
                    }
                }
            }
        } catch (Exception e) {

        }
        try {
            jobs.shutdownNow();
        } catch (Exception e) {

        }
        log.info("all ongoing background jobs have been completed and the shutdown will continue");
    }

    private void checkCompactJob() {
        ThreadPoolExecutor jobs = BusinessHandlerImpl.getCompactionPool();
        try {
            jobs.shutdownNow();
        } catch (Exception e) {

        }
        log.info("closed compact job pool");
    }
}
