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

package org.apache.hugegraph.dist;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.sun.management.ThreadMXBean;

public class MemoryMonitor {

    private static final Logger LOG = Log.logger(MemoryMonitor.class);
    private final double MEMORY_MONITOR_THRESHOLD;
    private final int MEMORY_MONITOR_PERIOD;
    private final ScheduledExecutorService scheduler;

    public MemoryMonitor(String restServerConf) {
        HugeConfig restServerConfig = new HugeConfig(restServerConf);
        MEMORY_MONITOR_THRESHOLD =
                restServerConfig.get(ServerOptions.JVM_MEMORY_MONITOR_THRESHOLD);
        MEMORY_MONITOR_PERIOD =
                restServerConfig.get(ServerOptions.JVM_MEMORY_MONITOR_PERIOD);
        scheduler = Executors.newScheduledThreadPool(1);
    }

    private void manageMemory() {
        double memoryUsagePercentage = getMemoryUsagePercentage();

        if (memoryUsagePercentage > MEMORY_MONITOR_THRESHOLD) {
            LOG.info("JVM memory usage is '{}', exceeding the threshold of '{}'.",
                     memoryUsagePercentage, MEMORY_MONITOR_THRESHOLD);
            System.gc();
            LOG.info("Trigger System.gc()");

            double doubleCheckUsage = getMemoryUsagePercentage();
            if (doubleCheckUsage > MEMORY_MONITOR_THRESHOLD) {
                Thread targetThread = getHighestMemoryThread();
                if (targetThread != null) {
                    LOG.info("JVM memory usage is '{}', exceeding the threshold of '{}'.",
                             doubleCheckUsage, MEMORY_MONITOR_THRESHOLD);
                    targetThread.interrupt();
                    LOG.info("Send interrupt to '{}' thread",
                             targetThread.getName());
                }
            }
        }
    }

    private double getMemoryUsagePercentage() {
        MemoryUsage heapMemoryUsage =
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return (double) heapMemoryUsage.getUsed() / heapMemoryUsage.getMax();
    }

    private Thread getHighestMemoryThread() {
        long highestMemory = 0;
        Thread highestThread = null;

        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getState() != Thread.State.RUNNABLE ||
                thread.getName() == null ||
                !thread.getName().startsWith("grizzly-http-server-")) {
                continue;
            }

            long threadMemory =
                    threadMXBean.getThreadAllocatedBytes(thread.getId());
            if (threadMemory > highestMemory) {
                highestMemory = threadMemory;
                highestThread = thread;
            }
        }

        return highestThread;
    }

    public void start() {
        if (MEMORY_MONITOR_THRESHOLD >= 1.0) {
            return;
        }
        Runnable task = this::manageMemory;
        scheduler.scheduleAtFixedRate(task, 0, MEMORY_MONITOR_PERIOD,
                                      TimeUnit.MILLISECONDS);
        LOG.info("Memory monitoring task started.");
    }

    public void stop() {
        if (MEMORY_MONITOR_THRESHOLD >= 1.0) {
            return;
        }
        scheduler.shutdownNow();
        LOG.info("Memory monitoring task ended.");
    }
}
