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

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;

/**
 * copy from web
 */
public class AppShutdownHook extends Thread {

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
            mainThread.join(); //当收到停止信号时，等待mainThread的执行完成
        } catch (InterruptedException ignored) {
        }
        System.out.println("Shut down complete.");
    }

    public boolean shouldShutDown() {
        return shutDownSignalReceived;
    }

    private void doSomethingForShutdown() {
        RocksDBFactory.getInstance().releaseAllGraphDB();
    }
}
