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

package org.apache.hugegraph.store;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.store.client.HgStoreSessionProvider;

/**
 * Maintain HgStoreSession instances.
 * // TODO: Holding more than one HgSessionManager is available,if you want to connect multi
 * HgStore-clusters.
 */

@ThreadSafe
public final class HgSessionManager {
    private final static HgSessionManager INSTANCE = new HgSessionManager();
    private final HgSessionProvider sessionProvider;

    private HgSessionManager() {
        // TODO: constructed by SPI
        this.sessionProvider = new HgStoreSessionProvider();
    }


    public static HgSessionManager getInstance() {
        return INSTANCE;
    }

    /**
     * Retrieve or create a HgStoreSession.
     *
     * @param graphName
     * @return
     */
    public HgStoreSession openSession(String graphName) {
        return this.sessionProvider.createSession(graphName);
    }

}
