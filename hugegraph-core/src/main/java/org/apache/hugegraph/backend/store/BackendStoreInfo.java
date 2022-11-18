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

package org.apache.hugegraph.backend.store;

import org.apache.hugegraph.config.HugeConfig;
import org.slf4j.Logger;

import org.apache.hugegraph.util.Log;

public class BackendStoreInfo {

    private static final Logger LOG = Log.logger(BackendStoreInfo.class);

    private final BackendStoreProvider storeProvider;

    private final HugeConfig config;

    public BackendStoreInfo(HugeConfig config,
                            BackendStoreProvider storeProvider) {
        this.config = config;
        this.storeProvider = storeProvider;
    }

    public boolean exists() {
        return this.storeProvider.initialized();
    }

    public boolean checkVersion() {
        String driverVersion = this.storeProvider.driverVersion();
        String storedVersion = this.storeProvider.loadSystemStore(this.config)
                                                 .storedVersion();
        if (!driverVersion.equals(storedVersion)) {
            LOG.error("The backend driver version '{}' is inconsistent with " +
                      "the data version '{}' of backend store for graph '{}'",
                      driverVersion, storedVersion, this.storeProvider.graph());
            return false;
        }
        return true;
    }
}
