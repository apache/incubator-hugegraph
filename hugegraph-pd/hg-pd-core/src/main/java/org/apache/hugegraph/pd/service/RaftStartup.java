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

package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.store.HgKVStore;

/**
 * Starts the shared {@link RaftEngine} from any service's startup callback.
 */
public final class RaftStartup {

    private RaftStartup() {
    }

    /**
     * Open the metadata store before Raft. Its open waits for a LOCK still held by a stopping
     * PD, and that PD closes Raft before metadata, so the raft log LOCK is free once the
     * metadata store is open. Spring runs the service callbacks in no fixed order, so each of
     * them starts Raft through here. If Raft then fails to start, a metadata store this call
     * opened is closed again, since a callback other than PDService has no owner to close it.
     */
    public static void init(PDConfig pdConfig) {
        HgKVStore opened = MetadataFactory.openStore(pdConfig);
        try {
            RaftEngine.getInstance().init(pdConfig.getRaft());
        } catch (RuntimeException | Error failure) {
            // Only a store this call opened is ours to release; PDService and the
            // shutdown hook own the rest
            if (opened != null) {
                try {
                    // Join Raft before closing metadata, as on shutdown; a failed join
                    // keeps the database open
                    RaftEngine.getInstance().shutDown();
                    MetadataFactory.discardStore(opened);
                } catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) {
                        failure.addSuppressed(cleanup);
                    }
                }
            }
            throw failure;
        }
    }
}
