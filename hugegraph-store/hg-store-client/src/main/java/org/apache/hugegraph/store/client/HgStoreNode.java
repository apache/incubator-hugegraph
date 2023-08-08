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

package org.apache.hugegraph.store.client;

import org.apache.hugegraph.store.HgStoreSession;

/**
 * created on 2021/10/11
 *
 * @version 0.2.0
 */
public interface HgStoreNode {

    /**
     * Return boolean value of being online or not
     *
     * @return
     */
    default boolean isHealthy() {
        return true;
    }

    /**
     * Return the unique ID of store-node.
     *
     * @return
     */
    Long getNodeId();

    /**
     * A string value concatenated by host and port: "host:port"
     *
     * @return
     */
    String getAddress();

    /**
     * Return a new HgStoreSession instance, that is not Thread safe.
     * Return null when the node is not in charge of the graph that was passed from argument.
     *
     * @return
     */
    HgStoreSession openSession(String graphName);

}