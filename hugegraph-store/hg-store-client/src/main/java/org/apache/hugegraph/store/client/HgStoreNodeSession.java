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

package org.apache.hugegraph.store.client;

import org.apache.hugegraph.store.HgStoreSession;

/**
 * created on 2021/10/11
 *
 * @version 0.1.0
 */
public interface HgStoreNodeSession extends HgStoreSession {

    /**
     * Return the name of graph.
     *
     * @return
     */
    String getGraphName();

    /**
     * Return an instance of HgStoreNode, which provided the connection of Store-Node machine.
     *
     * @return
     */
    HgStoreNode getStoreNode();


}
