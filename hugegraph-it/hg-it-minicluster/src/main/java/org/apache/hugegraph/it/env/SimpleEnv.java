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

package org.apache.hugegraph.it.env;

import static org.apache.hugegraph.it.base.ClusterConstant.PD_LIB_PATH;
import static org.apache.hugegraph.it.base.ClusterConstant.SERVER_LIB_PATH;
import static org.apache.hugegraph.it.base.ClusterConstant.SIMPLE_PD_CONFIG_PATH;
import static org.apache.hugegraph.it.base.ClusterConstant.SIMPLE_SERVER_CONFIG_PATH;
import static org.apache.hugegraph.it.base.ClusterConstant.SIMPLE_STORE_CONFIG_PATH;
import static org.apache.hugegraph.it.base.ClusterConstant.STORE_LIB_PATH;

import org.apache.hugegraph.it.node.PDNodeWrapper;
import org.apache.hugegraph.it.node.ServerNodeWrapper;
import org.apache.hugegraph.it.node.StoreNodeWrapper;

public class SimpleEnv extends AbstractEnv {

    public SimpleEnv() {
        PDNodeWrapper pdNodeWrapper = new PDNodeWrapper("127.0.0.1", 8686, 1, 1);
        StoreNodeWrapper storeNodeWrapper = new StoreNodeWrapper(1, 1);
        ServerNodeWrapper serverNodeWrapper = new ServerNodeWrapper(1, 1);

        pdNodeWrapper.updateWorkPath(PD_LIB_PATH);
        pdNodeWrapper.updateConfigPath(SIMPLE_PD_CONFIG_PATH);
        super.addPDNode(pdNodeWrapper);

        storeNodeWrapper.updateWorkPath(STORE_LIB_PATH);
        storeNodeWrapper.updateConfigPath(SIMPLE_STORE_CONFIG_PATH);
        super.addStoreNode(storeNodeWrapper);

        serverNodeWrapper.updateWorkPath(SERVER_LIB_PATH);
        serverNodeWrapper.updateConfigPath(SIMPLE_SERVER_CONFIG_PATH);
        super.addServerNode(serverNodeWrapper);
    }

}
