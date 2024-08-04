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

import java.io.File;

import org.apache.hugegraph.it.node.PDNodeWrapper;
import org.apache.hugegraph.it.node.ServerNodeWrapper;
import org.apache.hugegraph.it.node.StoreNodeWrapper;

public class SimpleEnv extends AbstractEnv {

    String pdWorkPath, pdConfigPath, storeWorkPath, storeConfigPath, serverWorkPath,
            serverConfigPath;

    public SimpleEnv() {
        PDNodeWrapper pdNodeWrapper = new PDNodeWrapper("127.0.0.1", 8686, 1, 1);
        StoreNodeWrapper storeNodeWrapper = new StoreNodeWrapper(1, 1);
        ServerNodeWrapper serverNodeWrapper = new ServerNodeWrapper(1, 1);

        pdWorkPath =
                System.getProperty("user.dir")
                + File.separator
                + "hugegraph-pd"
                + File.separator
                + "dist"
                + File.separator;
        File directory = new File(pdWorkPath);
        String osName = System.getProperty("os.name").toLowerCase();
        if (directory.exists() && directory.isDirectory() && !osName.contains("win")) {
            File[] files = directory.listFiles();
            for (File file : files) {
                if (file.getName().startsWith("hugegraph-pd") && file.isDirectory()) {
                    pdWorkPath += file.getName() + File.separator;
                    break;
                }
            }
        }
        pdNodeWrapper.updateWorkPath(pdWorkPath);
        pdNodeWrapper.updateConfigPath(pdWorkPath);
        super.addPDNode(pdNodeWrapper);

        storeWorkPath = System.getProperty("user.dir")
                        + File.separator
                        + "hugegraph-store"
                        + File.separator
                        + "dist"
                        + File.separator;
        directory = new File(storeWorkPath);
        if (directory.exists() && directory.isDirectory() && !osName.contains("win")) {
            File[] files = directory.listFiles();
            for (File file : files) {
                if (file.getName().startsWith("hugegraph-store") && file.isDirectory()) {
                    storeWorkPath += file.getName() + File.separator;
                    break;
                }
            }
        }
        storeNodeWrapper.updateWorkPath(storeWorkPath);
        storeNodeWrapper.updateConfigPath(storeWorkPath);
        super.addStoreNode(storeNodeWrapper);

        serverConfigPath = System.getProperty("user.dir")
                           + File.separator
                           + "hugegraph-server"
                           + File.separator;
        directory = new File(serverConfigPath);
        if (directory.exists() && directory.isDirectory() && !osName.contains("win")) {
            File[] files = directory.listFiles();
            for (File file : files) {
                if (file.getName().startsWith("apache-hugegraph") && file.isDirectory()) {
                    serverConfigPath += file.getName() + File.separator;
                    break;
                }
            }
        }
        serverNodeWrapper.updateWorkPath(serverConfigPath);
        serverNodeWrapper.updateConfigPath(serverConfigPath);
        super.addServerNode(serverNodeWrapper);
    }
}
