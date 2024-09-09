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

package org.apache.hugegraph.ct.node;

import static org.apache.hugegraph.ct.base.ClusterConstant.COMPUTER_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.CONF_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.EMPTY_SAMPLE_GROOVY_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.EXAMPLE_GROOVY_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.EXT_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.GREMLIN_DRIVER_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.GREMLIN_SERVER_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.HUGEGRAPH_SERVER_KEYSTORE;
import static org.apache.hugegraph.ct.base.ClusterConstant.JAVA_CMD;
import static org.apache.hugegraph.ct.base.ClusterConstant.LIB_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.LOG4J_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.PLUGINS_DIR;
import static org.apache.hugegraph.ct.base.ClusterConstant.REMOTE_OBJECTS_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.REMOTE_SETTING_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.SERVER_LIB_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.isJava11OrHigher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ServerNodeWrapper extends AbstractNodeWrapper {

    public ServerNodeWrapper(int clusterIndex, int index) {
        super();
        this.clusterIndex = clusterIndex;
        this.index = index;
        this.fileNames = new ArrayList<>(
                List.of(LOG4J_FILE,
                        HUGEGRAPH_SERVER_KEYSTORE,
                        COMPUTER_SETTING_FILE,
                        GREMLIN_SERVER_FILE,
                        GREMLIN_DRIVER_SETTING_FILE,
                        REMOTE_SETTING_FILE,
                        REMOTE_OBJECTS_SETTING_FILE));
        this.workPath = SERVER_LIB_PATH;
        createNodeDir(getNodePath() + CONF_DIR + File.separator);
        this.fileNames = new ArrayList<>(
                List.of(EMPTY_SAMPLE_GROOVY_FILE,
                        EXAMPLE_GROOVY_FILE)
        );
        this.startLine = "INFO: [HttpServer] Started.";
        createNodeDir(getNodePath());
        createLogDir();
    }

    private static void addJarsToClasspath(File directory, List<String> classpath) {
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles((dir, name) -> name.endsWith(".jar"));
            if (files != null) {
                for (File file : files) {
                    classpath.add(file.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public void start() {
        try {
            File stdoutFile = new File(getLogPath());
            List<String> startCmd = new ArrayList<>();
            startCmd.add(JAVA_CMD);
            if (!isJava11OrHigher()) {
                LOG.error("Please make sure that the JDK is installed and the version >= 11");
                return;
            }

            List<String> classpath = new ArrayList<>();
            addJarsToClasspath(new File(workPath + LIB_DIR), classpath);
            addJarsToClasspath(new File(workPath + EXT_DIR), classpath);
            addJarsToClasspath(new File(workPath + PLUGINS_DIR), classpath);
            String storeClassPath =
                    String.join(":", classpath);
            startCmd.addAll(
                    Arrays.asList(
                            "-Dname=HugeGraphServer" + this.index,
                            "--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED",
                            "-cp", storeClassPath,
                            "org.apache.hugegraph.dist.HugeGraphServer",
                            "./conf/gremlin-server.yaml",
                            "./conf/rest-server.properties"));
            ProcessBuilder processBuilder = runCmd(startCmd, stdoutFile);
            this.instance = processBuilder.start();
        } catch (IOException ex) {
            throw new AssertionError("Start node failed. " + ex);
        }
    }

    @Override
    public String getID() {
        return "Server" + this.index;
    }
}
