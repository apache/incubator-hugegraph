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

package org.apache.hugegraph.it.node;

import static org.apache.hugegraph.it.base.ClusterConstant.EXT_DIR;
import static org.apache.hugegraph.it.base.ClusterConstant.JAVA_CMD;
import static org.apache.hugegraph.it.base.ClusterConstant.LIB_DIR;
import static org.apache.hugegraph.it.base.ClusterConstant.PLUGINS_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class ServerNodeWrapper extends AbstractNodeWrapper {

    public ServerNodeWrapper(int clusterIndex, int cnt) {
        this.clusterIndex = clusterIndex;
        this.cnt = cnt;
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
    public void createNodeDir() {
        super.createNodeDir();
    }

    @Override
    public void createLogDir() {
        super.createLogDir();
    }

    @Override
    public void deleteDir() {
        super.deleteDir();
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
                            "-Dname=HugeGraphServer" + this.cnt,
                            "--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED",
                            "-cp", storeClassPath,
                            "org.apache.hugegraph.dist.HugeGraphServer",
                            "./gremlin-server.yaml",
                            "./rest-server.properties"));
            FileUtils.write(
                    stdoutFile, String.join(" ", startCmd) + "\n\n", StandardCharsets.UTF_8, true);
            ProcessBuilder processBuilder =
                    new ProcessBuilder(startCmd)
                            .redirectOutput(ProcessBuilder.Redirect.appendTo(stdoutFile))
                            .redirectError(ProcessBuilder.Redirect.appendTo(stdoutFile));
            processBuilder.directory(new File(configPath));
            this.instance = processBuilder.start();
        } catch (IOException ex) {
            throw new AssertionError("Start node failed. " + ex);
        }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public boolean isAlive() {
        return super.isAlive();
    }

    @Override
    public String getID() {
        return "Server" + this.cnt;
    }
}
