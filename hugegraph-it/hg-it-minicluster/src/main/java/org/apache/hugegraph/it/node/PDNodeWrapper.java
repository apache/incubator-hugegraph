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

import static org.apache.hugegraph.it.base.ClusterConstant.JAVA_CMD;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.it.base.ClusterConstant;
import org.apache.hugegraph.pd.config.PDConfig;

public class PDNodeWrapper extends AbstractNodeWrapper {

    private PDConfig pdConfig;

    public PDNodeWrapper() {
        pdConfig = new PDConfig();
    }

    public PDNodeWrapper(String host, int grpcPort, int clusterIndex, int cnt) {
        super(host, grpcPort, clusterIndex, cnt);
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

    public void updatePDConfig(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
    }

    public void updateServerPost(int serverPort) {
        this.pdConfig.getRaft().setPort(serverPort);
    }

    public void updateDataPath(String dataPath) {
        this.pdConfig.setDataPath(dataPath);
    }

    public void updatePatrolInterval(int patrolInterval) {
        this.pdConfig.setPatrolInterval(patrolInterval);
    }

    public void updateInitialStoreList(String storeList) {
        this.pdConfig.setInitialStoreList(storeList);
    }

    public void updateInitialStoreCount(int minStoreCount) {
        this.pdConfig.setMinStoreCount(minStoreCount);
    }

    public void updateRaftAddress(String raftAdd) {
        this.pdConfig.getRaft().setAddress(raftAdd);
    }

    public void updatePeersList(String peersList) {
        this.pdConfig.getRaft().setPeersList(peersList);
    }

    /*
    workPath is path of JAR package, configPath is path of config files
     */
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
            String pdNodeJarPath = workPath + "lib" + File.separator;
            File jarDir = new File(pdNodeJarPath);
            File[] jarFiles = jarDir.listFiles();
            for (File jarFile : jarFiles) {
                if (jarFile.getName().startsWith("hg-pd-service")) {
                    pdNodeJarPath += jarFile.getName();
                    break;
                }
            }
            startCmd.addAll(
                    Arrays.asList(
                            "-Dname=HugeGraphPD" + this.cnt,
                            "-Xms512m",
                            "-Xmx4g",
                            "-XX:+HeapDumpOnOutOfMemoryError",
                            "-XX:HeapDumpPath=" + workPath + "logs",
                            "-Dlog4j.configurationFile=" + configPath + "conf" + File.separator +
                            "log4j2.xml",
                            "-Dspring.config.location=" + configPath + "conf" + File.separator +
                            "application.yml",
                            "-jar", pdNodeJarPath));
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
    public String getLogPath() {
        return this.workPath + ClusterConstant.LOG + File.separator + "pd-start.log";
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
        return "PD" + this.cnt;
    }
}
