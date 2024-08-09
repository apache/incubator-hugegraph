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

import static org.apache.hugegraph.it.base.ClusterConstant.CONF_DIR;
import static org.apache.hugegraph.it.base.ClusterConstant.JAVA_CMD;
import static org.apache.hugegraph.it.base.ClusterConstant.STORE_JAR_PREFIX;
import static org.apache.hugegraph.it.base.ClusterConstant.getFileInDir;
import static org.apache.hugegraph.it.base.ClusterConstant.isJava11OrHigher;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class StoreNodeWrapper extends AbstractNodeWrapper {

    protected StoreNodeWrapper(String host, int port, int clusterIndex, int cnt) {
        super(host, port, clusterIndex, cnt);
    }

    public StoreNodeWrapper(int clusterId, int cnt) {
        this.clusterIndex = clusterId;
        this.cnt = cnt;
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
            String storeNodeJarPath = getFileInDir(workPath, STORE_JAR_PREFIX);
            startCmd.addAll(
                    Arrays.asList(
                            "-Dname=HugeGraphStore" + this.cnt,
                            "-Dlog4j.configurationFile=" + configPath + CONF_DIR
                            + File.separator + "log4j2.xml",
                            "-Dfastjson.parser.safeMode=true",
                            "-Xms512m",
                            "-Xmx2048m",
                            "-XX:MetaspaceSize=256M",
                            "-XX:+UseG1GC",
                            "-XX:+ParallelRefProcEnabled",
                            "-XX:+HeapDumpOnOutOfMemoryError",
                            "-XX:HeapDumpPath=" + configPath + "logs",
                            "-Dspring.config.location=" + configPath + CONF_DIR
                            + File.separator + "application.yml",
                            "-jar", storeNodeJarPath));
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
    public String getID() {
        return "Store" + this.clusterIndex;
    }
}
