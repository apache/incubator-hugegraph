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

import static org.apache.hugegraph.it.base.ClusterConstant.IT_LOG_PATH;
import static org.apache.hugegraph.it.base.ClusterConstant.TARGET;
import static org.apache.hugegraph.it.base.ClusterConstant.TEMPLATE_NODE_DIR;
import static org.apache.hugegraph.it.base.ClusterConstant.USER_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.hugegraph.it.base.HGTestLogger;
import org.slf4j.Logger;

public abstract class AbstractNodeWrapper implements BaseNodeWrapper {

    protected static final Logger LOG = HGTestLogger.LOG;

    protected final String host;
    protected final int port;
    protected final Properties properties = new Properties();
    protected final long startTime;
    protected int clusterIndex;
    protected String workPath;
    protected String configPath;
    protected Process instance;
    protected int cnt;

    public AbstractNodeWrapper() {
        this.startTime = System.currentTimeMillis();
        this.clusterIndex = 1;
        this.host = "127.0.0.1";
        this.port = 8086;
    }

    protected AbstractNodeWrapper(String host, int port, int clusterIndex, int cnt) {
        this.host = host;
        this.port = port;
        this.clusterIndex = clusterIndex;
        this.startTime = System.currentTimeMillis();
        this.cnt = cnt;
    }

    /**
     * Node Dir should be created before changing Config
     */
    @Override
    public void createNodeDir() {
        String destDir = getNodePath();
        try {
            try {
                if (new File(destDir).exists()) {
                    PathUtils.delete(Paths.get(destDir));
                }
            } catch (NoSuchFileException fileException) {
                //no need to handle
            }
            // To avoid following symbolic links
            try (Stream<Path> stream = Files.walk(Paths.get(TEMPLATE_NODE_DIR))) {
                stream.forEach(
                        source -> {
                            Path destination =
                                    Paths.get(destDir,
                                              source.toString()
                                                    .substring(TEMPLATE_NODE_DIR.length()));
                            try {
                                Files.copy(source,
                                           destination,
                                           LinkOption.NOFOLLOW_LINKS,
                                           StandardCopyOption.COPY_ATTRIBUTES);
                            } catch (IOException ioException) {
                                LOG.error("Fail to copy files to node dest dir", ioException);
                                throw new RuntimeException(ioException);
                            }
                        }
                );
            }
        } catch (IOException ioException) {
            LOG.error("Got error copying files to node dest dir", ioException);
            throw new AssertionError();
        }
    }

    @Override
    public void createLogDir() {
        String logPath = getLogPath();
        try {
            FileUtils.createParentDirectories(new File(logPath));
        } catch (IOException e) {
            LOG.error("Create log dir failed", e);
            throw new AssertionError();
        }
    }

    public void deleteDir() {
        try {
            PathUtils.deleteDirectory(Paths.get(getNodePath()));
        } catch (IOException ex) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ex.printStackTrace();
                throw new AssertionError("Delete node dir failed. " + e);
            }
        }
    }

    /**
     * @return (user.dir).target.id
     */
    public String getNodePath() {
        return System.getProperty(USER_DIR) + File.separator + TARGET + File.separator +
               getID();
    }

    public String getLogPath() {
        return IT_LOG_PATH + getID() + "-start.log";
    }

    public void updateWorkPath(String workPath) {
        this.workPath = workPath;
    }

    public void updateConfigPath(String ConfigPath) {
        this.configPath = ConfigPath;
    }

    public void stop() {
        if (this.instance == null) {
            return;
        }
        this.instance.destroy();
        try {
            if (!this.instance.waitFor(20, TimeUnit.SECONDS)) {
                this.instance.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Waiting node to shutdown error.", e);
        }
    }

    public boolean isAlive() {
        return this.instance.isAlive();
    }
}
