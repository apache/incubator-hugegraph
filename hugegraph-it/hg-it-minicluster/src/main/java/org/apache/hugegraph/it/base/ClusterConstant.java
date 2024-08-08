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

package org.apache.hugegraph.it.base;

import java.io.File;

import org.apache.commons.lang3.SystemUtils;

public class ClusterConstant {

    public static final String USER_DIR = "user.dir";
    public static final String TARGET = "target";
    public static final String LOG = "logs";
    public static final String SIMPLE = "simple";
    public static final String MULTI = "multi";

    public static final String LIB_DIR = "lib";
    public static final String EXT_DIR = "ext";
    public static final String PLUGINS_DIR = "plugins";

    public static final String CONF_DIR = "conf";
    public static final String LOGS_DIR = "logs";
    public static final boolean OPEN_SECURITY_CHECK = true;
    public static final boolean OPEN_TELEMETRY = false;
    public static final boolean IS_WINDOWS = SystemUtils.IS_OS_WINDOWS;
    public static final String PD_PACKAGE_PREFIX = "hugegraph-pd";
    public static final String PD_JAR_PREFIX = "hg-pd-service";
    public static final String STORE_PACKAGE_PREFIX = "hugegraph-store";
    public static final String STORE_JAR_PREFIX = "hg-store-node";
    public static final String SERVER_PACKAGE_PREFIX = "apache-hugegraph-incubating";
    public static final String IT_PACKAGE_PREFIX = "apache-hugegraph-incubating-it";

    public static final String TEMPLATE_NODE_DIR =
            System.getProperty(USER_DIR) + File.separator + TARGET + File.separator
            + "template-node";

    public static final String JAVA_CMD =
            System.getProperty("java.home")
            + File.separator
            + "bin"
            + File.separator
            + (SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");

    public static final String PD_DIST_PATH =
            System.getProperty("user.dir")
            + File.separator
            + "hugegraph-pd"
            + File.separator
            + "dist"
            + File.separator;

    public static final String PD_WORK_PATH =
            getFileInDir(PD_DIST_PATH, PD_PACKAGE_PREFIX)
            + File.separator
            + LIB_DIR
            + File.separator;

    public static final String STORE_DIST_PATH =
            System.getProperty("user.dir")
            + File.separator
            + "hugegraph-store"
            + File.separator
            + "dist"
            + File.separator;

    public static final String STORE_WORK_PATH =
            getFileInDir(STORE_DIST_PATH, STORE_PACKAGE_PREFIX)
            + File.separator
            + "lib"
            + File.separator;

    public static final String SERVER_DIST_PATH =
            System.getProperty("user.dir")
            + File.separator
            + "hugegraph-server"
            + File.separator;

    public static final String SERVER_WORK_PATH =
            getFileInDir(SERVER_DIST_PATH, SERVER_PACKAGE_PREFIX)
            + File.separator;

    public static final String IT_DIST_PATH =
            System.getProperty("user.dir")
            + File.separator
            + "hugegraph-it"
            + File.separator;

    public static final String IT_CONFIG_PATH =
            getFileInDir(IT_DIST_PATH, IT_PACKAGE_PREFIX)
            + File.separator
            + CONF_DIR
            + File.separator;

    public static final String SIMPLE_PD_CONFIG_PATH =
            IT_CONFIG_PATH
            + SIMPLE
            + File.separator
            + "pd"
            + File.separator;

    public static final String SIMPLE_STORE_CONFIG_PATH =
            IT_CONFIG_PATH
            + SIMPLE
            + File.separator
            + "store"
            + File.separator;

    public static final String SIMPLE_SERVER_CONFIG_PATH =
            IT_CONFIG_PATH
            + SIMPLE
            + File.separator
            + "server"
            + File.separator;

    public static final String IT_LOG_PATH =
            getFileInDir(IT_DIST_PATH, IT_PACKAGE_PREFIX)
            + File.separator
            + LOG
            + File.separator;

    private ClusterConstant() {
        throw new IllegalStateException("Utility class");
    }

    public static String getFileInDir(String path, String fileName) {
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (file.getName().startsWith(fileName)) {
                    return path + file.getName();
                }
            }
        }
        return "";
    }
}
