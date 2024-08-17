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

package org.apache.hugegraph.ct.base;

import java.io.File;

import org.apache.commons.lang3.SystemUtils;

public class ClusterConstant {

    public static final String USER_DIR = "user.dir";
    public static final String LOG = "logs";

    public static final String LIB_DIR = "lib";
    public static final String EXT_DIR = "ext";
    public static final String PLUGINS_DIR = "plugins";
    public static final String BIN_DIR = "bin";
    public static final String DIST_DIR = "dist";
    public static final String CONF_DIR = "conf";

    public static final String PD_PACKAGE_PREFIX = "hugegraph-pd";
    public static final String PD_JAR_PREFIX = "hg-pd-service";
    public static final String STORE_PACKAGE_PREFIX = "hugegraph-store";
    public static final String STORE_JAR_PREFIX = "hg-store-node";
    public static final String SERVER_PACKAGE_PREFIX = "apache-hugegraph-incubating";
    public static final String CT_PACKAGE_PREFIX = "apache-hugegraph-incubating-ct";

    public static final String APPLICATION_FILE = "application.yml";
    public static final String SERVER_PROPERTIES = "rest-server.properties";
    public static final String HUGEGRAPH_PROPERTIES = "graphs/hugegraph.properties";
    public static final String LOG4J_FILE = "log4j2.xml";
    public static final String VERIFY_LICENSE_FILE = "verify-license.json";
    public static final String LICENSE_FILE = "hugegraph.license";
    public static final String PD_TEMPLATE_FILE = "pd-application.yml.template";
    public static final String STORE_TEMPLATE_FILE = "store-application.yml.template";
    public static final String SERVER_TEMPLATE_FILE = "rest-server.properties.template";
    public static final String GREMLIN_DRIVER_SETTING_FILE = "gremlin-driver-settings.yaml";
    public static final String GREMLIN_SERVER_FILE = "gremlin-server.yaml";
    public static final String COMPUTER_SETTING_FILE = "computer.yaml";
    public static final String HUGEGRAPH_SERVER_KEYSTORE = "hugegraph-server.keystore";
    public static final String REMOTE_SETTING_FILE = "remote.yaml";
    public static final String REMOTE_OBJECTS_SETTING_FILE = "remote-objects.yaml";

    public static final String JAVA_CMD =
            System.getProperty("java.home")
            + File.separator
            + BIN_DIR
            + File.separator
            + (SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");

    public static final String PD_DIST_PATH =
            System.getProperty(USER_DIR)
            + File.separator
            + "hugegraph-pd"
            + File.separator
            + DIST_DIR
            + File.separator;

    public static final String PD_LIB_PATH =
            getFileInDir(PD_DIST_PATH, PD_PACKAGE_PREFIX)
            + File.separator
            + LIB_DIR
            + File.separator;

    public static final String STORE_DIST_PATH =
            System.getProperty(USER_DIR)
            + File.separator
            + "hugegraph-store"
            + File.separator
            + DIST_DIR
            + File.separator;

    public static final String STORE_LIB_PATH =
            getFileInDir(STORE_DIST_PATH, STORE_PACKAGE_PREFIX)
            + File.separator
            + LIB_DIR
            + File.separator;

    public static final String SERVER_DIST_PATH =
            System.getProperty(USER_DIR)
            + File.separator
            + "hugegraph-server"
            + File.separator;

    public static final String SERVER_LIB_PATH =
            getFileInDir(SERVER_DIST_PATH, SERVER_PACKAGE_PREFIX)
            + File.separator;

    public static final String CT_DIST_PATH =
            System.getProperty(USER_DIR)
            + File.separator
            + "hugegraph-cluster-test"
            + File.separator;

    public static final String CT_PACKAGE_PATH =
            getFileInDir(CT_DIST_PATH, CT_PACKAGE_PREFIX)
            + File.separator;

    public static final String IT_LOG_PATH =
            CT_PACKAGE_PATH
            + LOG
            + File.separator;

    private ClusterConstant() {
        throw new IllegalStateException("Utility class");
    }

    public static String getFileInDir(String path, String fileName) {
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (file.getName().startsWith(fileName) && !file.getName().endsWith(".gz")) {
                    return path + file.getName();
                }
            }
        }
        return "";
    }

    public static boolean isJava11OrHigher() {
        String version = System.getProperty("java.version");
        if (version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if (dot != -1) {
                version = version.substring(0, dot);
            }
        }
        int versionNumber = Integer.parseInt(version);
        return versionNumber >= 11;
    }
}