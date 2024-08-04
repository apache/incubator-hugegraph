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

    public static final String LIB_DIR = "lib";
    public static final String EXT_DIR = "ext";
    public static final String PLUGINS_DIR = "plugins";
    public static final String CONF_DIR = "conf";
    public static final String LOGS_DIR = "logs";
    public static final boolean OPEN_SECURITY_CHECK = true;
    public static final boolean OPEN_TELEMETRY = false;

    public static final String TEMPLATE_NODE_DIR =
            System.getProperty(USER_DIR) + File.separator + TARGET + File.separator
            + "template-node";

    public static final String JAVA_CMD =
            System.getProperty("java.home")
            + File.separator
            + "bin"
            + File.separator
            + (SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");

    private ClusterConstant() {
        throw new IllegalStateException("Utility class");
    }
}
