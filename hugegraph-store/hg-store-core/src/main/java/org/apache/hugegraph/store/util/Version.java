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

package org.apache.hugegraph.store.util;

import java.io.InputStream;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Version {

    private static String version = "";

    /**
     * 软件版本号
     */
    public static String getVersion() {
        if (version.isEmpty()) {
            try (InputStream is = Version.class.getResourceAsStream("/version.txt")) {
                byte[] buf = new byte[64];
                int len = is.read(buf);
                version = new String(buf, 0, len);
            } catch (Exception e) {
                log.error("Version.getVersion exception: ", e);
            }
        }
        return version;
    }

    /**
     * 存储格式版本号
     */
    public static int getDataFmtVersion() {
        return 1;
    }
}
