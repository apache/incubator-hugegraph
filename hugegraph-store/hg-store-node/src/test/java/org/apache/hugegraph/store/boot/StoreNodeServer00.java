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

package org.apache.hugegraph.store.boot;

import java.io.File;
import java.util.Objects;

import org.apache.hugegraph.store.node.StoreNodeApplication;
import org.springframework.boot.SpringApplication;

import com.alipay.remoting.util.StringUtils;

public class StoreNodeServer00 {

    public static void main(String[] args) {
        // deleteDir(new File("tmp/8500"));
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs/8500");
            System.setProperty("com.alipay.remoting.client.log.level", "WARN");
        }
        if (System.getProperty("bolt.channel_write_buf_low_water_mark") == null) {
            System.setProperty("bolt.channel_write_buf_low_water_mark",
                               Integer.toString(4 * 1024 * 1024));
        }
        if (System.getProperty("bolt.channel_write_buf_high_water_mark") == null) {
            System.setProperty("bolt.channel_write_buf_high_water_mark",
                               Integer.toString(8 * 1024 * 1024));
        }
        SpringApplication.run(StoreNodeApplication.class, "--spring.profiles.active=server00");
        System.out.println("StoreNodeServer00 started.");
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
