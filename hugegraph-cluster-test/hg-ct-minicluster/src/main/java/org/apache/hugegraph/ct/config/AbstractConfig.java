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

package org.apache.hugegraph.ct.config;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.ct.base.HGTestLogger;
import org.slf4j.Logger;

public abstract class AbstractConfig {

    protected static final Logger LOG = HGTestLogger.LOG;
    protected String config;
    protected Map<String, String> properties = new HashMap<>();
    protected String fileName;

    protected void readTemplate(String filePath) {
        try {
            this.config = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            LOG.error("failed to get file", e);
        }
    }

    protected void updateConfigs() {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String placeholder = "$" + entry.getKey() + "$";
            config = config.replace(placeholder, entry.getValue());
        }
    }

    public void writeConfig(String filePath) {
        updateConfigs();
        String destPath = filePath + File.separator + fileName;
        try {
            if (Files.notExists(Path.of(destPath).getParent())) {
                Files.createDirectories(Path.of(destPath).getParent());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileWriter writer = new FileWriter(destPath)) {
            writer.write(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    protected void setProperty(String propertyName, String value) {
        if (properties.containsKey(propertyName)) {
            properties.replace(propertyName, value);
        } else {
            properties.put(propertyName, value);
        }
    }
}
