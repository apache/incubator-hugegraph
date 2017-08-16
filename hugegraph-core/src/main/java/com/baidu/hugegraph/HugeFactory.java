/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.baidu.hugegraph;

import java.io.File;
import java.net.URL;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;


public class HugeFactory {

    public static HugeGraph open(Configuration config) {
        return new HugeGraph(new HugeConfig(config));
    }

    public static HugeGraph open(String path) {
        return open(getLocalConfig(path));
    }

    public static HugeGraph open(URL url) {
        return open(getRemoteConfig(url));
    }

    private static PropertiesConfiguration getLocalConfig(String path) {
        File file = new File(path);
        E.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable config file rather than: %s",
                file.toString());
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(file);
            final File tmpParent = file.getParentFile();
            final File configParent;

            if (tmpParent == null) {
                configParent = new File(System.getProperty("user.dir"));
            } else {
                configParent = tmpParent;
            }

            E.checkNotNull(configParent, "config parent");
            E.checkArgument(configParent.isDirectory(),
                            "Config parent must be directory.");

            return config;
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load config file: %s", e, path);
        }
    }

    private static PropertiesConfiguration getRemoteConfig(URL url) {
        try {
            return new PropertiesConfiguration(url);
        } catch (ConfigurationException e) {
            throw new HugeException("Unable to load remote config file: %s",
                                    e, url);
        }
    }

}
