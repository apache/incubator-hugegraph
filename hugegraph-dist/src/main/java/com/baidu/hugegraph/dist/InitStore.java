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

package com.baidu.hugegraph.dist;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.util.E;

public class InitStore {

    public static void main(String[] args)
                  throws ConfigurationException, InterruptedException {

        E.checkArgument(args.length == 1,
                        "Init store only accept one config file.");
        E.checkArgument(args[0].endsWith(".yaml"),
                        "Init store only accept yaml config file.");

        String confFile = args[0];
        RegisterUtil.registerBackends();

        YamlConfiguration config = new YamlConfiguration();
        config.load(confFile);

        List<ConfigurationNode> graphNames = config.getRootNode().getChildren(
                CoreOptions.GRAPHS.name()).get(0).getChildren();

        E.checkArgumentNotNull(graphNames,
                               "Not found the node '%s' in the config file %s",
                               CoreOptions.GRAPHS.name(), confFile);
        E.checkArgument(!graphNames.isEmpty(),
                        "The node '%s' must contain at least one child node");

        for (ConfigurationNode graphName : graphNames) {
            String graphPropFile = graphName.getValue().toString();
            // get graph property file path
            HugeGraph graph = HugeFactory.open(graphPropFile);
            graph.initBackend();
            graph.close();
        }

        // Wait cache clear or init up to 30 seconds
        EventHub.destroy(30);
    }

}
