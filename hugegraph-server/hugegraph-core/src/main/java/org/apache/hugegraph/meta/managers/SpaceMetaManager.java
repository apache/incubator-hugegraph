/*
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

package org.apache.hugegraph.meta.managers;

import static org.apache.hugegraph.meta.MetaManager.META_PATH_ADD;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_CONF;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_EVENT;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE_LIST;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_REMOVE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_UPDATE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.collection.CollectionFactory;

public class SpaceMetaManager extends AbstractMetaManager {

    public SpaceMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    public List<String> listGraphSpace() {
        List<String> result = new ArrayList<>();
        Map<String, String> graphSpaceMap = this.metaDriver.scanWithPrefix(
                graphSpaceListKey());
        for (Map.Entry<String, String> item : graphSpaceMap.entrySet()) {
            result.add(item.getValue());
        }

        return result;
    }

    public Map<String, GraphSpace> graphSpaceConfigs() {
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                this.graphSpaceConfPrefix());
        Map<String, GraphSpace> configs =
                CollectionFactory.newMap(CollectionType.EC);
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMITER);
            configs.put(parts[parts.length - 1],
                        JsonUtil.fromJson(entry.getValue(), GraphSpace.class));
        }
        return configs;
    }

    public GraphSpace graphSpace(String name) {
        String space = this.metaDriver.get(this.graphSpaceConfKey(name));
        if (StringUtils.isEmpty(space)) {
            return null;
        }
        return JsonUtil.fromJson(space, GraphSpace.class);
    }

    public GraphSpace getGraphSpaceConfig(String graphSpace) {
        String gs = this.metaDriver.get(this.graphSpaceConfKey(graphSpace));
        if (StringUtils.isEmpty(gs)) {
            return null;
        }
        return JsonUtil.fromJson(gs, GraphSpace.class);
    }

    public void addGraphSpaceConfig(String name, GraphSpace space) {
        this.metaDriver.put(this.graphSpaceConfKey(name),
                            JsonUtil.toJson(space));
    }

    public void removeGraphSpaceConfig(String name) {
        this.metaDriver.delete(this.graphSpaceConfKey(name));
    }

    public void updateGraphSpaceConfig(String name, GraphSpace space) {
        this.metaDriver.put(this.graphSpaceConfKey(name),
                            JsonUtil.toJson(space));
    }

    public void appendGraphSpaceList(String name) {
        String key = this.graphSpaceListKey(name);
        this.metaDriver.put(key, name);
    }

    public void clearGraphSpaceList(String name) {
        String key = this.graphSpaceListKey(name);
        this.metaDriver.delete(key);
    }

    public <T> void listenGraphSpaceAdd(Consumer<T> consumer) {
        this.listen(this.graphSpaceAddKey(), consumer);
    }

    public <T> void listenGraphSpaceRemove(Consumer<T> consumer) {
        this.listen(this.graphSpaceRemoveKey(), consumer);
    }

    public <T> void listenGraphSpaceUpdate(Consumer<T> consumer) {
        this.listen(this.graphSpaceUpdateKey(), consumer);
    }

    public void notifyGraphSpaceAdd(String graphSpace) {
        this.metaDriver.put(this.graphSpaceAddKey(), graphSpace);
    }

    public void notifyGraphSpaceRemove(String graphSpace) {
        this.metaDriver.put(this.graphSpaceRemoveKey(), graphSpace);
    }

    public void notifyGraphSpaceUpdate(String graphSpace) {
        this.metaDriver.put(this.graphSpaceUpdateKey(), graphSpace);
    }

    private String graphSpaceConfPrefix() {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           META_PATH_CONF);
    }

    private String graphSpaceAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPHSPACE/ADD
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPHSPACE,
                           META_PATH_ADD);
    }

    private String graphSpaceRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPHSPACE/REMOVE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPHSPACE,
                           META_PATH_REMOVE);
    }

    private String graphSpaceUpdateKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPHSPACE/UPDATE
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_EVENT,
                           META_PATH_GRAPHSPACE,
                           META_PATH_UPDATE);
    }

    private String graphSpaceConfKey(String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           META_PATH_CONF,
                           name);
    }

    private String graphSpaceListKey(String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE_LIST/{graphspace}
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE_LIST,
                           name);
    }

    private String graphSpaceListKey() {
        // HUGEGRAPH/{cluster}/GRAPHSPACE_LIST
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE_LIST);
    }
}
