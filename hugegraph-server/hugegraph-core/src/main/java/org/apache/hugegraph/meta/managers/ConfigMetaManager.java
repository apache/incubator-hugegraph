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

import static org.apache.hugegraph.meta.MetaManager.META_PATH_DELIMITER;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GRAPHSPACE;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_GREMLIN_YAML;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_HUGEGRAPH;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_REST_PROPERTIES;
import static org.apache.hugegraph.meta.MetaManager.META_PATH_SERVICE;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.util.JsonUtil;

public class ConfigMetaManager extends AbstractMetaManager {

    public ConfigMetaManager(MetaDriver metaDriver, String cluster) {
        super(metaDriver, cluster);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceId) {
        Map<String, Object> map = null;
        String result = this.metaDriver.get(restPropertiesKey(graphSpace,
                                                              serviceId));
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceId,
                                              Map<String, Object> properties) {
        Map<String, Object> map;
        String result = this.metaDriver.get(restPropertiesKey(graphSpace,
                                                              serviceId));
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
            for (Map.Entry<String, Object> item : properties.entrySet()) {
                map.put(item.getKey(), item.getValue());
            }
        } else {
            map = properties;
        }
        this.metaDriver.put(restPropertiesKey(graphSpace, serviceId),
                            JsonUtil.toJson(map));
        return map;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> deleteRestProperties(String graphSpace,
                                                    String serviceId,
                                                    String key) {
        Map<String, Object> map = null;
        String result = this.metaDriver.get(restPropertiesKey(graphSpace,
                                                              serviceId));
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
            map.remove(key);
            this.metaDriver.put(restPropertiesKey(graphSpace, serviceId),
                                JsonUtil.toJson(map));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> clearRestProperties(String graphSpace,
                                                   String serviceId) {
        Map<String, Object> map = null;
        String key = restPropertiesKey(graphSpace, serviceId);
        String result = this.metaDriver.get(key);
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
            this.metaDriver.delete(key);
        }
        return map;
    }

    public String gremlinYaml(String graphSpace, String serviceId) {
        return this.metaDriver.get(gremlinYamlKey(graphSpace, serviceId));
    }

    public String gremlinYaml(String graphSpace, String serviceId,
                              String yaml) {
        this.metaDriver.put(gremlinYamlKey(graphSpace, serviceId), yaml);
        return yaml;
    }

    public <T> void listenRestPropertiesUpdate(String graphSpace,
                                               String serviceId,
                                               Consumer<T> consumer) {
        this.listen(this.restPropertiesKey(graphSpace, serviceId), consumer);
    }

    public <T> void listenGremlinYamlUpdate(String graphSpace,
                                            String serviceId,
                                            Consumer<T> consumer) {
        this.listen(this.gremlinYamlKey(graphSpace, serviceId), consumer);
    }


    private String restPropertiesKey(String graphSpace, String serviceId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE/
        // {serviceId}/REST_PROPERTIES
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_SERVICE,
                           serviceId,
                           META_PATH_REST_PROPERTIES);
    }

    private String gremlinYamlKey(String graphSpace, String serviceId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE/
        // {serviceId}/GREMLIN_YAML
        return String.join(META_PATH_DELIMITER,
                           META_PATH_HUGEGRAPH,
                           this.cluster,
                           META_PATH_GRAPHSPACE,
                           graphSpace,
                           META_PATH_SERVICE,
                           serviceId,
                           META_PATH_GREMLIN_YAML);
    }
}
