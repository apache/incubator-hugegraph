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

package org.apache.hugegraph.pd.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PromTargetsModel {

    private static final String LABEL_METRICS_PATH = "__metrics_path__";
    private static final String LABEL_SCHEME = "__scheme__";
    private static final String LABEL_JOB_NAME = "job";
    private static final String LABEL_CLUSTER = "cluster";
    private final Map<String, String> labels = new HashMap<>();
    private Set<String> targets = new HashSet<>();

    private PromTargetsModel() {
    }

    public static PromTargetsModel of() {
        return new PromTargetsModel();
    }

    public Set<String> getTargets() {
        return targets;
    }

    public PromTargetsModel setTargets(Set<String> targets) {
        if (targets != null) {
            this.targets = targets;
        }
        return this;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public PromTargetsModel addTarget(String target) {
        if (target == null) {
            return this;
        }
        this.targets.add(target);
        return this;
    }

    public PromTargetsModel setMetricsPath(String path) {
        return this.addLabel(LABEL_METRICS_PATH, path);
    }

    public PromTargetsModel setScheme(String scheme) {
        return this.addLabel(LABEL_SCHEME, scheme);
    }

    public PromTargetsModel setClusterId(String clusterId) {
        return this.addLabel(LABEL_CLUSTER, clusterId);
    }

    public PromTargetsModel addLabel(String label, String value) {
        if (label == null || value == null) {
            return this;
        }
        this.labels.put(label, value);
        return this;
    }

    @Override
    public String toString() {
        return "PromTargetModel{" +
               "targets=" + targets +
               ", labels=" + labels +
               '}';
    }
}
