<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
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

========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java
package org.apache.hugegraph.pd.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
public class PromTargetsModel {
========
/**
 * @author lynn.bond@hotmail.com on 2022/2/14
 */
public class SDConfig {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java

    private static final String LABEL_METRICS_PATH = "__metrics_path__";
    private static final String LABEL_SCHEME = "__scheme__";
    private static final String LABEL_JOB_NAME = "job";
    private static final String LABEL_CLUSTER = "cluster";
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
    private final Map<String, String> labels = new HashMap<>();
    private Set<String> targets = new HashSet<>();

    private PromTargetsModel() {
    }

    public static PromTargetsModel of() {
        return new PromTargetsModel();
========

    private Set<String> targets = new HashSet<>();
    private Map<String, String> labels = new HashMap<>();

    private SDConfig() {
    }

    public static SDConfig of() {
        return new SDConfig();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java
    }

    public Set<String> getTargets() {
        return targets;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
    public PromTargetsModel setTargets(Set<String> targets) {
========
    public SDConfig setTargets(Set<String> targets) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java
        if (targets != null) {
            this.targets = targets;
        }
        return this;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
    public PromTargetsModel addTarget(String target) {
        if (target == null) {
            return this;
        }
========
    public SDConfig addTarget(String target) {
        if (target == null) return this;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java
        this.targets.add(target);
        return this;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
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
========
    public SDConfig setMetricsPath(String path) {
        return this.addLabel(LABEL_METRICS_PATH, path);
    }

    public SDConfig setScheme(String scheme) {
        return this.addLabel(LABEL_SCHEME, scheme);
    }

    public SDConfig setClusterId(String clusterId) {
        return this.addLabel(LABEL_CLUSTER, clusterId);
    }

    public SDConfig addLabel(String label, String value) {
        if (label == null || value == null) return this;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java
        this.labels.put(label, value);
        return this;
    }

    @Override
    public String toString() {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/PromTargetsModel.java
        return "PromTargetModel{" +
========
        return "SDConfig{" +
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/model/SDConfig.java
               "targets=" + targets +
               ", labels=" + labels +
               '}';
    }
}
