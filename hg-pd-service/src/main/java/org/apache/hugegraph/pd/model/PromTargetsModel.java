package org.apache.hugegraph.pd.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author lynn.bond@hotmail.com on 2022/2/14
 */
public class PromTargetsModel {
    private static final String LABEL_METRICS_PATH = "__metrics_path__";
    private static final String LABEL_SCHEME = "__scheme__";
    private static final String LABEL_JOB_NAME = "job";
    private static final String LABEL_CLUSTER = "cluster";

    private Set<String> targets = new HashSet<>();
    private Map<String, String> labels = new HashMap<>();

    public static PromTargetsModel of() {
        return new PromTargetsModel();
    }

    private PromTargetsModel() {}

    public Set<String> getTargets() {
        return targets;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public PromTargetsModel addTarget(String target) {
        if (target == null) return this;
        this.targets.add(target);
        return this;
    }

    public PromTargetsModel setTargets(Set<String> targets) {
        if (targets != null) {
            this.targets = targets;
        }
        return this;
    }

    public PromTargetsModel setMetricsPath(String path) {
        return this.addLabel(LABEL_METRICS_PATH, path);
    }

    public PromTargetsModel setScheme(String scheme) {
        return this.addLabel(LABEL_SCHEME, scheme);
    }

    public PromTargetsModel setClusterId(String clusterId){
        return this.addLabel(LABEL_CLUSTER,clusterId);
    }

    public PromTargetsModel addLabel(String label, String value) {
        if (label == null || value == null) return this;
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
