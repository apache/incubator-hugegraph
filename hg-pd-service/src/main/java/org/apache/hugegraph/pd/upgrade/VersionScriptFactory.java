package org.apache.hugegraph.pd.upgrade;

import org.apache.hugegraph.pd.upgrade.scripts.PartitionMetaUpgrade;
import org.apache.hugegraph.pd.upgrade.scripts.TaskCleanUpgrade;

import java.util.LinkedList;
import java.util.List;

public class VersionScriptFactory {
    private static volatile VersionScriptFactory factory;

    private static List<VersionUpgradeScript> scripts = new LinkedList<>();

    static {
        registerScript(new PartitionMetaUpgrade());
        registerScript(new TaskCleanUpgrade());
    }

    private VersionScriptFactory(){

    }

    public static VersionScriptFactory getInstance(){
        if (factory == null) {
            synchronized (VersionScriptFactory.class) {
                if (factory == null) {
                    factory = new VersionScriptFactory();
                }
            }
        }
        return factory;
    }

    public static void registerScript(VersionUpgradeScript script) {
        scripts.add(script);
    }

    public List<VersionUpgradeScript> getScripts() {
        return scripts;
    }
}
