package com.baidu.hugegraph;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.baidu.hugegraph.configuration.HugeConfiguration;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeFactory {

    public static HugeGraph open() {
        return new HugeGraph(new HugeConfiguration());
    }

    public static HugeGraph open(String shortcutOrFile) {
        return open(getLocalConfiguration(shortcutOrFile));
    }

    public static HugeGraph open(Configuration configuration) {
        return new HugeGraph(new HugeConfiguration(configuration));
    }

    private static Configuration getLocalConfiguration(String shortcutOrFile) {
        File file = new File(shortcutOrFile);
        Preconditions.checkArgument(file != null && file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file, but was given: %s", file.toString());

        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(file);

            final File tmpParent = file.getParentFile();
            final File configParent;

            if (null == tmpParent) {
                configParent = new File(System.getProperty("user.dir"));
            } else {
                configParent = tmpParent;
            }

            Preconditions.checkNotNull(configParent);
            Preconditions.checkArgument(configParent.isDirectory());

            return configuration;
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + file, e);
        }
    }
}
