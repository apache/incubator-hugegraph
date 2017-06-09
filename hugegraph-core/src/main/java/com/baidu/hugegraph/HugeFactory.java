package com.baidu.hugegraph;

import java.io.File;
import java.net.URL;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeFactory {

    public static HugeGraph open(Configuration config) {
        E.checkArgument(config instanceof PropertiesConfiguration,
                "HugeConfig can only accept PropertiesConfiguration object.");
        return new HugeGraph(new HugeConfig((PropertiesConfiguration) config));
    }

    public static HugeGraph open(String path) {
        return open(getLocalConfig(path));
    }

    public static HugeGraph open(URL url) {
        return open(getRemoteConfig(url));
    }

    private static PropertiesConfiguration getLocalConfig(String path) {
        File file = new File(path);
        Preconditions.checkArgument(
                file.exists() && file.isFile() && file.canRead(),
                "Need to specify a readable configuration file rather than: %s",
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

            Preconditions.checkNotNull(configParent);
            Preconditions.checkArgument(configParent.isDirectory());

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
