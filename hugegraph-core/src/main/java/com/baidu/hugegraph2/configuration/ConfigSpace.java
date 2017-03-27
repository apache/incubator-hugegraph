package com.baidu.toolkit.hugegraph.configuration;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Created by liningrui on 2017/3/27.
 */
public class ConfigSpace {

    private static final Map<String, ConfigOption> configOptions = Maps.newHashMap();

    /**
     * 每个configOption只会被注册一次
     * @param element
     */
    public static void register(ConfigOption element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(!configOptions.containsKey(element.name()),
                "A configuration element with the same name has already been added to this namespace: %s",
                element.name());
        configOptions.put(element.name(), element);
    }


    public static Boolean containKey(String key) {
        Preconditions.checkNotNull(key);
        return configOptions.containsKey(key);
    }

    public static ConfigOption get(String key) {
        Preconditions.checkNotNull(key);
        return configOptions.get(key);
    }
}
