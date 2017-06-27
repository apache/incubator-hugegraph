package com.baidu.hugegraph.config;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Created by liningrui on 2017/3/27.
 */
public class ConfigSpace {

    private static final Logger logger = LoggerFactory.getLogger(ConfigSpace.class);

    private static final Map<String, ConfigOption> options = Maps.newHashMap();

    public static void register(OptionHolder holder) {
        options.putAll(holder.options());
        logger.debug("Registered " + holder.getClass().getSimpleName());
    }

    public static void register(ConfigOption<?> element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(!options.containsKey(element.name()),
                "An same config option has already been added to this " +
                "namespace: %s", element.name());
        options.put(element.name(), element);
    }

    public static Boolean containKey(String key) {
        Preconditions.checkNotNull(key);
        return options.containsKey(key);
    }

    public static ConfigOption<?> get(String key) {
        Preconditions.checkNotNull(key);
        return options.get(key);
    }
}
