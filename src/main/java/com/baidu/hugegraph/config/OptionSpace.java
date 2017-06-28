package com.baidu.hugegraph.config;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.util.E;
import com.google.common.collect.Maps;

/**
 * Created by liningrui on 2017/3/27.
 */
public class OptionSpace {

    private static final Logger logger = LoggerFactory.getLogger(OptionSpace.class);

    private static final Map<String, ConfigOption> options = Maps.newHashMap();

    public static void register(OptionHolder holder) {
        options.putAll(holder.options());
        logger.debug("Registered " + holder.getClass().getSimpleName());
    }

    public static void register(ConfigOption<?> element) {
        E.checkArgument(!options.containsKey(element.name()),
                "The option: '%s' has already been registered",
                element.name());
        options.put(element.name(), element);
    }

    public static Boolean containKey(String key) {
        return options.containsKey(key);
    }

    public static ConfigOption<?> get(String key) {
        return options.get(key);
    }
}
