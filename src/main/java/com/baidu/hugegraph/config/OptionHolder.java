package com.baidu.hugegraph.config;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.exception.ConfigException;

/**
 * Created by liningrui on 2017/5/25.
 */
public class OptionHolder {

    private static final Logger logger = LoggerFactory.getLogger(HugeConfig.class);

    protected Map<String, ConfigOption> options;

    public OptionHolder() {
        this.options = new HashMap<>();
    }

    protected void registerOptions() {
        Field[] fields = this.getClass().getFields();
        for (Field field : fields) {
            try {
                ConfigOption option = (ConfigOption) field.get(this);
                this.options.put(option.name(), option);
            } catch (Exception e) {
                String msg = String.format("Failed to regiser option : %s", field);
                logger.error(msg, e);
                throw new ConfigException(msg, e);
            }
        }
    }

    public Map<String, ConfigOption> options() {
        return options;
    }
}
