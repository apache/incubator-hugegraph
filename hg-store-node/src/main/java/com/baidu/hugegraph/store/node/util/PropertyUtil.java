package com.baidu.hugegraph.store.node.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class PropertyUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PropertyUtil.class);
    public static String get(String key) {
        return get(key, null);
    }

    public static String get(final String key, String def) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty.");
        }

        String value = null;
        try {
            if (System.getSecurityManager() == null) {
                value = System.getProperty(key);
            } else {
                value = AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty(key));
            }
        } catch (Exception e) {
            LOG.error("exception {}", e);
        }

        if (value == null) {
            return def;
        }

        return value;
    }


    public static boolean getBoolean(String key, boolean def) {
        String value = get(key, Boolean.toString(def));
        value = value.trim().toLowerCase();
        if (value.isEmpty()) {
            return true;
        }

        if ("true".equals(value) || "yes".equals(value) || "1".equals(value)) {
            return true;
        }

        if ("false".equals(value) || "no".equals(value) || "0".equals(value)) {
            return false;
        }
        return def;
    }

    public static int getInt(String key, int def) {
        String value = get(key);
        if (value == null) {
            return def;
        }

        value = value.trim().toLowerCase();
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            LOG.warn("exception ", e);
        }
        return def;
    }

    public static Object setProperty(String key, String value) {
        return System.getProperties().setProperty(key, value);
    }
}
