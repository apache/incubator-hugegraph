package com.baidu.hugegraph.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Log {

    public static Logger logger(String name) {
        return LoggerFactory.getLogger(name);
    }

    public static Logger logger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }
}
