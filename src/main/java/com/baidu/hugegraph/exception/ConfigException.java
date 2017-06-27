package com.baidu.hugegraph.exception;

public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = -8711375282196157058L;

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
