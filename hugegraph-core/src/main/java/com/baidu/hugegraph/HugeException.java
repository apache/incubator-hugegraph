package com.baidu.hugegraph;

public class HugeException extends RuntimeException {

    private static final long serialVersionUID = -8711375282196157058L;

    public HugeException(String message) {
        super(message);
    }

    public HugeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
