package com.baidu.hugegraph;

public class HugeException extends RuntimeException {

    private static final long serialVersionUID = -8711375282196157058L;

    public HugeException(String message) {
        super(message);
    }

    public HugeException(String message, Throwable cause) {
        super(message, cause);
    }

    public HugeException(String message, Object... args) {
        super(String.format(message, args));
    }

    public HugeException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }
}
