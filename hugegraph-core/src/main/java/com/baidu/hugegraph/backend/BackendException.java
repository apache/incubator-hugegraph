package com.baidu.hugegraph.backend;

import com.baidu.hugegraph.HugeException;

public class BackendException extends HugeException {

    private static final long serialVersionUID = -1947589125372576298L;

    public BackendException(String message) {
        super(message);
    }

    public BackendException(String message, Throwable cause) {
        super(message, cause);
    }

    public BackendException(String message, Object... args) {
        super(message, args);
    }

    public BackendException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    public BackendException(Throwable cause) {
        this("Exception in backend", cause);
    }
}
