package com.baidu.hugegraph2.backend;

import com.baidu.hugegraph2.HugeException;

/**
 * Created by jishilei on 17/3/19.
 */
public class BackendException extends HugeException {

    private static final long serialVersionUID = -1947589125372576298L;

    /**
     * @param message Exception message
     */
    public BackendException(String message) {
        super(message);
    }

    /**
     * @param message   Exception message
     * @param cause Cause of the exception
     */
    public BackendException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public BackendException(Throwable cause) {
        this("Exception in backend.", cause);
    }
}
