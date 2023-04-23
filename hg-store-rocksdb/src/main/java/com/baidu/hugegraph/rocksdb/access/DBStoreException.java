package com.baidu.hugegraph.rocksdb.access;

import java.io.InterruptedIOException;

public class DBStoreException extends RuntimeException {

    private static final long serialVersionUID = 5956983547131986887L;

    public DBStoreException(String message) {
        super(message);
    }

    public DBStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBStoreException(String message, Object... args) {
        super(String.format(message, args));
    }

    public DBStoreException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }

    public DBStoreException(Throwable cause) {
        this("Exception in DBStore " + cause.getMessage(), cause);
    }

    public Throwable rootCause() {
        return rootCause(this);
    }

    public static Throwable rootCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    public static boolean isInterrupted(Throwable e) {
        Throwable rootCause = DBStoreException.rootCause(e);
        return rootCause instanceof InterruptedException ||
                rootCause instanceof InterruptedIOException;
    }
}
