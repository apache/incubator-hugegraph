package org.apache.hugegraph.pd.common;

import org.apache.hugegraph.pd.grpc.common.ErrorType;

public class PDException extends Exception{
    private int errorCode = 0;

    public PDException(int error) {
        super(String.format("Error code = %d", error));
        this.errorCode = error;
    }

    public PDException(int error, String msg) {
        super(msg);
        this.errorCode = error;
    }

    public PDException(ErrorType errorType) {
        super(errorType.name());
        this.errorCode = errorType.getNumber();
    }

    public PDException(ErrorType errorType, String message) {
        super(message);
        this.errorCode = errorType.getNumber();
    }

    public PDException(ErrorType errorType, Throwable e) {
        super(errorType.name(), e);
        this.errorCode = errorType.getNumber();
    }

    public PDException(ErrorType errorType, String message, Throwable e) {
        super(message, e);
        this.errorCode = errorType.getNumber();
    }

    public PDException(int error, Throwable e) {
        super(e);
        this.errorCode = error;
    }

    public PDException(int error, String msg, Throwable e) {
        super(msg, e);
        this.errorCode = error;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
