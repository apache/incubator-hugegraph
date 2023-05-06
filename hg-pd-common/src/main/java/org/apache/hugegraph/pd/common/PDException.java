package org.apache.hugegraph.pd.common;

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
