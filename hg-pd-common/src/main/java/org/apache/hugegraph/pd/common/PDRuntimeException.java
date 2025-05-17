package org.apache.hugegraph.pd.common;

/**
 * @author zhangyingjie
 * @date 2022/8/1
 **/
public class PDRuntimeException extends RuntimeException {

    // public static final int LICENSE_ERROR = -11;

    private int errorCode = 0;

    public PDRuntimeException(int error) {
        super(String.format("Error code = %d", error));
        this.errorCode = error;
    }

    public PDRuntimeException(int error, String msg) {
        super(msg);
        this.errorCode = error;
    }

    public PDRuntimeException(int error, Throwable e) {
        super(e);
        this.errorCode = error;
    }

    public PDRuntimeException(int error, String msg, Throwable e) {
        super(msg, e);
        this.errorCode = error;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
