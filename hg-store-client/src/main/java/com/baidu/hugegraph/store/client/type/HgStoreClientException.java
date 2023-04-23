package com.baidu.hugegraph.store.client.type;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/27
 */
public class HgStoreClientException extends RuntimeException {

    public static HgStoreClientException of(String msg) {
        return new HgStoreClientException(msg);
    }
    public static HgStoreClientException of(String msg,Throwable cause) {
        return new HgStoreClientException(msg,cause);
    }

    public static HgStoreClientException of(Throwable cause) {
        return new HgStoreClientException(cause);
    }

    public HgStoreClientException(String msg) {
        super(msg);
    }

    public HgStoreClientException(Throwable cause) {
        super(cause);
    }

    public HgStoreClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
