package org.apache.hugegraph.pd.client.interceptor;

/**
 * @author zhangyingjie
 * @date 2023/8/7
 **/
public class AuthenticationException extends RuntimeException{

    public AuthenticationException(String msg) {
        super(msg);
    }

    public AuthenticationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
