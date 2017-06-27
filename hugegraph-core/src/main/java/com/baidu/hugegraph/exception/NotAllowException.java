package com.baidu.hugegraph.exception;

import com.baidu.hugegraph.HugeException;

/**
 * Created by liningrui on 2017/6/27.
 */
public class NotAllowException extends HugeException {

    private static final long serialVersionUID = 5152465646323494842L;

    public NotAllowException(String message, Object... args) {
        super(message, args);
    }
}
