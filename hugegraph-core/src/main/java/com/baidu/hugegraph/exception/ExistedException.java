package com.baidu.hugegraph.exception;

import com.baidu.hugegraph.HugeException;

/**
 * Created by liningrui on 2017/6/27.
 */
public class ExistedException extends HugeException {

    private static final long serialVersionUID = 5152465646323494840L;

    public ExistedException(String type, Object arg) {
        super("The %s '%s' has existed", type, arg);
    }

}
