package com.baidu.hugegraph.exception;

import com.baidu.hugegraph.HugeException;

public class NotFoundException extends HugeException {

    private static final long serialVersionUID = 5152465646323494849L;

    public NotFoundException(String message, Object... args) {
        super(message, args);
    }
}
