/*
 * Copyright (C) 2018 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.hugegraph.exception;

import com.baidu.hugegraph.HugeException;

public class SecurityException extends HugeException {

    private static final long serialVersionUID = -1427924451828873200L;

    public SecurityException(String message) {
        super(message);
    }

    public SecurityException(String message, Object... args) {
        super(message, args);
    }
}
