package com.baidu.hugegraph.util;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/5/26.
 */
public class E {

    public static void checkArgument(boolean expression,
                                     @Nullable String message,
                                     @Nullable Object... args) {
        Preconditions.checkArgument(expression, message, args);
    }
}
