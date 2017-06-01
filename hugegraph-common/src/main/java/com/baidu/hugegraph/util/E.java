package com.baidu.hugegraph.util;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/5/26.
 */
public class E {

    public static void checkNotNull(Object object,
                                    @Nullable String elem,
                                    @Nullable Object... args) {
        String message = String.format("%s can't be null.", elem);
        Preconditions.checkNotNull(object, message, args);
    }

    public static void checkNotEmpty(Collection collection,
                                     @Nullable String elem,
                                     @Nullable Object... args) {
        String message = String.format("%s can't be empty.", elem);
        Preconditions.checkArgument(!collection.isEmpty(), message, args);
    }

    public static void checkArgument(boolean expression,
                                     @Nullable String message,
                                     @Nullable Object... args) {
        Preconditions.checkArgument(expression, message, args);
    }

    public static void checkArgumentNotNull(Object object,
                                     @Nullable String message,
                                     @Nullable Object... args) {
        Preconditions.checkArgument(object != null, message, args);
    }
}
