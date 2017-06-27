package com.baidu.hugegraph.util;

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/5/26.
 */
public class E {

    public static void checkNotNull(Object object, String elem) {
        Preconditions.checkNotNull(object, "The '%s' can't be null", elem);
    }

    public static void checkNotNull(Object object, String elem, String owner) {
        Preconditions.checkNotNull(object, "The '%s' of '%s' can't be null",
                                   elem, owner);
    }

    public static void checkNotEmpty(Collection<?> collection, String elem) {
        Preconditions.checkArgument(!collection.isEmpty(),
                                    "The '%s' can't be empty", elem);
    }

    public static void checkNotEmpty(Collection<?> collection,
                                     String elem,
                                     String owner) {
        Preconditions.checkArgument(!collection.isEmpty(),
                                    "The '%s' of '%s' can't be empty",
                                    elem, owner);
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

    public static void checkState(boolean expression,
                                  @Nullable String message,
                                  @Nullable Object... args) {
        Preconditions.checkState(expression, message, args);
    }
}
