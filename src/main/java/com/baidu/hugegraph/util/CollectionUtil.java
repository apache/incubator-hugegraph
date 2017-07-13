package com.baidu.hugegraph.util;

import java.util.Collection;

/**
 * Created by liningrui on 2017/3/30.
 */
public final class CollectionUtil {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static boolean containsAll(Collection a, Collection b) {
        return a.containsAll(b);
    }
}
