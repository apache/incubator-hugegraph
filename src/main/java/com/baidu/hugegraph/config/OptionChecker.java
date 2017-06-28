package com.baidu.hugegraph.config;

import java.lang.reflect.Array;
import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Predicate;

/**
 * Created by liningrui on 2017/3/23.
 */
public class OptionChecker {

    public static final <O> Predicate<O> disallowEmpty(Class<O> clazz) {
        return new Predicate<O>() {
            @Override
            public boolean apply(@Nullable O o) {
                if (o == null) {
                    return false;
                }
                if (o instanceof String) {
                    return StringUtils.isNotBlank((String) o);
                }
                if (o.getClass().isArray() && (Array.getLength(o) == 0 ||
                    Array.get(o, 0) == null)) {
                    return false;
                }
                if (o instanceof Collection && (((Collection) o).isEmpty() ||
                    ((Collection) o).iterator().next() == null)) {
                    return false;
                }
                return true;
            }
        };
    }

    public static final Predicate<Integer> positiveInt() {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer num) {
                return num != null && num > 0;
            }
        };
    }

    public static final Predicate<Integer> nonNegativeInt() {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer num) {
                return num != null && num >= 0;
            }
        };
    }

    public static final Predicate<Long> positiveLong() {
        return new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long num) {
                return num != null && num > 0;
            }
        };
    }

    public static final Predicate<Integer> rangeInt(int min, int max) {
        return new Predicate<Integer>() {
            @Override
            public boolean apply(@Nullable Integer num) {
                return num != null && num >= min && num <= max;
            }
        };
    }
}
