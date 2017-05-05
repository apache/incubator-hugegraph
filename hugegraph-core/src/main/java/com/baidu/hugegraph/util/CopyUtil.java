package com.baidu.hugegraph.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.baidu.hugegraph.HugeException;

public class CopyUtil {

    public static <T> T cloneObject(T o, T clone) throws Exception {
        if (clone == null) {
            clone = (T) o.getClass().newInstance();
        }
        for (Field field : o.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (field.get(o) == null || Modifier.isFinal(field.getModifiers())) {
                continue;
            }
            if (field.getType().isPrimitive()
                    || field.getType().equals(String.class)
                    || field.getType().getSuperclass().equals(Number.class)
                    || field.getType().equals(Boolean.class)) {
                field.set(clone, field.get(o));
            } else {
                Object childObj = field.get(o);
                if (childObj == o) {
                    field.set(clone, clone);
                } else {
                    field.set(clone, cloneObject(field.get(o), null));
                }
            }
        }
        return clone;
    }

    public static <T> T copy(T object) {
        return copy(object, null);
    }

    public static <T> T copy(T object, T clone) {
        try {
            return cloneObject(object, clone);
        } catch (Exception e) {
            throw new HugeException("Failed to clone object", e);
        }
    }

    public static <T> T deepCopy(T object) {
        @SuppressWarnings("unchecked")
        Class<T> cls = (Class<T>) object.getClass();
        return JsonUtil.fromJson(JsonUtil.toJson(object), cls);
    }
}
