package com.baidu.hugegraph.util;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.baidu.hugegraph.type.ExtendableIterator;
import com.google.common.collect.Lists;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

public final class ReflectionUtil {

    public static List<Method> getMethodsAnnotatedWith(
            Class<?> type,
            Class<? extends Annotation> annotation,
            boolean withSuperClass) {
        final List<Method> methods = new LinkedList<>();
        Class<?> klass = type;
        do {
            for (Method method : klass.getDeclaredMethods()) {
                if (method.isAnnotationPresent(annotation)) {
                    methods.add(method);
                }
            }
            klass = klass.getSuperclass();
        } while (klass != Object.class && withSuperClass);
        return methods;
    }

    public static List<CtMethod> getMethodsAnnotatedWith(
            CtClass type,
            Class<? extends Annotation> annotation,
            boolean withSuperClass) throws NotFoundException {
        final List<CtMethod> methods = new LinkedList<>();

        CtClass klass = type;
        do {
            for (CtMethod method : klass.getDeclaredMethods()) {
                if (method.hasAnnotation(annotation)) {
                    methods.add(method);
                }
            }
            klass = klass.getSuperclass();
        } while (klass != null && withSuperClass);
        return methods;
    }

    public static Iterator<ClassInfo> classes(String... packages)
            throws IOException {
        ClassPath path = ClassPath.from(ReflectionUtil.class.getClassLoader());
        ExtendableIterator<ClassInfo> results = new ExtendableIterator<>();
        for (String p : packages) {
            results.extend(path.getTopLevelClassesRecursive(p).iterator());
        }
        return results;
    }

    public static List<String> superClasses(String clazz)
            throws NotFoundException {
        CtClass klass = ClassPool.getDefault().get(clazz);
        klass = klass.getSuperclass();

        List<String> results = new LinkedList<>();
        while (klass != null) {
            results.add(klass.getName());
            klass = klass.getSuperclass();
        }
        return Lists.reverse(results);
    }
}
