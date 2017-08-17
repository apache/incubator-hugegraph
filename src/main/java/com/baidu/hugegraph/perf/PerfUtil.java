/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.perf;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.slf4j.Logger;

import com.baidu.hugegraph.type.TriFunction;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.ReflectionUtil;
import com.google.common.reflect.ClassPath.ClassInfo;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

public class PerfUtil {

    private static final Logger LOG = Log.logger(PerfUtil.class);
    private static ThreadLocal<PerfUtil> instance = new ThreadLocal<>();

    private Map<String, Stopwatch> stopwatches;
    private Stack<String> callStack;

    private PerfUtil() {
        this.stopwatches = new HashMap<>();
        this.callStack = new Stack<>();
    }

    public static PerfUtil instance() {
        PerfUtil p = instance.get();
        if (p == null) {
            p = new PerfUtil();
            instance.set(p);
        }
        return p;
    }

    private static long now() {
        return System.nanoTime();
    }

    public boolean start(String name) {
        String parent = this.callStack.empty() ? "" : this.callStack.peek();
        Stopwatch item = this.stopwatches.get(Stopwatch.id(parent, name));
        if (item == null) {
            item = new Stopwatch(name, parent);
            this.stopwatches.put(item.id(), item);
        }
        this.callStack.push(item.id());
        item.startTime(now());

        return true; // just for assert
    }

    public boolean end(String name) {
        String current = this.callStack.pop();
        assert current.endsWith(name);

        String parent = this.callStack.empty() ? "" : this.callStack.peek();
        Stopwatch item = this.stopwatches.get(Stopwatch.id(parent, name));
        if (item == null) {
            throw new InvalidParameterException(name);
        }
        item.endTime(now());

        return true;
    }

    public void profilePackage(String... packages) throws
            NotFoundException, CannotCompileException,
            ClassNotFoundException, IOException {
        Set<String> loadedClasses = new LinkedHashSet<>();

        Iterator<ClassInfo> classes = ReflectionUtil.classes(packages);
        while (classes.hasNext()) {
            String cls = classes.next().getName();
            // super class first
            for (String s : ReflectionUtil.superClasses(cls)) {
                if (!loadedClasses.contains(s)) {
                    profileClass(s);
                    loadedClasses.add(s);
                }
            }
            // self class
            if (!loadedClasses.contains(cls)) {
                profileClass(cls);
                loadedClasses.add(cls);
            }
        }
    }

    public void profileClass(String... classes) throws
            NotFoundException, CannotCompileException, ClassNotFoundException {
        ClassPool classPool = ClassPool.getDefault();

        for (String cls : classes) {
            CtClass ctClass = classPool.get(cls);
            List<CtMethod> methods = ReflectionUtil.getMethodsAnnotatedWith(
                    ctClass, Watched.class, false);
            for (CtMethod method : methods) {
                profile(method);
            }

            // load class and make it effective
            if (!methods.isEmpty()) {
                ctClass.toClass();
            }
        }
    }

    private void profile(CtMethod ctMethod)
            throws CannotCompileException, ClassNotFoundException {
        final String START =
                "com.baidu.hugegraph.perf.PerfUtil.instance().start(\"%s\");";
        final String END =
                "com.baidu.hugegraph.perf.PerfUtil.instance().end(\"%s\");";

        Watched annotation = (Watched) ctMethod.getAnnotation(Watched.class);

        String name = annotation.value();
        if (name.isEmpty()) {
            name = ctMethod.getName();
        }
        if (!annotation.prefix().isEmpty()) {
            name = annotation.prefix() + "." + name;
        }

        ctMethod.insertBefore(String.format(START, name));
        // Insert as a finally-statement
        ctMethod.insertAfter(String.format(END, name), true);

        LOG.debug("Profiled for: '{}' [{}]", name, ctMethod.getLongName());
    }

    @Override
    public String toString() {
        return this.stopwatches.toString();
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder(8 + this.stopwatches.size() * 96);
        sb.append('{');
        for (Map.Entry<String, Stopwatch> w : this.stopwatches.entrySet()) {
            sb.append('"');
            sb.append(w.getKey());
            sb.append('"');

            sb.append(':');

            sb.append(w.getValue().toJson());

            sb.append(',');
        }
        if (!this.stopwatches.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append('}');
        return sb.toString();
    }

    // TODO: move toECharts() method out of this class
    public String toECharts() {

        TriFunction<Integer, Integer, List<Stopwatch>, String> formatLevel = (
                totalDepth, depth, items) -> {
            float factor = 100.0f / (totalDepth + 1);
            float showFactor = 1 + (totalDepth - depth) / (float) depth;

            float radiusFrom = depth * factor;
            float radiusTo = depth * factor + factor;
            if (depth == 1) {
                radiusFrom = 0;
            }

            StringBuilder sb = new StringBuilder(8 + items.size() * 128);
            sb.append('{');
            sb.append("name: 'Total Cost',");
            sb.append("type: 'pie',");
            sb.append(String.format("radius: ['%s%%', '%s%%'],",
                                    radiusFrom, radiusTo));
            sb.append(String.format(
                    "label: {normal: {position: 'inner', formatter:" +
                    "function(params) {" +
                    "  if (params.percent > %s) return params.data.name;" +
                    "  else return '';" +
                    "}}},", showFactor));
            sb.append("data: [");

            items.sort((i, j) -> i.id().compareTo(j.id()));
            for (Stopwatch w : items) {
                sb.append('{');

                sb.append("value:");
                sb.append(w.totalCost() / 1000000.0);
                sb.append(',');

                sb.append("min:");
                sb.append(w.minCost());
                sb.append(',');

                sb.append("max:");
                sb.append(w.maxCost());
                sb.append(',');

                sb.append("id:'");
                sb.append(w.id());
                sb.append("',");

                sb.append("name:'");
                sb.append(w.name());
                sb.append("',");

                sb.append("times:");
                sb.append(w.times());

                sb.append('}');
                sb.append(',');
            }
            if (!items.isEmpty()) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("]}");
            return sb.toString();
        };

        BiConsumer<List<Stopwatch>, List<Stopwatch>> fillOther =
            (itemsOfI, parents) -> {
            for (Stopwatch parent : parents) {
                Stream<Stopwatch> children = itemsOfI.stream().filter(c -> {
                    return c.parent().equals(parent.id());
                });
                long sum = children.mapToLong(c -> c.totalCost()).sum();
                if (sum < parent.totalCost()) {
                    Stopwatch other = new Stopwatch("~", parent.id());
                    other.totalCost(parent.totalCost() - sum);
                    itemsOfI.add(other);
                }
            }
        };

        Map<String, Stopwatch> items = this.stopwatches;
        Map<Integer, List<Stopwatch>> levelItems = new HashMap<>();
        int maxDepth = 1;
        for (Map.Entry<String, Stopwatch> e : items.entrySet()) {
            int depth = e.getKey().split("/").length;
            levelItems.putIfAbsent(depth, new LinkedList<>());
            levelItems.get(depth).add(e.getValue().copy());
            if (depth > maxDepth) {
                maxDepth = depth;
            }
        }

        StringBuilder sb = new StringBuilder(8 + items.size() * 128);
        sb.append("{");
        sb.append("tooltip: {trigger: 'item', " +
            "formatter: function(params) {" +
            "    return params.data.name + ' ' + params.percent + '% <br/>'" +
            "        + 'cost: ' + params.data.value + ' (ms) <br/>'" +
            "        + 'min: ' + params.data.min + ' (ns) <br/>'" +
            "        + 'max: ' + params.data.max + ' (ns) <br/>'" +
            "        + 'times: ' + params.data.times + '<br/>'" +
            "       + params.data.id + '<br/>';" +
            "}");
        sb.append("},");
        sb.append("series: [");
        for (int i = 1; levelItems.containsKey(i); i++) {
            List<Stopwatch> itemsOfI = levelItems.get(i);
            if (i > 1) {
                fillOther.accept(itemsOfI, levelItems.get(i - 1));
            }
            sb.append(formatLevel.apply(maxDepth, i, itemsOfI));
            sb.append(',');
        }
        if (!items.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]}");

        return sb.toString();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface Watched {
        public String value() default "";
        public String prefix() default "";
    }
}
