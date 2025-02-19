/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hugegraph.pd.raft.serializer;

import com.caucho.hessian.io.Deserializer;
import com.caucho.hessian.io.HessianProtocolException;
import com.caucho.hessian.io.Serializer;
import com.caucho.hessian.io.SerializerFactory;


import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class HugegraphHessianSerializerFactory extends SerializerFactory {

    private static final HugegraphHessianSerializerFactory INSTANCE = new HugegraphHessianSerializerFactory();

    private HugegraphHessianSerializerFactory() {
        super();
        initWhitelist();
    }

    public static HugegraphHessianSerializerFactory getInstance() {
        return INSTANCE;
    }

    private final Set<String> whitelist = new HashSet<>();

    private void initWhitelist() {
        allowBasicType();
        allowCollections();
        allowConcurrent();
        allowTime();
        allowBusinessClasses();
    }

    private void allowBasicType() {
        addToWhitelist(
                boolean.class, byte.class, char.class, double.class,
                float.class, int.class, long.class, short.class,
                Boolean.class, Byte.class, Character.class, Double.class,
                Float.class, Integer.class, Long.class, Short.class,
                String.class, Class.class, Number.class
        );
    }

    private void allowCollections() {
        addToWhitelist(
                List.class, ArrayList.class, LinkedList.class,
                Set.class, HashSet.class, LinkedHashSet.class, TreeSet.class,
                Map.class, HashMap.class, LinkedHashMap.class, TreeMap.class
        );
    }

    private void allowConcurrent() {
        addToWhitelist(
                AtomicBoolean.class, AtomicInteger.class, AtomicLong.class, AtomicReference.class,
                ConcurrentMap.class, ConcurrentHashMap.class, ConcurrentSkipListMap.class, CopyOnWriteArrayList.class
        );
    }

    private void allowTime() {
        addToWhitelist(
                Date.class, Calendar.class, TimeUnit.class,
                SimpleDateFormat.class, DateTimeFormatter.class
        );
        tryAddClass("java.time.LocalDate");
        tryAddClass("java.time.LocalDateTime");
        tryAddClass("java.time.Instant");
    }

    private void allowBusinessClasses() {
        addToWhitelist(
                org.apache.hugegraph.pd.raft.KVOperation.class,
                byte[].class
        );
    }

    private void addToWhitelist(Class<?>... classes) {
        for (Class<?> clazz : classes) {
            whitelist.add(clazz.getName());
        }
    }

    private void tryAddClass(String className) {
        try {
            Class.forName(className);
            whitelist.add(className);
        } catch (ClassNotFoundException e) {
            log.warn("Failed to load class {}", className);
        }
    }

    @Override
    public Serializer getSerializer(Class cl) throws HessianProtocolException {
        checkWhitelist(cl);
        return super.getSerializer(cl);
    }

    @Override
    public Deserializer getDeserializer(Class cl) throws HessianProtocolException {
        checkWhitelist(cl);
        return super.getDeserializer(cl);
    }

    private void checkWhitelist(Class cl) {
        String className = cl.getName();
        if (!whitelist.contains(className)) {
            log.warn("Security alert: Blocked unauthorized class [{}] at {}",
                     className, new Date());
            throw new SecurityException("hessian serialize unauthorized class: " + className);
        }
    }
}
