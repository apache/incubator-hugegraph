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

package org.apache.hugegraph.store.query.func;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.store.query.Tuple2;
import org.apache.hugegraph.store.query.concurrent.AtomicFloat;

import com.google.common.util.concurrent.AtomicDouble;

public class AggregationFunctions {

    public static Supplier getAggregationBufferSupplier(String genericType) {
        switch (genericType) {
            case "java.lang.Long":
                return () -> 0L;
            case "java.lang.Integer":
                return () -> 0;
            case "java.lang.Float":
                // fall through to case "java.lang.Double"
            case "java.lang.Double":
                return () -> 0.0;
            case "java.lang.String":
                return () -> "";
            default:
                throw new RuntimeException("unsupported generic type of buffer: " + genericType);
        }
    }

    public static class SumFunction<U, T extends Number> extends UnaryAggregationFunction<U, T> {

        public SumFunction(Id field, Supplier<U> supplier) {
            super(field, supplier);
        }

        public SumFunction(Supplier<U> supplier) {
            super();
            this.supplier = supplier;
            this.buffer = initBuffer();
        }

        /**
         * 获取并添加记录
         *
         * @param record - 添加的记录
         */
        @Override
        public void iterate(T record) {
            if (record != null) {
                switch (buffer.getClass().getName()) {
                    case "java.util.concurrent.atomic.AtomicLong":
                        ((AtomicLong) buffer).getAndAdd((long) record);
                        break;
                    case "java.util.concurrent.atomic.AtomicInteger":
                        ((AtomicInteger) buffer).getAndAdd((Integer) record);
                        break;
                    case "com.google.common.util.concurrent.AtomicDouble":
                        ((AtomicDouble) buffer).getAndAdd((Double) record);
                        break;
                    case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                        ((AtomicFloat) buffer).getAndAdd((Float) record);
                        break;
                    default:
                        // throw new Exception ?
                        break;
                }
            }
        }

        /**
         * {@inheritDoc}
         * 将另一个 U 对象合并到当前对象。
         */
        @Override
        public void merge(U other) {
            switch (buffer.getClass().getName()) {
                case "java.util.concurrent.atomic.AtomicLong":
                    ((AtomicLong) buffer).getAndAdd(((AtomicLong) other).get());
                    break;
                case "java.util.concurrent.atomic.AtomicInteger":
                    ((AtomicInteger) buffer).getAndAdd(((AtomicInteger) other).get());
                    break;
                case "com.google.common.util.concurrent.AtomicDouble":
                    ((AtomicDouble) buffer).getAndAdd(((AtomicDouble) other).get());
                    break;
                case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                    ((AtomicFloat) buffer).getAndAdd(((AtomicFloat) other).get());
                    break;
                default:
                    // throw new Exception ?
                    break;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public T reduce() {
            switch (buffer.getClass().getName()) {
                case "java.util.concurrent.atomic.AtomicLong":
                    return (T) Long.valueOf(((AtomicLong) buffer).get());
                case "java.util.concurrent.atomic.AtomicInteger":
                    return (T) Integer.valueOf(((AtomicInteger) buffer).get());
                case "com.google.common.util.concurrent.AtomicDouble":
                    return (T) Double.valueOf(((AtomicDouble) buffer).get());
                case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                    return (T) Float.valueOf(((AtomicFloat) buffer).get());
                default:
                    // throw new Exception ?
                    break;
            }
            return null;
        }

        /**
         * {@inheritDoc}
         * 初始化缓冲区，返回相应类型的 Atomic 引用对象。
         *
         * @return 返回初始化后的 Atomic 对象。
         */
        @Override
        protected U initBuffer() {
            return getInitValue(() -> new AtomicLong(0),
                                () -> new AtomicInteger(0),
                                () -> new AtomicDouble(0.0),
                                () -> new AtomicFloat(0.0f));
        }
    }

    public static class MaxFunction<U, T> extends UnaryAggregationFunction<U, T> {

        public MaxFunction(Id field, Supplier<U> supplier) {
            super(field, supplier);
        }

        public MaxFunction(Supplier<U> supplier) {
            super();
            this.supplier = supplier;
            this.buffer = initBuffer();
        }

        @Override
        protected U initBuffer() {
            return getInitValue(() -> new AtomicLong(Long.MIN_VALUE),
                                () -> new AtomicInteger(Integer.MIN_VALUE),
                                () -> new AtomicDouble(Double.MIN_VALUE),
                                () -> new AtomicFloat(Float.MIN_VALUE));
        }

        @Override
        public void iterate(T record) {
            if (record != null) {
                // string case
                if (this.buffer == null && record != null) {
                    this.buffer = (U) record;
                    return;
                }

                switch (buffer.getClass().getName()) {
                    case "java.util.concurrent.atomic.AtomicLong":
                        if (((AtomicLong) buffer).get() < (long) record) {
                            ((AtomicLong) buffer).set((long) record);
                        }
                        break;
                    case "java.util.concurrent.atomic.AtomicInteger":
                        if (((AtomicInteger) buffer).get() < (int) record) {
                            ((AtomicInteger) buffer).set((int) record);
                        }
                        break;
                    case "com.google.common.util.concurrent.AtomicDouble":
                        if (((AtomicDouble) buffer).get() < (double) record) {
                            ((AtomicDouble) buffer).set((double) record);
                        }
                        break;
                    case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                        if (((AtomicFloat) buffer).get() < (float) record) {
                            ((AtomicFloat) buffer).set((float) record);
                        }
                        break;

                    case "java.lang.String":
                        this.buffer = (U) maxString((String) buffer, (String) record);
                        break;
                    default:
                        // throw new Exception ?
                        break;
                }
            }

        }

        @Override
        public void merge(U other) {
            if (this.buffer == null && other != null) {
                this.buffer = other;
                return;
            }

            switch (buffer.getClass().getName()) {
                case "java.util.concurrent.atomic.AtomicLong":
                    if (((AtomicLong) buffer).get() < ((AtomicLong) other).get()) {
                        ((AtomicLong) buffer).set(((AtomicLong) other).get());
                    }
                    break;
                case "java.util.concurrent.atomic.AtomicInteger":
                    if (((AtomicInteger) buffer).get() < ((AtomicInteger) other).get()) {
                        ((AtomicInteger) buffer).set(((AtomicInteger) other).get());
                    }
                    break;
                case "com.google.common.util.concurrent.AtomicDouble":
                    if (((AtomicDouble) buffer).get() < ((AtomicDouble) other).get()) {
                        ((AtomicDouble) buffer).set(((AtomicDouble) other).get());
                    }
                    break;
                case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                    if (((AtomicFloat) buffer).compareTo(((AtomicFloat) other)) < 0) {
                        ((AtomicFloat) buffer).set(((AtomicFloat) other).get());
                    }
                    break;
                case "java.lang.String":
                    this.buffer = (U) maxString((String) buffer, (String) other);
                    break;
                default:
                    // throw new Exception ?
                    break;
            }
        }

        /**
         * 获取两个字符串中较长的那个。如果一个为null，则返回另一个。
         *
         * @param s1 第一个字符串
         * @param s2 第二个字符串
         * @return 较长的字符串
         */
        private String maxString(String s1, String s2) {
            if (s1 == null || s2 == null) {
                return s1 == null ? s2 : s1;
            }
            return s1.compareTo(s2) >= 0 ? s1 : s2;
        }

        @Override
        public T reduce() {
            switch (buffer.getClass().getName()) {
                case "java.util.concurrent.atomic.AtomicLong":
                    return (T) Long.valueOf(((AtomicLong) this.buffer).get());
                case "java.util.concurrent.atomic.AtomicInteger":
                    return (T) Integer.valueOf(((AtomicInteger) this.buffer).get());
                case "com.google.common.util.concurrent.AtomicDouble":
                    return (T) Double.valueOf(((AtomicDouble) this.buffer).get());
                case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                    return (T) Float.valueOf(((AtomicFloat) this.buffer).get());
                case "java.lang.String":
                    return (T) this.buffer;
                default:
                    // throw new Exception ?
                    break;
            }
            return null;
        }
    }

    public static class MinFunction<U, T> extends UnaryAggregationFunction<U, T> {

        public MinFunction(Id field, Supplier<U> supplier) {
            super(field, supplier);
        }

        public MinFunction(Supplier<U> supplier) {
            super();
            this.supplier = supplier;
            this.buffer = initBuffer();
        }

        @Override
        protected U initBuffer() {
            return getInitValue(() -> new AtomicLong(Long.MAX_VALUE),
                                () -> new AtomicInteger(Integer.MAX_VALUE),
                                () -> new AtomicDouble(Double.MAX_VALUE),
                                () -> new AtomicFloat(Float.MAX_VALUE));
        }

        @Override
        public void iterate(T record) {
            if (record != null) {
                // string case
                if (this.buffer == null && record != null) {
                    this.buffer = (U) record;
                    return;
                }

                switch (buffer.getClass().getName()) {
                    case "java.util.concurrent.atomic.AtomicLong":
                        if (((AtomicLong) buffer).get() < (long) record) {
                            ((AtomicLong) buffer).set((long) record);
                        }
                        break;
                    case "java.util.concurrent.atomic.AtomicInteger":
                        if (((AtomicInteger) buffer).get() < (int) record) {
                            ((AtomicInteger) buffer).set((int) record);
                        }
                        break;
                    case "com.google.common.util.concurrent.AtomicDouble":
                        if (((AtomicDouble) buffer).get() < (double) record) {
                            ((AtomicDouble) buffer).set((double) record);
                        }
                        break;
                    case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                        if (((AtomicFloat) buffer).get() < (float) record) {
                            ((AtomicFloat) buffer).set((float) record);
                        }
                        break;

                    case "java.lang.String":
                        this.buffer = (U) minString((String) buffer, (String) record);
                        break;
                    default:
                        // throw new Exception ?
                        break;
                }
            }
        }

        @Override
        public void merge(U other) {
            if (this.buffer == null && other != null) {
                this.buffer = other;
                return;
            }

            switch (buffer.getClass().getName()) {
                case "java.util.concurrent.atomic.AtomicLong":
                    if (((AtomicLong) buffer).get() < ((AtomicLong) other).get()) {
                        ((AtomicLong) buffer).set(((AtomicLong) other).get());
                    }
                    break;
                case "java.util.concurrent.atomic.AtomicInteger":
                    if (((AtomicInteger) buffer).get() < ((AtomicInteger) other).get()) {
                        ((AtomicInteger) buffer).set(((AtomicInteger) other).get());
                    }
                    break;
                case "com.google.common.util.concurrent.AtomicDouble":
                    if (((AtomicDouble) buffer).get() < ((AtomicDouble) other).get()) {
                        ((AtomicDouble) buffer).set(((AtomicDouble) other).get());
                    }
                    break;
                case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                    if (((AtomicFloat) buffer).compareTo(((AtomicFloat) other)) < 0) {
                        ((AtomicFloat) buffer).set(((AtomicFloat) other).get());
                    }
                    break;
                case "java.lang.String":
                    this.buffer = (U) minString((String) buffer, (String) other);
                    break;
                default:
                    // throw new Exception ?
                    break;
            }
        }

        /**
         * 返回两个字符串中的较小值。如果一个值为null则返回另一个值。
         *
         * @param s1 第一个需要比较的字符串
         * @param s2 第二个需要比较的字符串
         * @return 较小的字符串
         */
        private String minString(String s1, String s2) {
            if (s1 == null || s2 == null) {
                return s1 == null ? s2 : s1;
            }
            return s1.compareTo(s2) <= 0 ? s1 : s2;
        }

        @Override
        public T reduce() {
            switch (buffer.getClass().getName()) {
                case "java.util.concurrent.atomic.AtomicLong":
                    return (T) Long.valueOf(((AtomicLong) this.buffer).get());
                case "java.util.concurrent.atomic.AtomicInteger":
                    return (T) Integer.valueOf(((AtomicInteger) this.buffer).get());
                case "com.google.common.util.concurrent.AtomicDouble":
                    return (T) Double.valueOf(((AtomicDouble) this.buffer).get());
                case "java.lang.Float":
                    return (T) Float.valueOf(((AtomicFloat) this.buffer).get());
                case "org.apache.hugegraph.store.query.concurrent.AtomicFloat":
                    return (T) this.buffer;
                default:
                    // throw new Exception ?
                    break;
            }
            return null;
        }

    }

    public static class AvgFunction extends
                                    AbstractAggregationFunction<Tuple2<AtomicLong, AtomicDouble>,
                                            Double, Double> {

        private final Class filedClassType;

        public AvgFunction(Supplier supplier) {
            createBuffer();
            filedClassType = supplier.get().getClass();
        }

        public Class getFiledClassType() {
            return filedClassType;
        }

        /**
         * 创建缓冲区，返回一个包含两个原子变量的元组。
         *
         * @return 包含两个原子变量的元组
         */
        @Override
        public Tuple2<AtomicLong, AtomicDouble> createBuffer() {
            this.buffer = new Tuple2<>(new AtomicLong(0), new AtomicDouble(0.0));
            return this.buffer;
        }

        @Override
        public void iterate(Double record) {
            if (record != null) {
                buffer.getV1().getAndAdd(1);
                buffer.getV2().getAndAdd(record.doubleValue());
            }
        }

        @Override
        public void merge(Tuple2<AtomicLong, AtomicDouble> other) {
            buffer.getV1().getAndAdd(other.getV1().get());
            buffer.getV2().getAndAdd(other.getV2().get());
        }

        @Override
        public Double reduce() {
            if (buffer.getV1().get() == 0) {
                return Double.NaN;
            }

            return buffer.getV2().get() / buffer.getV1().get();
        }
    }

    public static class CountFunction extends AbstractAggregationFunction<AtomicLong, Long, Long> {

        public CountFunction() {
            createBuffer();
        }

        @Override
        public AtomicLong createBuffer() {
            this.buffer = new AtomicLong();
            return this.buffer;
        }

        @Override
        public AtomicLong getBuffer() {
            return this.buffer;
        }

        @Override
        public void iterate(Long record) {
            this.buffer.getAndIncrement();
        }

        @Override
        public void merge(AtomicLong other) {
            this.buffer.getAndAdd(other.get());
        }

        @Override
        public Long reduce() {
            return this.buffer.get();
        }
    }

    /**
     * 应对 group by 无 aggregator的情况
     */
    public static class EmptyFunction implements AggregationFunction<Integer, Integer, Integer> {

        @Override
        public Integer createBuffer() {
            return 0;
        }

        @Override
        public Integer getBuffer() {
            return 0;
        }

        @Override
        public void iterate(Integer record) {

        }

        @Override
        public void merge(Integer other) {

        }

        @Override
        public Integer reduce() {
            return null;
        }
    }

}
