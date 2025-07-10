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

package org.apache.hugegraph.store.query.concurrent;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class AtomicFloat extends Number implements Serializable, Comparable<AtomicFloat> {

    private static final AtomicIntegerFieldUpdater<AtomicFloat> FIELD_UPDATER;

    static {
        FIELD_UPDATER = AtomicIntegerFieldUpdater.newUpdater(AtomicFloat.class, "intBits");
    }

    private volatile int intBits;

    public AtomicFloat() {
        this.intBits = Float.floatToIntBits(0.0f);
    }

    public AtomicFloat(float value) {
        this.intBits = Float.floatToRawIntBits(value);
    }

    public float get() {
        return Float.intBitsToFloat(intBits);
    }

    public final void set(float newValue) {
        this.intBits = Float.floatToIntBits(newValue);
    }

    public final float getAndSet(float newValue) {
        return getAndSetFloat(newValue);
    }

    public final float getAndAdd(float delta) {
        return getAndAddFloat(delta);
    }

    /**
     * 向当前值添加指定值并返回总和。
     *
     * @param delta 需要添加的值
     * @return 当前值与参数delta的总和
     */
    public final float addAndGet(float delta) {
        return getAndAddFloat(delta) + delta;
    }

    /**
     * 计算并添加浮点数。将给定的浮点数delta加到当前的浮点数上，并返回结果。
     *
     * @param delta 浮点数的增量值
     * @return 返回更新后的浮点数
     */
    private float getAndAddFloat(float delta) {
        int oldBits;
        int newBits;
        do {
            oldBits = intBits;
            newBits = Float.floatToIntBits(Float.intBitsToFloat(oldBits) + delta);
        } while (!FIELD_UPDATER.compareAndSet(this, oldBits, newBits));
        return Float.intBitsToFloat(oldBits);
    }

    /**
     * 将float值设置为给定的新值，并返回旧值。
     *
     * @param newValue 新的float值
     * @return 旧值
     */
    private float getAndSetFloat(float newValue) {
        int oldBits;
        int newBits;
        do {
            oldBits = intBits;
            newBits = Float.floatToIntBits(newValue);
        } while (!FIELD_UPDATER.compareAndSet(this, oldBits, newBits));
        return Float.intBitsToFloat(oldBits);
    }

    /**
     * {@inheritDoc}
     * 返回值将转换为int类型并返回。
     *
     * @return 整型数值
     */
    @Override
    public int intValue() {
        return (int) get();
    }

    /**
     * {@inheritDoc}
     * 返回一个长整型值。
     *
     * @return 长整型值
     */
    @Override
    public long longValue() {
        return (long) get();
    }

    /**
     * {@inheritDoc} 返回当前值的float类型值。
     */
    @Override
    public float floatValue() {
        return get();
    }

    /**
     * {@inheritDoc}
     * 返回当前对象的值对应的double类型值。
     *
     * @return 当前对象的对应double类型值。
     */
    @Override
    public double doubleValue() {
        return get();
    }

    /**
     * {@inheritDoc}
     * 重写父类方法，实现了浮点数的比较。
     *
     * @param o 待比较的浮点数
     * @return 如果当前浮点数小于o，返回-1；如果相等，返回0；否则返回1
     */
    @Override
    public int compareTo(AtomicFloat o) {
        return Float.compare(get(), o.get());
    }

    /**
     * {@inheritDoc}
     * 返回字符串表示形式。
     *
     * @return 包含整型位（intBits）和值的字符串
     */
    @Override
    public String toString() {
        return "AtomicFloat{" +
               "intBits=" + intBits +
               ", value = " + get() +
               '}';
    }
}
