/*
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

package org.apache.hugegraph.store.term;

import java.util.Objects;

/**
 * created on 2021/10/19
 */
public class HgTriple<X, Y, Z> {
    private final X x;
    private final Y y;
    private final Z z;
    private int hash = -1;

    public HgTriple(X x, Y y, Z z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public X getX() {
        return x;
    }

    public Y getY() {
        return y;
    }

    public Z getZ() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HgTriple<?, ?, ?> hgTriple = (HgTriple<?, ?, ?>) o;
        return Objects.equals(x, hgTriple.x) && Objects.equals(y, hgTriple.y) &&
               Objects.equals(z, hgTriple.z);
    }

    @Override
    public int hashCode() {
        if (hash == -1) {
            hash = Objects.hash(x, y, z);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        return "HgTriple{" +
               "x=" + x +
               ", y=" + y +
               ", z=" + z +
               '}';
    }
}
