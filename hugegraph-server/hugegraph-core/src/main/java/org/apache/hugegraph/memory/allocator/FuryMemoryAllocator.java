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

package org.apache.hugegraph.memory.allocator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.fury.format.encoder.Encoders;
import org.apache.fury.format.encoder.RowEncoder;
import org.apache.fury.format.row.binary.BinaryArray;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.hugegraph.memory.MemoryManager;

public class FuryMemoryAllocator implements MemoryAllocator {

    private final MemoryManager memoryManager;

    public FuryMemoryAllocator(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public BinaryRow forceAllocate(long size) {
        return null;
    }

    @Override
    public BinaryRow tryToAllocate(long size) {
        return null;
    }

    @Override
    public void releaseMemory(long size) {
        memoryManager.getCurrentOffHeapAllocatedMemory().addAndGet(-size);
    }

    public static void main(String[] args) {
        RowEncoder<Foo> encoder = Encoders.bean(Foo.class);
        Foo foo = new Foo();
        foo.f1 = 10;
        foo.f2 = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());
        foo.f3 =
                IntStream.range(0, 1000000).boxed().collect(Collectors.toMap(i -> "k" + i, i -> i));
        List<Bar> bars = new ArrayList<>(1000000);
        for (int i = 0; i < 1000000; i++) {
            Bar bar = new Bar();
            bar.f1 = "s" + i;
            bar.f2 = LongStream.range(0, 10).boxed().collect(Collectors.toList());
            bars.add(bar);
        }
        foo.f4 = bars;
        // Can be zero-copy read by python
        BinaryRow binaryRow = encoder.toRow(foo);
        foo = null;
        for (int i = 0; i < 1000; i++) {
            System.gc();
        }
        // can be data from python
        Foo newFoo = encoder.fromRow(binaryRow);
        System.out.println(newFoo);
        // zero-copy read List<Integer> f2
        BinaryArray binaryArray2 = binaryRow.getArray(1);
        // zero-copy read List<Bar> f4
        BinaryArray binaryArray4 = binaryRow.getArray(3);
        // zero-copy read 11th element of `readList<Bar> f4`
        BinaryRow barStruct = binaryArray4.getStruct(10);
        // zero-copy read 6th of f2 of 11th element of `readList<Bar> f4`
        barStruct.getArray(1).getInt64(5);
        RowEncoder<Bar> barEncoder = Encoders.bean(Bar.class);
        // deserialize part of data.
        Bar newBar = barEncoder.fromRow(barStruct);
        Bar newBar2 = barEncoder.fromRow(binaryArray4.getStruct(20));
        System.out.println(newBar);
        System.out.println(newBar2);
    }
}


