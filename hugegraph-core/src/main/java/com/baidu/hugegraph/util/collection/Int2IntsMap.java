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

package com.baidu.hugegraph.util.collection;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

/**
 * TODO: move to common-module
 */
public class Int2IntsMap {

    private static final int INIT_KEY_CAPACITY = 16;
    private static final int CHUNK_SIZE = 10;
    private static final int EXPANSION_FACTOR = 2;

    private static final int OFFSET_NEXT_FREE = 0;
    private static final int OFFSET_SIZE = 1;
    private static final int OFFSET_FIRST_CHUNK_DATA = 2;

    /*
     *  chunkMap              chunkTable
     *
     *   --------              ---------------
     *  | 1 | 0  |--------->0 |      33       | nextFree (free entry pointer)
     *  | 2 | 10 |-----+    1 |      19       | size (values count of key)
     *  | 3 | 40 |---+ |    2 |   int data    |
     *  | . | .  |   | |    3 |   int data    |
     *  | x | y  |   | |    . |     ...       | point to nextFreeChunk
     *   --------    | |    9 |      20       |-----------------+
     *               | |       ---------------                  |
     *               | +-->10 |     13        | nextFree        |
     *               |     11 |      1        | size            |
     *               |     12 |   int data    |                 |
     *               |     13 |      0        |                 |
     *               |      . |     ...       |                 |
     *               |     19 |      0        |                 |
     *               |         ---------------                  |
     *               |     20 |   int data    |<----------------+
     *               |     21 |   int data    |
     *               |     22 |   int data    |
     *               |     23 |   int data    |
     *               |      . |     ...       | point to nextFreeChunk
     *               |     29 |      30       |-----------------+
     *               |         ---------------                  |
     *               |     30 |   int data    |<----------------+
     *               |     31 |   int data    |
     *               |     32 |   int data    |
     *               |     33 |      0        |
     *               |      . |     ...       |
     *               |     39 |      0        |
     *               |         ---------------
     *               +---->40 |      48       | nextFree
     *                     41 |       6       | size
     *                     42 |   int data    |
     *                     43 |   int data    |
     *                      . |     ...       |
     *                     47 |   int data    |
     *                     48 |      0        |
     *                     49 |      0        |
     *                         ---------------
     *                     50 |     ...       |
     *                        |     ...       |
     *                        |     ...       |
     *                        |     ...       |
     */

    private final IntIntHashMap chunkMap;
    private int[] chunkTable;

    private int nextFreeChunk;

    public Int2IntsMap() {
        this.chunkMap = new IntIntHashMap(INIT_KEY_CAPACITY);
        this.chunkTable = new int[INIT_KEY_CAPACITY * CHUNK_SIZE];
        this.nextFreeChunk = 0;
    }

    public void add(int key, int value) {
        if (this.chunkMap.containsKey(key)) {
            int firstChunk = this.chunkMap.get(key);
            /*
             * The nextFree represent the position where the next element
             * will be located.
             */
            int nextFree = this.chunkTable[firstChunk + OFFSET_NEXT_FREE];
            if (!this.endOfChunk(nextFree)) {
                this.chunkTable[nextFree] = value;
                this.chunkTable[firstChunk + OFFSET_NEXT_FREE]++;
            } else {
                /*
                 * If the nextFree points to the end of last chunk,
                 * allocate a new chunk and let the nextFree point to
                 * the start of new allocated chunk.
                 */
                this.ensureCapacity();

                int lastEntryOfChunk = nextFree;
                this.chunkTable[lastEntryOfChunk] = this.nextFreeChunk;

                nextFree = this.nextFreeChunk;
                this.chunkTable[nextFree] = value;
                this.chunkTable[firstChunk + OFFSET_NEXT_FREE] = nextFree + 1;

                // Update next block
                this.nextFreeChunk += CHUNK_SIZE;
            }
            this.chunkTable[firstChunk + OFFSET_SIZE]++;
        } else {
            // New key, allocate 1st chunk and init
            this.ensureCapacity();

            // Allocate 1st chunk
            this.chunkMap.put(key, this.nextFreeChunk);

            // Init first chunk
            int firstChunk = this.nextFreeChunk;
            int nextFree = firstChunk + OFFSET_FIRST_CHUNK_DATA;
            this.chunkTable[firstChunk + OFFSET_NEXT_FREE] = nextFree + 1;
            this.chunkTable[firstChunk + OFFSET_SIZE] = 1;
            this.chunkTable[nextFree] = value;

            // Update next block
            this.nextFreeChunk += CHUNK_SIZE;
        }
    }

    public boolean containsKey(int key) {
        return this.chunkMap.containsKey(key);
    }

    public int[] getValues(int key) {
        int firstChunk = this.chunkMap.getIfAbsent(key, -1);
        if (firstChunk == -1) {
            return com.baidu.hugegraph.util.collection.IntIterator.EMPTY_INTS;
        }

        int size = this.chunkTable[firstChunk + OFFSET_SIZE];
        int[] values = new int[size];
        int position = firstChunk + OFFSET_FIRST_CHUNK_DATA;
        int i = 0;
        while (i < size) {
            if (!this.endOfChunk(position)) {
                values[i++] = this.chunkTable[position++];
            } else {
                position = this.chunkTable[position];
            }
        }
        return values;
    }

    public IntIterator keys() {
        return this.chunkMap.keySet().intIterator();
    }

    public int size() {
        return this.chunkMap.size();
    }

    @Override
    public String toString() {
        int capacity = (this.size() + 1) * 64;
        StringBuilder sb = new StringBuilder(capacity);
        sb.append("{");
        for (IntIterator iter = this.keys(); iter.hasNext();) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            int key = iter.next();
            sb.append(key).append(": [");
            int[] values = this.getValues(key);
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(values[i]);
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    private boolean endOfChunk(int position) {
        // The last entry of chunk is next chunk pointer
        return (position + 1) % CHUNK_SIZE == 0;
    }

    private void ensureCapacity() {
        if (this.nextFreeChunk >= this.chunkTable.length) {
            this.expand();
        }
    }

    private void expand() {
        int currentSize = this.chunkTable.length;
        int[] newTable = new int[currentSize * EXPANSION_FACTOR];
        System.arraycopy(this.chunkTable, 0, newTable, 0, currentSize);
        this.chunkTable = newTable;
    }
}
