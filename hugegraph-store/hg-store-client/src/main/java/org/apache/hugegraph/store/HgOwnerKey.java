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

package org.apache.hugegraph.store;

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_OWNER_KEY;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.hugegraph.store.client.util.HgStoreClientUtil;

/**
 * created on 2021/10/14
 *
 * @version 1.3.0 add canceled assert
 */
public class HgOwnerKey implements Serializable {
    private final byte[] owner;
    private int keyCode = 0;// TODO: Be here OK?
    private byte[] key;
    private int serialNo;   //顺序号，用于批量查询保证返回结果的顺序性

    /**
     * @param owner
     * @param key
     * @see HgOwnerKey:of(byte[] owner, byte[] key)
     */
    @Deprecated
    public HgOwnerKey(byte[] owner, byte[] key) {
        if (owner == null) {
            owner = EMPTY_BYTES;
        }
        if (key == null) {
            key = EMPTY_BYTES;
        }
        this.owner = owner;
        this.key = key;
    }

    public HgOwnerKey(int code, byte[] key) {
        if (key == null) {
            key = EMPTY_BYTES;
        }
        this.owner = EMPTY_BYTES;
        this.key = key;
        this.keyCode = code;
    }

    public static HgOwnerKey emptyOf() {
        return EMPTY_OWNER_KEY;
    }

    public static HgOwnerKey newEmpty() {
        return HgOwnerKey.of(EMPTY_BYTES, EMPTY_BYTES);
    }

    public static HgOwnerKey ownerOf(byte[] owner) {
        return new HgOwnerKey(owner, EMPTY_BYTES);
    }

    public static HgOwnerKey codeOf(int code) {
        return HgOwnerKey.of(EMPTY_BYTES, EMPTY_BYTES).setKeyCode(code);
    }

    public static HgOwnerKey of(byte[] owner, byte[] key) {
        return new HgOwnerKey(owner, key);
    }

    public static HgOwnerKey of(int keyCode, byte[] key) {
        return new HgOwnerKey(keyCode, key);
    }

    public byte[] getOwner() {
        return owner;
    }

    public byte[] getKey() {
        return key;
    }

    public int getKeyCode() {
        return keyCode;
    }

    public HgOwnerKey setKeyCode(int keyCode) {
        this.keyCode = keyCode;
        return this;
    }

    public HgOwnerKey codeToKey(int keyCode) {
        this.keyCode = keyCode;
        this.key = HgStoreClientUtil.toIntBytes(keyCode);
        return this;
    }

    public int getSerialNo() {
        return this.serialNo;
    }

    public HgOwnerKey setSerialNo(int serialNo) {
        this.serialNo = serialNo;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HgOwnerKey that = (HgOwnerKey) o;
        return Arrays.equals(owner, that.owner) && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(owner);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }

    @Override
    public String toString() {
        return "HgOwnerKey{" +
               "owner=" + Arrays.toString(owner) +
               ", key=" + Arrays.toString(key) +
               ", code=" + keyCode +
               '}';
    }

}
