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

package org.apache.hugegraph.pd.meta;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.store.KV;

import com.google.protobuf.Parser;

public abstract class MetadataStoreBase {

    //  public long timeout = 3;

    public abstract byte[] getOne(byte[] key) throws PDException;

    public abstract <E> E getOne(Parser<E> parser, byte[] key) throws PDException;

    public abstract void put(byte[] key, byte[] value) throws PDException;

    /**
     * A put with an expiration time
     */
    public abstract void putWithTTL(byte[] key,
                                    byte[] value,
                                    long ttl) throws PDException;

    public abstract void putWithTTL(byte[] key,
                                    byte[] value,
                                    long ttl, TimeUnit timeUnit) throws PDException;

    public abstract byte[] getWithTTL(byte[] key) throws PDException;

    public abstract List getListWithTTL(byte[] key) throws PDException;

    public abstract void removeWithTTL(byte[] key) throws PDException;

    /**
     * Prefix queries
     *
     * @param prefix
     * @return
     * @throws PDException
     */
    public abstract List<KV> scanPrefix(byte[] prefix) throws PDException;

    /**
     * Prefix queries
     *
     * @param prefix
     * @return
     * @throws PDException
     */
    public abstract <E> List<E> scanPrefix(Parser<E> parser, byte[] prefix) throws PDException;

    public abstract List<KV> scanRange(byte[] start, byte[] end) throws PDException;

    public abstract <E> List<E> scanRange(Parser<E> parser, byte[] start, byte[] end) throws
                                                                                      PDException;

    /**
     * Check if the key exists
     *
     * @param key
     * @return
     * @throws PDException
     */
    public abstract boolean containsKey(byte[] key) throws PDException;

    public abstract long remove(byte[] key) throws PDException;

    public abstract long removeByPrefix(byte[] prefix) throws PDException;

    public abstract void clearAllCache() throws PDException;

    public abstract void close() throws IOException;

    public <T> T getInstanceWithTTL(Parser<T> parser, byte[] key) throws PDException {
        try {
            byte[] withTTL = this.getWithTTL(key);
            return parser.parseFrom(withTTL);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    public <T> List<T> getInstanceListWithTTL(Parser<T> parser, byte[] key)
            throws PDException {
        try {
            List withTTL = this.getListWithTTL(key);
            LinkedList<T> ts = new LinkedList<>();
            for (int i = 0; i < withTTL.size(); i++) {
                ts.add(parser.parseFrom((byte[]) withTTL.get(i)));
            }
            return ts;
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }
}
