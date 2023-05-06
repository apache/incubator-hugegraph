package org.apache.hugegraph.pd.meta;


import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Pdpb;

import org.apache.hugegraph.pd.store.KV;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Parser;

public abstract class MetadataStoreBase {

  //  public long timeout = 3; // 请求超时时间，默认三秒

    public abstract byte[] getOne(byte[] key) throws PDException;

    public abstract <E> E getOne(Parser<E> parser, byte[] key) throws PDException;

    public abstract void put(byte[] key, byte[] value) throws PDException;

    /**
     * 带有过期时间的put
     */

    public abstract void putWithTTL(byte[] key,
                                        byte[] value,
                                        long ttl) throws PDException;
    public abstract void putWithTTL(byte[] key,
                                    byte[] value,
                                    long ttl, TimeUnit timeUnit) throws PDException;
    public abstract byte[] getWithTTL(byte[] key) throws PDException;

    public abstract  List getListWithTTL(byte[] key) throws PDException;

    public abstract void removeWithTTL(byte[] key) throws PDException;
    /**
     * 前缀查询
     *
     * @param prefix
     * @return
     * @throws PDException
     */
    public abstract List<KV> scanPrefix(byte[] prefix) throws PDException;
    public abstract List<KV> scanRange(byte[] start,byte[] end) throws PDException;
    public abstract <E> List<E> scanRange(Parser<E> parser, byte[] start,byte[] end) throws PDException;
    /**
     * 前缀查询
     *
     * @param prefix
     * @return
     * @throws PDException
     */

    public abstract <E> List<E> scanPrefix(Parser<E> parser, byte[] prefix) throws PDException;


    /**
     * 检查Key是否存在
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

    public <T> T getInstanceWithTTL(Parser<T> parser,byte[] key) throws PDException{
        try{
            byte[] withTTL = this.getWithTTL(key);
            return parser.parseFrom(withTTL);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE,e);
        }
    }
    public <T> List<T> getInstanceListWithTTL(Parser<T> parser,byte[] key)
                                                            throws PDException{
        try{
            List withTTL = this.getListWithTTL(key);
            LinkedList<T> ts = new LinkedList<>();
            for (int i = 0; i < withTTL.size(); i++) {
                ts.add(parser.parseFrom((byte[]) withTTL.get(i)));
            }
            return ts;
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE,e);
        }
    }
}
