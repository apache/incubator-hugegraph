package com.baidu.hugegraph.backend.store.hstore.fake;

import com.baidu.hugegraph.backend.store.hstore.HstoreSessions;
import com.baidu.hugegraph.pd.grpc.Pdpb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author zhangyingjie
 * @date 2022/1/17
 **/
public abstract class IdClient {

    protected HstoreSessions.Session session;
    protected String table;

    public IdClient(HstoreSessions.Session session,String table) {
        this.session=session;
        this.table = table;
    }

    public abstract Pdpb.GetIdResponse getIdByKey(String key, int delta)
           throws Exception ;
    public abstract Pdpb.ResetIdResponse resetIdByKey(String key) throws Exception ;
    public abstract void increaseId(String key, long increment)
           throws Exception;

    protected static byte[] b(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(
                ByteOrder.nativeOrder()).putLong(value).array();
    }

    protected static long l(byte[] bytes) {
        assert bytes.length == Long.BYTES;
        return ByteBuffer.wrap(bytes).order(
                ByteOrder.nativeOrder()).getLong();
    }
}
