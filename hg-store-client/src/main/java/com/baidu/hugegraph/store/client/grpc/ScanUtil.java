package com.baidu.hugegraph.store.client.grpc;

import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.client.HgStoreNodeSession;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.store.grpc.common.Header;
import com.google.protobuf.ByteString;

/**
 * @author zhangyingjie
 * @date 2022/10/8
 **/
public class ScanUtil {


    public static Header getHeader(HgStoreNodeSession nodeSession) {
        return Header.newBuilder().setGraph(nodeSession.getGraphName()).build();
    }

    public static HgOwnerKey toOk(HgOwnerKey key) {
        return key == null ? HgStoreClientConst.EMPTY_OWNER_KEY : key;
    }

    public static ByteString toBs(byte[] bytes) {
        return ByteString.copyFrom((bytes != null) ? bytes : EMPTY_BYTES);
    }

    public static ByteString getHgOwnerKey(HgOwnerKey ownerKey) {
        return toBs(toOk(ownerKey).getKey());
    }

    public static byte[] getQuery(byte[] query) {
        return query != null ? query : HgStoreClientConst.EMPTY_BYTES;
    }
    public static long getLimit(long limit) {
        return limit <= HgStoreClientConst.NO_LIMIT ? Integer.MAX_VALUE : limit;
    }
}
