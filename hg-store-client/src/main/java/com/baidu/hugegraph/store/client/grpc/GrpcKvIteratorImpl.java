package com.baidu.hugegraph.store.client.grpc;

import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toIntBytes;

import java.util.List;

import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgKvOrderedIterator;
import com.baidu.hugegraph.store.HgKvPagingIterator;
import com.baidu.hugegraph.store.HgPageSize;
import com.baidu.hugegraph.store.HgSeekAble;
import com.baidu.hugegraph.store.client.HgStoreNodeSession;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.store.grpc.common.Kv;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/20
 * @version 0.2.1
 */
@Slf4j
class GrpcKvIteratorImpl implements HgKvPagingIterator<HgKvEntry>, HgKvOrderedIterator<HgKvEntry> {

    private byte[] emptyBytes = HgStoreClientConst.EMPTY_BYTES;
    private KvCloseableIterator<Kv> iterator;
    private HgKvEntry element;
    private HgPageSize pageLimiter;
    private HgStoreNodeSession session;

    public static HgKvIterator<HgKvEntry> of(HgStoreNodeSession nodeSession, KvCloseableIterator<Kv> iterator) {
        if (HgPageSize.class.isInstance(iterator)) {
            return of(nodeSession, iterator, (HgPageSize) iterator);
        }
        return new GrpcKvIteratorImpl(nodeSession, iterator, () -> 1);
    }

    public static HgKvIterator<HgKvEntry> of(HgStoreNodeSession nodeSession, KvCloseableIterator<Kv> iterator,
                                             HgPageSize pageLimiter) {
        return new GrpcKvIteratorImpl(nodeSession, iterator, pageLimiter);
    }

    public static HgKvIterator<HgKvEntry> of(HgStoreNodeSession nodeSession, List<Kv> kvList) {
        int pageSize = kvList.size();
        return new GrpcKvIteratorImpl(nodeSession, new KvListIterator<Kv>(kvList), () -> pageSize);
    }

    private GrpcKvIteratorImpl(HgStoreNodeSession session, KvCloseableIterator<Kv> iterator, HgPageSize pageLimiter) {
        this.iterator = iterator;
        this.pageLimiter = pageLimiter;
        this.session = session;
    }

    @Override
    public boolean hasNext() {
        // if (log.isDebugEnabled()) {
        //    if (!this.iterator.hasNext() && !nodeSession.getGraphName().endsWith("/s")) {
        //        log.debug("[ANALYSIS GrpcKv hasNext-> FALSE] ");
        //    }
        // }
        return this.iterator.hasNext();
    }

    @Override
    public HgKvEntry next() {
        Kv kv = this.iterator.next();
        this.element = new GrpcKvEntryImpl(kv.getKey().toByteArray(), kv.getValue().toByteArray(), kv.getCode());
        return this.element;
    }

    @Override
    public byte[] key() {
        if (this.element == null) {
            return null;
        }
        return this.element.key();
    }

    @Override
    public byte[] value() {
        if (this.element == null) {
            return null;
        }
        return this.element.value();
    }

    @Override
    public byte[] position() {
        if (this.element == null) {
            return emptyBytes;
        }
        byte[] key = this.element.key();
        if (key == null) {
            return emptyBytes;
        }
        if (!HgSeekAble.class.isInstance(this.iterator)) {
            return emptyBytes;
        }
        byte[] upstream = ((HgSeekAble) this.iterator).position();
        byte[] code = toIntBytes(this.element.code());
        byte[] result = new byte[upstream.length + Integer.BYTES + key.length];
        System.arraycopy(upstream, 0, result, 0, upstream.length);
        System.arraycopy(code, 0, result, upstream.length, Integer.BYTES);
        System.arraycopy(key, 0, result, upstream.length + Integer.BYTES, key.length);
        return result;
    }

    @Override
    public void seek(byte[] position) {
        if (HgSeekAble.class.isInstance(this.iterator)) {
            ((HgSeekAble) this.iterator).seek(position);
        }
    }

    @Override
    public long getPageSize() {
        return pageLimiter.getPageSize();
    }

    @Override
    public boolean isPageEmpty() {
        return !iterator.hasNext();
    }


    @Override
    public int compareTo(HgKvOrderedIterator o) {
        return Long.compare(this.getSequence(), o.getSequence());
    }

    @Override
    public long getSequence() {
        return this.session.getStoreNode().getNodeId().longValue();
    }

    @Override
    public void close() {
        this.iterator.close();
    }
}
