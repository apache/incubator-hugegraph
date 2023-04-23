package com.baidu.hugegraph.store.client.grpc;

import com.baidu.hugegraph.store.HgSeekAble;
import com.baidu.hugegraph.store.client.util.HgAssert;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com on 2022/3/11
 */
class SeekAbleIterator<E> implements Iterator, HgSeekAble {
    private Iterator<E> iterator;
    private Consumer<byte[]> seeker;
    private Supplier<byte[]> positioner;

    public static <E> SeekAbleIterator of(Iterator<E> iterator,Supplier<byte[]> positioner, Consumer<byte[]> seeker) {
        HgAssert.isArgumentNotNull(iterator, "iterator");
        HgAssert.isArgumentNotNull(positioner, "positioner");
        HgAssert.isArgumentNotNull(seeker, "seeker");
        return new SeekAbleIterator(iterator,positioner, seeker);
    }

    private SeekAbleIterator(Iterator<E> iterator, Supplier<byte[]> positioner,Consumer<byte[]> seeker) {
        this.iterator = iterator;
        this.positioner=positioner;
        this.seeker = seeker;
    }

    @Override
    public byte[] position() {
        return this.positioner.get();
    }

    @Override
    public void seek(byte[] position) {
        this.seeker.accept(position);
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public E next() {
        return this.iterator.next();
    }
}