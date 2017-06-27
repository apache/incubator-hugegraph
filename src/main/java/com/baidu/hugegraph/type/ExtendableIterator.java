package com.baidu.hugegraph.type;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ExtendableIterator<T> implements Iterator<T> {

    private Deque<Iterator<T>> itors;

    public ExtendableIterator() {
        this.itors = new ConcurrentLinkedDeque<Iterator<T>>();
    }

    public ExtendableIterator(Iterator<T> itor) {
        this();
        this.extend(itor);
    }

    public ExtendableIterator(Iterator<T> itor1, Iterator<T> itor2) {
        this();
        this.extend(itor1);
        this.extend(itor2);
    }

    @Override
    public boolean hasNext() {
        // this is true since we never hold empty iterators
        return !this.itors.isEmpty() && this.itors.peekLast().hasNext();
    }

    @Override
    public T next() {
        T next = this.itors.peekFirst().next();
        if (!this.itors.peekFirst().hasNext()) {
            this.itors.removeFirst();
        }
        return next;
    }

    public ExtendableIterator<T> extend(Iterator<T> itor) {
        if (itor != null && itor.hasNext()) {
            this.itors.addLast(itor);
        }
        return this;
    }
}
